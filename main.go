package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Структуры данных
type Message struct {
	Type     string `json:"type"`    // "text", "file", "system", "room_destroyed"
	Content  string `json:"content"` // Текст сообщения или имя файла
	FileID   string `json:"file_id"` // ID файла для скачивания
	SenderID string `json:"sender_id"`
}

type Client struct {
	ID   string
	Conn *websocket.Conn
	mu   sync.Mutex // Защита от конкурентной записи в вебсокет
}

type Room struct {
	ID      string
	Clients map[string]*Client
	History []Message
	FileIDs []string // Для очистки памяти
}

type MemoryFile struct {
	Name string
	Data []byte
}

// Глобальное состояние (в оперативной памяти)
var (
	roomsMu sync.RWMutex
	rooms   = make(map[string]*Room)

	filesMu sync.RWMutex
	files   = make(map[string]MemoryFile)
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func generateCode() string {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 5)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func main() {
	// Раздача фронтенда
	fs := http.FileServer(http.Dir("./frontend"))
	http.Handle("/", fs)

	// Эндпоинты
	http.HandleFunc("/ws", handleWebSocket)
	http.HandleFunc("/upload", handleFileUpload)
	http.HandleFunc("/download", handleFileDownload)

	fmt.Println("🚀 WarpRoom запущен на http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Ошибка WS:", err)
		return
	}

	roomCode := r.URL.Query().Get("room")
	if roomCode == "" {
		roomCode = generateCode()
	}

	clientID := generateCode()
	client := &Client{ID: clientID, Conn: conn}

	roomsMu.Lock()
	room, exists := rooms[roomCode]
	if !exists {
		room = &Room{
			ID:      roomCode,
			Clients: make(map[string]*Client),
			History: make([]Message, 0),
		}
		rooms[roomCode] = room
	}
	room.Clients[clientID] = client
	roomsMu.Unlock()

	// Отправляем клиенту его ID и код комнаты
	client.mu.Lock()
	conn.WriteJSON(map[string]interface{}{
		"type": "init", "room": roomCode, "client_id": clientID,
	})
	// Отправляем историю комнаты
	for _, msg := range room.History {
		conn.WriteJSON(msg)
	}
	client.mu.Unlock()

	broadcast(roomCode, Message{Type: "system", Content: "Пользователь присоединился"})

	// Чтение сообщений от клиента
	defer func() {
		roomsMu.Lock()
		if r, ok := rooms[roomCode]; ok {
			delete(r.Clients, clientID)
			broadcastUnsafe(r, Message{Type: "system", Content: "Пользователь отключился"})

			// Если лобби пустое — уничтожаем всё
			if len(r.Clients) == 0 {
				broadcastUnsafe(r, Message{Type: "room_destroyed"}) // На всякий случай

				// Очищаем файлы из ОЗУ
				filesMu.Lock()
				for _, fID := range r.FileIDs {
					delete(files, fID)
				}
				filesMu.Unlock()

				delete(rooms, roomCode)
				log.Printf("Комната %s и ее файлы уничтожены.", roomCode)
			}
		}
		roomsMu.Unlock()
		conn.Close()
	}()

	for {
		var msg Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			break
		}
		msg.SenderID = clientID

		roomsMu.Lock()
		if r, ok := rooms[roomCode]; ok {
			r.History = append(r.History, msg)
		}
		roomsMu.Unlock()

		broadcast(roomCode, msg)
	}
}

func broadcast(roomCode string, msg Message) {
	roomsMu.RLock()
	defer roomsMu.RUnlock()
	if room, ok := rooms[roomCode]; ok {
		broadcastUnsafe(room, msg)
	}
}

// Вызывать только внутри заблокированного мьютекса roomsMu
func broadcastUnsafe(room *Room, msg Message) {
	for _, client := range room.Clients {
		client.mu.Lock()
		client.Conn.WriteJSON(msg)
		client.mu.Unlock()
	}
}

func handleFileUpload(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(50 << 20) // Лимит 50 MB в ОЗУ
	if err != nil {
		http.Error(w, "Файл слишком большой", http.StatusBadRequest)
		return
	}

	roomCode := r.FormValue("room")
	senderID := r.FormValue("sender_id")

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Ошибка чтения файла", http.StatusBadRequest)
		return
	}
	defer file.Close()

	var buf bytes.Buffer
	io.Copy(&buf, file)

	fileID := generateCode() + generateCode()

	filesMu.Lock()
	files[fileID] = MemoryFile{
		Name: header.Filename,
		Data: buf.Bytes(),
	}
	filesMu.Unlock()

	// Привязываем файл к комнате для удаления
	roomsMu.Lock()
	if room, ok := rooms[roomCode]; ok {
		room.FileIDs = append(room.FileIDs, fileID)

		msg := Message{
			Type:     "file",
			Content:  header.Filename,
			FileID:   fileID,
			SenderID: senderID,
		}
		room.History = append(room.History, msg)
		broadcastUnsafe(room, msg)
	}
	roomsMu.Unlock()

	w.WriteHeader(http.StatusOK)
}

func handleFileDownload(w http.ResponseWriter, r *http.Request) {
	fileID := r.URL.Query().Get("id")

	filesMu.RLock()
	memFile, exists := files[fileID]
	filesMu.RUnlock()

	if !exists {
		http.Error(w, "Файл не найден или уничтожен", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", memFile.Name))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(memFile.Data)
}
