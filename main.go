package main

import (
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

type Message struct {
	ID         string      `json:"id,omitempty"`
	Type       string      `json:"type"`
	Content    string      `json:"content"`
	SenderID   string      `json:"sender_id,omitempty"`
	TargetID   string      `json:"target_id,omitempty"`
	SignalData interface{} `json:"signal_data,omitempty"`
}

type Client struct {
	ID   string
	Conn *websocket.Conn
	mu   sync.Mutex
}

type Room struct {
	ID      string
	OwnerID string
	Clients map[string]*Client
	History []Message
}

var (
	roomsMu sync.RWMutex
	rooms   = make(map[string]*Room)
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
	fs := http.FileServer(http.Dir("./frontend"))
	http.Handle("/", fs)
	http.HandleFunc("/ws", handleWebSocket)

	log.Println("🚀 WarpRoom запущен на http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Ошибка WS:", err)
		return
	}

	action := r.URL.Query().Get("action")
	roomCode := r.URL.Query().Get("room")

	var clientID string
	var isOwner bool

	roomsMu.Lock()
	if action == "join" {
		if roomCode == "" {
			roomsMu.Unlock()
			conn.WriteJSON(Message{Type: "error", Content: "Не указан код комнаты"})
			conn.Close()
			return
		}
		if _, exists := rooms[roomCode]; !exists {
			roomsMu.Unlock()
			conn.WriteJSON(Message{Type: "error", Content: "Комната не найдена или уже уничтожена"})
			conn.Close()
			return
		}
		clientID = generateCode()
	} else {
		roomCode = generateCode()
		clientID = generateCode()
		isOwner = true
		rooms[roomCode] = &Room{
			ID:      roomCode,
			OwnerID: clientID,
			Clients: make(map[string]*Client),
			History: make([]Message, 0),
		}
	}

	room := rooms[roomCode]
	client := &Client{ID: clientID, Conn: conn}
	room.Clients[clientID] = client
	roomsMu.Unlock()

	client.mu.Lock()
	conn.WriteJSON(map[string]interface{}{
		"type": "init", "room": roomCode, "client_id": clientID, "is_owner": isOwner,
	})
	for _, msg := range room.History {
		conn.WriteJSON(msg)
	}
	client.mu.Unlock()

	broadcastUnsafeTargeted(roomCode, Message{
		Type:     "peer_joined",
		SenderID: clientID,
		Content:  "Пользователь присоединился",
	}, clientID)

	defer func() {
		roomsMu.Lock()
		r, ok := rooms[roomCode]
		if !ok {
			roomsMu.Unlock()
			conn.Close()
			return
		}

		delete(r.Clients, clientID)
		isOwner := r.OwnerID == clientID
		isEmpty := len(r.Clients) == 0

		clientsToNotify := make([]*Client, 0, len(r.Clients))
		for _, c := range r.Clients {
			clientsToNotify = append(clientsToNotify, c)
		}

		if isOwner || isEmpty {
			delete(rooms, roomCode)
		}
		roomsMu.Unlock()

		var msg Message
		if isOwner {
			msg = Message{Type: "room_destroyed"}
		} else {
			msg = Message{Type: "system", Content: "Пользователь покинул комнату"}
		}

		for _, c := range clientsToNotify {
			c.mu.Lock()
			c.Conn.WriteJSON(msg)
			c.mu.Unlock()
		}
		conn.Close()
	}()

	for {
		var msg Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			break
		}
		msg.SenderID = clientID

		if msg.Type == "text" {
			roomsMu.Lock()
			if r, ok := rooms[roomCode]; ok {
				r.History = append(r.History, msg)
			}
			roomsMu.Unlock()
			broadcastUnsafeTargeted(roomCode, msg, clientID)
		} else if msg.Type == "signal" {
			if msg.TargetID != "" {
				roomsMu.RLock()
				if r, ok := rooms[roomCode]; ok {
					if target, ok := r.Clients[msg.TargetID]; ok {
						target.mu.Lock()
						target.Conn.WriteJSON(msg)
						target.mu.Unlock()
					}
				}
				roomsMu.RUnlock()
			} else {
				broadcastUnsafeTargeted(roomCode, msg, clientID)
			}
		}
	}
}

func broadcastUnsafeTargeted(roomCode string, msg Message, excludedID string) {
	roomsMu.RLock()
	defer roomsMu.RUnlock()
	if room, ok := rooms[roomCode]; ok {
		for id, client := range room.Clients {
			if id != excludedID {
				client.mu.Lock()
				client.Conn.WriteJSON(msg)
				client.mu.Unlock()
			}
		}
	}
}
