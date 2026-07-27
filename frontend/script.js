let ws;
let currentRoom = '';
let myClientId = '';

// Элементы UI
const authScreen = document.getElementById('auth-screen');
const roomScreen = document.getElementById('room-screen');
const destroyedScreen = document.getElementById('destroyed-screen');
const createBtn = document.getElementById('create-btn');
const joinBtn = document.getElementById('join-btn');
const roomCodeInput = document.getElementById('room-code-input');
const displayRoomCode = document.getElementById('display-room-code');
const messageInput = document.getElementById('message-input');
const sendBtn = document.getElementById('send-btn');
const messagesDiv = document.getElementById('messages');
const leaveBtn = document.getElementById('leave-btn');
const chatArea = document.getElementById('chat-area');
const dropOverlay = document.getElementById('drop-overlay');
const fileBtn = document.getElementById('file-btn');
const fileInput = document.getElementById('file-input');

// Вход и создание
createBtn.onclick = () => connect('');
joinBtn.onclick = () => {
    const code = roomCodeInput.value.trim().toUpperCase();
    if(code) connect(code);
};

function connect(roomCode) {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/ws` + (roomCode ? `?room=${roomCode}` : '');
    
    ws = new WebSocket(wsUrl);

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        
        if (data.type === 'init') {
            currentRoom = data.room;
            myClientId = data.client_id;
            displayRoomCode.innerText = currentRoom;
            switchScreen(roomScreen);
        } else if (data.type === 'room_destroyed') {
            triggerDissolve();
        } else {
            renderMessage(data);
        }
    };

    ws.onclose = () => {
        if(roomScreen.classList.contains('active')) triggerDissolve();
    };
}

// Отправка сообщений
function sendText() {
    const text = messageInput.value.trim();
    if (!text || !ws) return;
    
    ws.send(JSON.stringify({ type: 'text', content: text }));
    messageInput.value = '';
}

sendBtn.onclick = sendText;
messageInput.onkeypress = (e) => { if(e.key === 'Enter') sendText(); };

// Отправка файлов
fileBtn.onclick = () => fileInput.click();
fileInput.onchange = (e) => {
    if (e.target.files.length > 0) uploadFile(e.target.files[0]);
};

function uploadFile(file) {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('room', currentRoom);
    formData.append('sender_id', myClientId);

    fetch('/upload', { method: 'POST', body: formData })
        .then(response => {
            if(!response.ok) alert("Ошибка загрузки. Возможно файл слишком большой.");
            fileInput.value = ''; // очистка
        })
        .catch(err => console.error("Ошибка HTTP", err));
}

// Drag & Drop
roomScreen.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropOverlay.classList.add('active');
});

roomScreen.addEventListener('dragleave', (e) => {
    e.preventDefault();
    if (e.relatedTarget === null) dropOverlay.classList.remove('active');
});

roomScreen.addEventListener('drop', (e) => {
    e.preventDefault();
    dropOverlay.classList.remove('active');
    
    if (e.dataTransfer.files.length > 0) {
        uploadFile(e.dataTransfer.files[0]);
    } else {
        // Поддержка перетаскивания текста/ссылок
        const text = e.dataTransfer.getData('text');
        if(text) {
            ws.send(JSON.stringify({ type: 'text', content: text }));
        }
    }
});

// Рендер UI
function renderMessage(msg) {
    const emptyState = document.querySelector('.empty-state');
    if(emptyState) emptyState.style.display = 'none';

    const div = document.createElement('div');
    
    if (msg.type === 'system') {
        div.className = 'msg system';
        div.innerText = msg.content;
    } else {
        const isMine = msg.sender_id === myClientId;
        div.className = `msg ${isMine ? 'mine' : 'theirs'}`;
        
        if (msg.type === 'text') {
            // Проверка на ссылку
            if (msg.content.startsWith('http')) {
                div.innerHTML = `<a href="${msg.content}" target="_blank" style="color:white; text-decoration:underline;">${msg.content}</a>`;
            } else {
                div.innerText = msg.content;
            }
        } else if (msg.type === 'file') {
            div.classList.add('file-msg');
            div.innerHTML = `
                <svg viewBox="0 0 24 24" width="24" height="24" stroke="currentColor" stroke-width="2" fill="none"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><polyline points="13 2 13 9 20 9"></polyline></svg>
                <a href="/download?id=${msg.file_id}" style="color:white; text-decoration:none;">${msg.content}</a>
            `;
        }
    }

    messagesDiv.appendChild(div);
    chatArea.scrollTop = chatArea.scrollHeight;
}

function switchScreen(screen) {
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    screen.classList.add('active');
}

// Анимация растворения
function triggerDissolve() {
    roomScreen.classList.add('dissolve');
    setTimeout(() => {
        switchScreen(destroyedScreen);
        roomScreen.classList.remove('dissolve');
        messagesDiv.innerHTML = '';
        currentRoom = '';
        if(ws) ws.close();
    }, 1500); // время совпадает с CSS анимацией
}

leaveBtn.onclick = () => {
    if(ws) ws.close();
    triggerDissolve();
};