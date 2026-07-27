let ws;
let currentRoom = '';
let myClientId = '';
let peers = {}; // Храним WebRTC соединения: peerId -> RTCPeerConnection
let dataChannels = {}; // peerId -> RTCDataChannel

const CHUNK_SIZE = 16384; // 16KB для WebRTC

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

// Проверка URL на наличие кода комнаты
window.onload = () => {
    const params = new URLSearchParams(window.location.search);
    const roomParam = params.get('room');
    if (roomParam) {
        connect(roomParam.toUpperCase());
    }
};

createBtn.onclick = () => connect('');
joinBtn.onclick = () => {
    const code = roomCodeInput.value.trim().toUpperCase();
    if(code) connect(code);
};

// Копирование ссылки
displayRoomCode.onclick = () => {
    const link = `${window.location.origin}/?room=${currentRoom}`;
    navigator.clipboard.writeText(link).then(() => {
        const orig = displayRoomCode.innerText;
        displayRoomCode.innerText = 'СКОПИРОВАНО';
        setTimeout(() => displayRoomCode.innerText = orig, 1500);
    });
};

function connect(roomCode) {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(`${protocol}//${window.location.host}/ws` + (roomCode ? `?room=${roomCode}` : ''));

    ws.onmessage = async (event) => {
        const data = JSON.parse(event.data);
        
        if (data.type === 'init') {
            currentRoom = data.room;
            myClientId = data.client_id;
            displayRoomCode.innerText = currentRoom;
            window.history.replaceState(null, '', `?room=${currentRoom}`);
            switchScreen(roomScreen);
        } else if (data.type === 'peer_joined') {
            renderMessage({type: 'system', content: 'Новый участник подключился (установка P2P...)'});
            initWebRTC(data.sender_id, true); // Инициатор создает Offer
        } else if (data.type === 'signal') {
            handleSignal(data.sender_id, data.signal_data);
        } else {
            renderMessage(data);
        }
    };

    ws.onclose = () => { if(roomScreen.classList.contains('active')) triggerDissolve(); };
}

// === WEBRTC LOGIC ===
const configuration = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };

async function initWebRTC(peerId, isInitiator) {
    const pc = new RTCPeerConnection(configuration);
    peers[peerId] = pc;

    pc.onicecandidate = (e) => {
        if (e.candidate) {
            ws.send(JSON.stringify({ type: 'signal', target_id: peerId, signal_data: { candidate: e.candidate } }));
        }
    };

    if (isInitiator) {
        const dc = pc.createDataChannel('fileTransfer');
        setupDataChannel(dc, peerId);
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        ws.send(JSON.stringify({ type: 'signal', target_id: peerId, signal_data: { sdp: pc.localDescription } }));
    } else {
        pc.ondatachannel = (e) => setupDataChannel(e.channel, peerId);
    }
}

async function handleSignal(peerId, signal) {
    if (!peers[peerId]) await initWebRTC(peerId, false);
    const pc = peers[peerId];

    if (signal.sdp) {
        await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
        if (signal.sdp.type === 'offer') {
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            ws.send(JSON.stringify({ type: 'signal', target_id: peerId, signal_data: { sdp: pc.localDescription } }));
        }
    } else if (signal.candidate) {
        await pc.addIceCandidate(new RTCIceCandidate(signal.candidate));
    }
}

let incomingFileInfo = null;
let incomingFileChunks = [];

function setupDataChannel(dc, peerId) {
    dataChannels[peerId] = dc;
    dc.binaryType = 'arraybuffer';
    
    dc.onopen = () => console.log(`P2P канал с ${peerId} открыт!`);
    
    dc.onmessage = (e) => {
        if (typeof e.data === 'string') {
            const msg = JSON.parse(e.data);
            if (msg.type === 'file_start') {
                incomingFileInfo = msg.info;
                incomingFileChunks = [];
            } else if (msg.type === 'file_end') {
                assembleAndRenderFile(incomingFileInfo, incomingFileChunks, peerId);
                incomingFileInfo = null;
                incomingFileChunks = [];
            }
        } else {
            incomingFileChunks.push(e.data);
        }
    };
}

function assembleAndRenderFile(info, chunks, senderId) {
    const blob = new Blob(chunks, { type: info.type });
    const url = URL.createObjectURL(blob);
    
    renderMessage({
        type: 'file',
        sender_id: senderId,
        file_info: info,
        url: url
    });
}

function sendFileP2P(file) {
    if (Object.keys(dataChannels).length === 0) {
        alert("Нет подключенных P2P участников в комнате.");
        return;
    }

    // Отображаем у себя
    const localUrl = URL.createObjectURL(file);
    renderMessage({ type: 'file', sender_id: myClientId, file_info: { name: file.name, type: file.type, size: file.size }, url: localUrl });

    // Рассылаем всем пирам
    const reader = new FileReader();
    reader.onload = (e) => {
        const buffer = e.target.result;
        
        Object.values(dataChannels).forEach(dc => {
            if (dc.readyState === 'open') {
                dc.send(JSON.stringify({ type: 'file_start', info: { name: file.name, type: file.type, size: file.size } }));
                
                let offset = 0;
                while (offset < buffer.byteLength) {
                    dc.send(buffer.slice(offset, offset + CHUNK_SIZE));
                    offset += CHUNK_SIZE;
                }
                
                dc.send(JSON.stringify({ type: 'file_end' }));
            }
        });
    };
    reader.readAsArrayBuffer(file);
}

// === UI LOGIC ===
function sendText() {
    const text = messageInput.value.trim();
    if (!text || !ws) return;
    ws.send(JSON.stringify({ type: 'text', content: text }));
    messageInput.value = '';
}

sendBtn.onclick = sendText;
messageInput.onkeypress = (e) => { if(e.key === 'Enter') sendText(); };

fileBtn.onclick = () => fileInput.click();
fileInput.onchange = (e) => { if (e.target.files.length > 0) sendFileP2P(e.target.files[0]); };

roomScreen.addEventListener('dragover', (e) => { e.preventDefault(); dropOverlay.classList.add('active'); });
roomScreen.addEventListener('dragleave', (e) => { e.preventDefault(); if (e.relatedTarget === null) dropOverlay.classList.remove('active'); });
roomScreen.addEventListener('drop', (e) => {
    e.preventDefault(); dropOverlay.classList.remove('active');
    if (e.dataTransfer.files.length > 0) sendFileP2P(e.dataTransfer.files[0]);
});

function renderMessage(msg) {
    document.querySelector('.empty-state')?.remove();
    const div = document.createElement('div');
    
    if (msg.type === 'system') {
        div.className = 'msg system'; div.innerText = msg.content;
    } else {
        const isMine = msg.sender_id === myClientId;
        div.className = `msg ${isMine ? 'mine' : 'theirs'}`;
        
        if (msg.type === 'text') {
            if (msg.content.startsWith('http')) {
                div.innerHTML = `<a href="${msg.content}" target="_blank" style="color:inherit; text-decoration:underline;">${msg.content}</a>`;
            } else {
                div.innerText = msg.content;
            }
        } else if (msg.type === 'file') {
            const isImage = msg.file_info.type.startsWith('image/');
            const icon = `<svg viewBox="0 0 24 24" width="20" height="20" stroke="currentColor" stroke-width="2" fill="none"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><polyline points="13 2 13 9 20 9"></polyline></svg>`;
            
            let contentHTML = `<a href="${msg.url}" download="${msg.file_info.name}" class="msg-file-link">${icon} ${msg.file_info.name}</a>`;
            
            if (isImage) {
                // Добавляем предпросмотр картинки
                contentHTML += `<a href="${msg.url}" target="_blank"><img src="${msg.url}" class="msg-image" alt="preview"></a>`;
            }
            div.innerHTML = contentHTML;
        }
    }
    messagesDiv.appendChild(div);
    chatArea.scrollTop = chatArea.scrollHeight;
}

function switchScreen(screen) {
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    screen.classList.add('active');
}

function triggerDissolve() {
    roomScreen.classList.add('dissolve');
    setTimeout(() => {
        switchScreen(destroyedScreen);
        roomScreen.classList.remove('dissolve');
        messagesDiv.innerHTML = '';
        currentRoom = '';
        window.history.replaceState(null, '', '/');
        if(ws) ws.close();
        Object.values(peers).forEach(pc => pc.close());
        peers = {}; dataChannels = {};
    }, 1000);
}

leaveBtn.onclick = triggerDissolve;