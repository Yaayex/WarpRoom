let ws;
let currentRoom = '';
let myClientId = '';
let isOwner = false;
let peers = {}; 
let dataChannels = {}; 
let incomingFiles = {}; // peerId -> { id, info, chunks, receivedSize }

const CHUNK_SIZE = 16384 * 4; // 64KB (Оптимально для WebRTC)

// DOM Элементы
const roomScreen = document.getElementById('room-screen');
const displayRoomCode = document.getElementById('display-room-code');
const messageInput = document.getElementById('message-input');
const messagesDiv = document.getElementById('messages');
const chatArea = document.getElementById('chat-area');

const generateId = () => 'msg_' + Math.random().toString(36).substr(2, 9);

window.onload = () => {
    const roomParam = new URLSearchParams(window.location.search).get('room');
    if (roomParam) connect(roomParam.toUpperCase());
};

document.getElementById('create-btn').onclick = () => connect('');
document.getElementById('join-btn').onclick = () => {
    const code = document.getElementById('room-code-input').value.trim().toUpperCase();
    if(code) connect(code);
};

displayRoomCode.onclick = () => {
    navigator.clipboard.writeText(`${window.location.origin}/?room=${currentRoom}`);
    displayRoomCode.innerText = 'СКОПИРОВАНО';
    setTimeout(() => displayRoomCode.innerText = currentRoom, 1500);
};

function connect(roomCode) {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(`${protocol}//${window.location.host}/ws` + (roomCode ? `?room=${roomCode}` : ''));

    ws.onmessage = async (event) => {
        const data = JSON.parse(event.data);
        
        if (data.type === 'init') {
            currentRoom = data.room;
            myClientId = data.client_id;
            isOwner = data.is_owner;
            displayRoomCode.innerText = currentRoom;
            window.history.replaceState(null, '', `?room=${currentRoom}`);
            switchScreen(roomScreen);
            
            if (isOwner) {
                renderMessage({type: 'system', content: 'Вы создатель комнаты. Ваш уход уничтожит её для всех.'});
            }
        } else if (data.type === 'peer_joined') {
            renderMessage({type: 'system', content: 'Участник подключился (P2P...)'});
            initWebRTC(data.sender_id, true);
        } else if (data.type === 'signal') {
            handleSignal(data.sender_id, data.signal_data);
        } else if (data.type === 'read_receipt') {
            // Кто-то прочитал наше сообщение
            const statusEl = document.getElementById(`status-${data.content}`);
            if (statusEl) {
                statusEl.innerText = '✓✓';
                statusEl.style.color = '#0a84ff';
            }
        } else if (data.type === 'room_destroyed') {
            triggerDissolve();
        } else {
            renderMessage(data);
            // Если это текстовое сообщение, отправляем отчет о прочтении
            if (data.type === 'text' && data.id) {
                ws.send(JSON.stringify({ type: 'read_receipt', content: data.id, target_id: data.sender_id }));
            }
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
        if (e.candidate) ws.send(JSON.stringify({ type: 'signal', target_id: peerId, signal_data: { candidate: e.candidate } }));
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

function setupDataChannel(dc, peerId) {
    dataChannels[peerId] = dc;
    dc.binaryType = 'arraybuffer';
    dc.bufferedAmountLowThreshold = 1024 * 512; // Порог для backpressure (512 KB)
    
    dc.onmessage = (e) => {
        if (typeof e.data === 'string') {
            const msg = JSON.parse(e.data);
            if (msg.type === 'file_start') {
                incomingFiles[peerId] = { id: msg.id, info: msg.info, chunks: [], receivedSize: 0 };
                renderIncomingFileUI(msg.id, msg.info.name, peerId);
            } else if (msg.type === 'file_end') {
                assembleAndRenderFile(peerId);
            }
        } else {
            const transfer = incomingFiles[peerId];
            transfer.chunks.push(e.data);
            transfer.receivedSize += e.data.byteLength;
            
            const percent = Math.floor((transfer.receivedSize / transfer.info.size) * 100);
            updateProgressUI(transfer.id, percent);
        }
    };
}

async function sendFileP2P(file) {
    if (Object.keys(dataChannels).length === 0) return alert("Нет подключенных участников.");
    
    const msgId = generateId();
    renderOutgoingFileUI(msgId, file);

    Object.values(dataChannels).forEach(dc => dc.send(JSON.stringify({
        type: 'file_start', id: msgId, info: { name: file.name, type: file.type, size: file.size }
    })));

    let offset = 0;
    while (offset < file.size) {
        const chunk = file.slice(offset, offset + CHUNK_SIZE);
        const buffer = await chunk.arrayBuffer();

        for (let peerId in dataChannels) {
            const dc = dataChannels[peerId];
            if (dc.readyState !== 'open') continue;
            
            // Backpressure: Ждем, если буфер переполнен (защита от потери пакетов)
            if (dc.bufferedAmount > dc.bufferedAmountLowThreshold) {
                await new Promise(resolve => {
                    const listener = () => { dc.removeEventListener('bufferedamountlow', listener); resolve(); };
                    dc.addEventListener('bufferedamountlow', listener);
                });
            }
            dc.send(buffer);
        }
        offset += buffer.byteLength;
        updateProgressUI(msgId, Math.floor((offset / file.size) * 100));
    }

    Object.values(dataChannels).forEach(dc => dc.send(JSON.stringify({ type: 'file_end', id: msgId })));
    finalizeSendingUI(msgId, file);
}

// Сборка файла получателем
function assembleAndRenderFile(peerId) {
    const transfer = incomingFiles[peerId];
    const blob = new Blob(transfer.chunks, { type: transfer.info.type });
    const url = URL.createObjectURL(blob);
    
    // Заменяем блок загрузки на финальный файл
    const wrapper = document.getElementById(`wrapper-${transfer.id}`);
    wrapper.outerHTML = getFileHTML(url, transfer.info.name, transfer.info.type, false);
    
    // Отправляем отчет о получении
    ws.send(JSON.stringify({ type: 'read_receipt', content: transfer.id, target_id: peerId }));
    delete incomingFiles[peerId];
}

// === UI LOGIC ===
function sendText() {
    const text = messageInput.value.trim();
    if (!text || !ws) return;
    
    const msgId = generateId();
    ws.send(JSON.stringify({ id: msgId, type: 'text', content: text }));
    renderMessage({ id: msgId, type: 'text', content: text, sender_id: myClientId });
    messageInput.value = '';
}

document.getElementById('send-btn').onclick = sendText;
messageInput.onkeypress = (e) => { if(e.key === 'Enter') sendText(); };

const fileInput = document.getElementById('file-input');
document.getElementById('file-btn').onclick = () => fileInput.click();
fileInput.onchange = (e) => { if (e.target.files.length > 0) sendFileP2P(e.target.files[0]); };

function renderMessage(msg) {
    document.querySelector('.empty-state')?.remove();
    const div = document.createElement('div');
    
    if (msg.type === 'system') {
        div.className = 'msg system'; div.innerText = msg.content;
    } else {
        const isMine = msg.sender_id === myClientId;
        div.className = `msg ${isMine ? 'mine' : 'theirs'}`;
        
        let contentHtml = '';
        if (msg.content && msg.content.startsWith('http')) {
            contentHtml = `<a href="${msg.content}" target="_blank" style="color:inherit; text-decoration:underline;">${msg.content}</a>`;
        } else {
            contentHtml = msg.content;
        }

        // Добавляем галочки для своих сообщений
        const statusHTML = isMine ? `<span id="status-${msg.id}" class="msg-status">✓</span>` : '';
        div.innerHTML = `<div>${contentHtml}</div> ${statusHTML}`;
    }
    messagesDiv.appendChild(div);
    chatArea.scrollTop = chatArea.scrollHeight;
}

// UI для отправляемого файла
function renderOutgoingFileUI(msgId, file) {
    document.querySelector('.empty-state')?.remove();
    const div = document.createElement('div');
    div.className = 'msg mine file-progress-wrap';
    div.id = `wrapper-${msgId}`;
    div.innerHTML = `
        <div class="file-info">📤 Отправка: ${file.name}</div>
        <div class="progress-bar"><div class="progress-fill" id="fill-${msgId}"></div></div>
        <div class="progress-text"><span id="text-${msgId}">0</span>%</div>
    `;
    messagesDiv.appendChild(div);
    chatArea.scrollTop = chatArea.scrollHeight;
}

// UI для принимаемого файла
function renderIncomingFileUI(msgId, filename, peerId) {
    document.querySelector('.empty-state')?.remove();
    const div = document.createElement('div');
    div.className = 'msg theirs file-progress-wrap';
    div.id = `wrapper-${msgId}`;
    div.innerHTML = `
        <div class="file-info">📥 Получение: ${filename}</div>
        <div class="progress-bar"><div class="progress-fill" id="fill-${msgId}"></div></div>
        <div class="progress-text"><span id="text-${msgId}">0</span>%</div>
    `;
    messagesDiv.appendChild(div);
    chatArea.scrollTop = chatArea.scrollHeight;
}

function updateProgressUI(msgId, percent) {
    const fill = document.getElementById(`fill-${msgId}`);
    const text = document.getElementById(`text-${msgId}`);
    if (fill && text) {
        fill.style.width = `${percent}%`;
        text.innerText = percent;
    }
}

function finalizeSendingUI(msgId, file) {
    const wrapper = document.getElementById(`wrapper-${msgId}`);
    if(wrapper) {
        const localUrl = URL.createObjectURL(file);
        wrapper.outerHTML = getFileHTML(localUrl, file.name, file.type, true, msgId);
    }
}

function getFileHTML(url, name, type, isMine, msgId = '') {
    const isImage = type.startsWith('image/');
    const icon = `<svg viewBox="0 0 24 24" width="20" height="20" stroke="currentColor" stroke-width="2" fill="none"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><polyline points="13 2 13 9 20 9"></polyline></svg>`;
    
    let contentHTML = `<a href="${url}" download="${name}" class="msg-file-link">${icon} ${name}</a>`;
    if (isImage) contentHTML += `<a href="${url}" target="_blank"><img src="${url}" class="msg-image" alt="preview"></a>`;
    
    const statusHTML = isMine ? `<span id="status-${msgId}" class="msg-status">✓</span>` : '';
    return `<div class="msg ${isMine ? 'mine' : 'theirs'}"><div>${contentHTML}</div>${statusHTML}</div>`;
}

function switchScreen(screen) {
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    screen.classList.add('active');
}

function triggerDissolve() {
    roomScreen.classList.add('dissolve');
    setTimeout(() => {
        switchScreen(document.getElementById('destroyed-screen'));
        roomScreen.classList.remove('dissolve');
        messagesDiv.innerHTML = '';
        currentRoom = '';
        window.history.replaceState(null, '', '/');
        if(ws) ws.close();
        Object.values(peers).forEach(pc => pc.close());
        peers = {}; dataChannels = {};
    }, 1000);
}

document.getElementById('leave-btn').onclick = triggerDissolve;