let ws;
let currentRoom = '';
let myClientId = '';
let isOwner = false;
let peers = {}; 
let dataChannels = {}; 
let incomingFiles = {}; 

const CHUNK_SIZE = 16384 * 4; 

const authScreen = document.getElementById('auth-screen');
const roomScreen = document.getElementById('room-screen');
const destroyedScreen = document.getElementById('destroyed-screen');
const displayRoomCode = document.getElementById('display-room-code');
const messageInput = document.getElementById('message-input');
const messagesDiv = document.getElementById('messages');
const chatArea = document.getElementById('chat-area');
const dropOverlay = document.getElementById('drop-overlay');

const generateId = () => 'msg_' + Math.random().toString(36).substr(2, 9);

// Toast уведомления
function showToast(msg, type = 'error') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerText = msg;
    container.appendChild(toast);
    setTimeout(() => toast.remove(), 3000);
}

window.onload = () => {
    const roomParam = new URLSearchParams(window.location.search).get('room');
    if (roomParam) connect('join', roomParam.toUpperCase());
};

document.getElementById('create-btn').onclick = () => connect('create');
document.getElementById('join-btn').onclick = () => {
    const code = document.getElementById('room-code-input').value.trim().toUpperCase();
    if(code) connect('join', code);
    else showToast("Введите код комнаты");
};

displayRoomCode.onclick = () => {
    navigator.clipboard.writeText(`${window.location.origin}/?room=${currentRoom}`);
    showToast('Ссылка скопирована', 'success');
};

function connect(action, roomCode = '') {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    let url = `${protocol}//${window.location.host}/ws?action=${action}`;
    if (roomCode) url += `&room=${roomCode}`;

    ws = new WebSocket(url);

    ws.onmessage = async (event) => {
        const data = JSON.parse(event.data);
        
        if (data.type === 'error') {
            showToast(data.content);
            ws.close();
            return;
        }

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
            const statusEl = document.getElementById(`status-${data.content}`);
            if (statusEl) {
                statusEl.innerText = '✓✓';
                statusEl.style.color = '#30d158'; // Зеленый цвет для прочитанного
            }
        } else if (data.type === 'room_destroyed') {
            triggerDissolve();
        } else {
            renderMessage(data);
            if (data.type === 'text' && data.id) {
                ws.send(JSON.stringify({ type: 'read_receipt', content: data.id, target_id: data.sender_id }));
            }
        }
    };
    ws.onclose = () => { if(roomScreen.classList.contains('active')) triggerDissolve(); };
}

// === WEBRTC LOGIC (без изменений) ===
const configuration = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };
async function initWebRTC(peerId, isInitiator) {
    const pc = new RTCPeerConnection(configuration);
    peers[peerId] = pc;
    pc.onicecandidate = (e) => { if (e.candidate) ws.send(JSON.stringify({ type: 'signal', target_id: peerId, signal_data: { candidate: e.candidate } })); };
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
    dc.bufferedAmountLowThreshold = 1024 * 512;
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
            updateProgressUI(transfer.id, Math.floor((transfer.receivedSize / transfer.info.size) * 100));
        }
    };
}

async function sendFileP2P(file) {
    if (Object.keys(dataChannels).length === 0) return showToast("Нет участников для отправки");
    const msgId = generateId();
    renderOutgoingFileUI(msgId, file);
    Object.values(dataChannels).forEach(dc => dc.send(JSON.stringify({ type: 'file_start', id: msgId, info: { name: file.name, type: file.type, size: file.size } })));

    let offset = 0;
    while (offset < file.size) {
        const chunk = file.slice(offset, offset + CHUNK_SIZE);
        const buffer = await chunk.arrayBuffer();
        for (let peerId in dataChannels) {
            const dc = dataChannels[peerId];
            if (dc.readyState !== 'open') continue;
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

function assembleAndRenderFile(peerId) {
    const transfer = incomingFiles[peerId];
    const blob = new Blob(transfer.chunks, { type: transfer.info.type });
    const url = URL.createObjectURL(blob);
    const wrapper = document.getElementById(`wrapper-${transfer.id}`);
    wrapper.outerHTML = getFileHTML(url, transfer.info.name, transfer.info.type, false);
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

roomScreen.addEventListener('dragover', (e) => { e.preventDefault(); dropOverlay.classList.add('active'); });
roomScreen.addEventListener('dragleave', (e) => { e.preventDefault(); if (e.relatedTarget === null) dropOverlay.classList.remove('active'); });
roomScreen.addEventListener('drop', (e) => { e.preventDefault(); dropOverlay.classList.remove('active'); if (e.dataTransfer.files.length > 0) sendFileP2P(e.dataTransfer.files[0]); });

function renderMessage(msg) {
    document.querySelector('.empty-state')?.remove();
    const div = document.createElement('div');
    if (msg.type === 'system') {
        div.className = 'msg system'; div.innerText = msg.content;
    } else {
        const isMine = msg.sender_id === myClientId;
        div.className = `msg ${isMine ? 'mine' : 'theirs'}`;
        let contentHtml = msg.content.startsWith('http') ? `<a href="${msg.content}" target="_blank" style="color:inherit; text-decoration:underline;">${msg.content}</a>` : msg.content;
        const statusHTML = isMine ? `<span id="status-${msg.id}" class="msg-status">✓</span>` : '';
        div.innerHTML = `<div>${contentHtml}</div> ${statusHTML}`;
    }
    messagesDiv.appendChild(div);
    chatArea.scrollTop = chatArea.scrollHeight;
}

function renderOutgoingFileUI(msgId, file) {
    document.querySelector('.empty-state')?.remove();
    messagesDiv.insertAdjacentHTML('beforeend', `<div class="msg mine file-progress-wrap" id="wrapper-${msgId}">
        <div class="file-info">📤 Отправка: ${file.name}</div>
        <div class="progress-bar"><div class="progress-fill" id="fill-${msgId}"></div></div>
        <div class="progress-text"><span id="text-${msgId}">0</span>%</div>
    </div>`);
    chatArea.scrollTop = chatArea.scrollHeight;
}
function renderIncomingFileUI(msgId, filename) {
    document.querySelector('.empty-state')?.remove();
    messagesDiv.insertAdjacentHTML('beforeend', `<div class="msg theirs file-progress-wrap" id="wrapper-${msgId}">
        <div class="file-info">📥 Получение: ${filename}</div>
        <div class="progress-bar"><div class="progress-fill" id="fill-${msgId}"></div></div>
        <div class="progress-text"><span id="text-${msgId}">0</span>%</div>
    </div>`);
    chatArea.scrollTop = chatArea.scrollHeight;
}
function updateProgressUI(msgId, percent) {
    const fill = document.getElementById(`fill-${msgId}`), text = document.getElementById(`text-${msgId}`);
    if (fill && text) { fill.style.width = `${percent}%`; text.innerText = percent; }
}
function finalizeSendingUI(msgId, file) {
    const wrapper = document.getElementById(`wrapper-${msgId}`);
    if(wrapper) wrapper.outerHTML = getFileHTML(URL.createObjectURL(file), file.name, file.type, true, msgId);
}
function getFileHTML(url, name, type, isMine, msgId = '') {
    const icon = `<svg viewBox="0 0 24 24" width="20" height="20" stroke="currentColor" stroke-width="2" fill="none"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><polyline points="13 2 13 9 20 9"></polyline></svg>`;
    let contentHTML = `<a href="${url}" download="${name}" class="msg-file-link">${icon} ${name}</a>`;
    if (type.startsWith('image/')) contentHTML += `<a href="${url}" target="_blank"><img src="${url}" class="msg-image" alt="preview"></a>`;
    return `<div class="msg ${isMine ? 'mine' : 'theirs'}"><div>${contentHTML}</div>${isMine ? `<span id="status-${msgId}" class="msg-status">✓</span>` : ''}</div>`;
}

function switchScreen(screen) {
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    screen.classList.add('active');
}

// Новый механизм очистки без перезагрузки всей страницы
function resetApp() {
    switchScreen(authScreen);
    messagesDiv.innerHTML = '';
    currentRoom = '';
    window.history.replaceState(null, '', '/');
    if(ws) { ws.onclose = null; ws.close(); ws = null; }
    Object.values(peers).forEach(pc => pc.close());
    peers = {}; dataChannels = {}; incomingFiles = {};
    document.getElementById('room-code-input').value = '';
}

function triggerDissolve() {
    roomScreen.classList.add('dissolve');
    setTimeout(() => {
        switchScreen(destroyedScreen);
        roomScreen.classList.remove('dissolve');
    }, 1000);
}

document.getElementById('leave-btn').onclick = () => {
    if(ws) ws.close(); // Спровоцирует onclose -> triggerDissolve()
};
document.getElementById('return-btn').onclick = resetApp;