const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const rooms = new Map();

app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.json({ ok: true, rooms: rooms.size }));

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}
function roomCode() { return crypto.randomBytes(3).toString('hex').toUpperCase(); }
function broadcast(room, from, data) {
  for (const peer of room.peers) if (peer !== from) send(peer, data);
}

wss.on('connection', (ws) => {
  ws.id = crypto.randomUUID();
  ws.roomCode = null;
  ws.role = null;
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === 'create') {
      let code;
      do { code = roomCode(); } while (rooms.has(code));
      const room = { peers: new Set([ws]), host: ws };
      rooms.set(code, room);
      ws.roomCode = code; ws.role = 'host';
      send(ws, { type: 'created', code });
      return;
    }

    if (msg.type === 'join') {
      const code = String(msg.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6);
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'error', message: 'Room not found. Ask the host to create a new room.' });
      if (room.peers.size >= 2) return send(ws, { type: 'error', message: 'This room already has a viewer.' });
      room.peers.add(ws); ws.roomCode = code; ws.role = 'viewer';
      send(ws, { type: 'joined', code });
      send(room.host, { type: 'peer-joined' });
      return;
    }

    const room = rooms.get(ws.roomCode);
    if (!room) return;
    if (['offer', 'answer', 'ice', 'play', 'pause', 'seek', 'sync', 'chat', 'movie-ready'].includes(msg.type)) {
      if (msg.type === 'chat') {
        const text = String(msg.text || '').trim().slice(0, 500);
        if (!text) return;
        const packet = { type: 'chat', text, from: ws.role, at: Date.now() };
        for (const peer of room.peers) send(peer, packet);
      } else {
        broadcast(room, ws, msg);
      }
    }
  });

  ws.on('close', () => {
    const code = ws.roomCode;
    const room = rooms.get(code);
    if (!room) return;
    room.peers.delete(ws);
    broadcast(room, ws, { type: 'peer-left' });
    if (room.peers.size === 0 || room.host === ws) {
      for (const peer of room.peers) send(peer, { type: 'room-closed' });
      rooms.delete(code);
    }
  });
});

const port = process.env.PORT || 3000;
server.listen(port, '0.0.0.0', () => console.log(`Watch Together listening on ${port}`));
