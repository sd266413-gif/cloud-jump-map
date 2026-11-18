'use strict';
/**
 * 구름섬 점프맵 서버
 * - index.html, admin.html 서빙 (같은 폴더)
 * - WebSocket 실시간 위치 동기화
 * - 랭킹 저장 (scores.json)
 *
 * 실행: npm install && npm start
 * 환경변수: PORT(기본 3000), MAX_PLAYERS(기본 100), ALLOWED_ORIGINS(쉼표로 구분, 비우면 모두 허용)
 *          ADMIN_USER(기본 admin), ADMIN_PASSWORD(없으면 실행할 때마다 임시 비밀번호를 출력)
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const MAX_PLAYERS = Number(process.env.MAX_PLAYERS) || 100;
const ALLOWED = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const STATIC_FILES = new Set(['index.html', 'admin.html']);   // 브라우저에 내려줄 파일 (server.js 등은 노출하지 않음)
const SCORE_FILE = path.join(__dirname, 'scores.json');
const MIN_TIME = 40;          // 이보다 빠른 기록은 비정상으로 보고 거부 (초)
const TICK_MS = 66;           // 위치 방송 주기 (약 15회/초)
const TOP_N = 10;

// ---------- 랭킹 ----------
/** key(토큰 해시) -> { name, time, falls, at } */
const scores = new Map();
try {
  const raw = JSON.parse(fs.readFileSync(SCORE_FILE, 'utf8'));
  for (const [k, v] of Object.entries(raw)) scores.set(k, v);
} catch (e) { /* 파일이 없으면 새로 시작 */ }

let saveTimer = null;
function saveScores(now = false) {
  const write = () => {
    saveTimer = null;
    const tmp = SCORE_FILE + '.tmp';
    try {
      fs.writeFileSync(tmp, JSON.stringify(Object.fromEntries(scores)));
      fs.renameSync(tmp, SCORE_FILE);
    } catch (e) { console.error('랭킹 저장 실패:', e.message); }
  };
  if (now) { if (saveTimer) clearTimeout(saveTimer); write(); }
  else if (!saveTimer) saveTimer = setTimeout(write, 1000);
}

function topScores() {
  return [...scores.entries()]
    .sort((a, b) => a[1].time - b[1].time)
    .slice(0, TOP_N)
    .map(([k, v]) => ({ k, n: v.name, t: v.time, f: v.falls }));
}

// ---------- HTTP (정적 파일 + 간단한 API) ----------
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.ico': 'image/x-icon',
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname.startsWith('/api/admin/')) return handleAdmin(req, res, url);
  if (url.pathname === '/health') { res.writeHead(200); return res.end('ok'); }
  if (url.pathname === '/api/scores') {
    res.writeHead(200, { 'Content-Type': MIME['.json'], 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify(topScores()));
  }
  let rel;
  try { rel = decodeURIComponent(url.pathname); } catch (e) { res.writeHead(400); return res.end(); }
  const name = rel === '/' ? 'index.html' : rel.slice(1);
  if (!STATIC_FILES.has(name)) { res.writeHead(404); return res.end('Not found'); }
  const file = path.join(__dirname, name);
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(buf);
  });
});

// ---------- 관리자 ----------
const ADMIN_USER = process.env.ADMIN_USER || 'admin';
let ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';
let passwordGenerated = false;
if (!ADMIN_PASSWORD) { ADMIN_PASSWORD = crypto.randomBytes(9).toString('base64url'); passwordGenerated = true; }
const SESSION_MS = 2 * 60 * 60 * 1000;
const sessions = new Map();   // 토큰 -> 만료 시각
const failed = new Map();     // IP -> { n, until }
const banned = new Map();     // 플레이어 key -> 해제 시각

const sha = s => crypto.createHash('sha256').update(String(s)).digest();
const safeEqual = (a, b) => crypto.timingSafeEqual(sha(a), sha(b));
const json = (res, code, obj) => {
  res.writeHead(code, { 'Content-Type': MIME['.json'], 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
};
const readJson = req => new Promise(resolve => {
  let b = '';
  req.on('data', c => { b += c; if (b.length > 2048) req.destroy(); });
  req.on('end', () => { try { resolve(JSON.parse(b || '{}')); } catch (e) { resolve(null); } });
  req.on('error', () => resolve(null));
});

async function handleAdmin(req, res, url) {
  const route = url.pathname.slice('/api/admin/'.length);
  const ip = req.socket.remoteAddress;

  if (route === 'login' && req.method === 'POST') {
    const f = failed.get(ip);
    if (f && f.until > Date.now()) return json(res, 429, { error: '시도가 너무 많아요. 10분 뒤에 다시 해 주세요.' });
    const b = await readJson(req);
    const okUser = !!b && typeof b.user === 'string' && safeEqual(b.user, ADMIN_USER);
    const okPass = !!b && typeof b.password === 'string' && safeEqual(b.password, ADMIN_PASSWORD);
    if (!(okUser && okPass)) {
      const n = (f ? f.n : 0) + 1;
      failed.set(ip, { n, until: n >= 5 ? Date.now() + 10 * 60 * 1000 : 0 });
      return json(res, 401, { error: '아이디 또는 비밀번호가 맞지 않아요.' });
    }
    failed.delete(ip);
    const token = crypto.randomBytes(24).toString('hex');
    sessions.set(token, Date.now() + SESSION_MS);
    return json(res, 200, { token });
  }

  const auth = (req.headers.authorization || '').replace(/^Bearer /, '');
  const exp = sessions.get(auth);
  if (!exp || exp < Date.now()) { sessions.delete(auth); return json(res, 401, { error: '로그인이 필요해요.' }); }

  if (route === 'logout' && req.method === 'POST') { sessions.delete(auth); return json(res, 200, { ok: true }); }

  if (route === 'state' && req.method === 'GET') {
    return json(res, 200, {
      players: [...players.values()].filter(p => p.ready).map(p => ({ id: p.id, n: p.n, x: p.x, y: p.y, z: p.z })),
      scores: [...scores.entries()].sort((a, b) => a[1].time - b[1].time).slice(0, 200)
        .map(([k, v]) => ({ k, n: v.name, t: v.time, f: v.falls, at: v.at })),
    });
  }

  if (req.method !== 'POST') return json(res, 405, { error: 'POST만 가능해요.' });
  const b = await readJson(req);
  if (!b) return json(res, 400, { error: '잘못된 요청이에요.' });

  if (route === 'reset-scores') {
    scores.clear(); saveScores(true);
    broadcast({ t: 'scores', scores: [] });
    console.log('[관리자] 랭킹 전체 초기화');
    return json(res, 200, { ok: true });
  }
  if (route === 'delete-score' && typeof b.k === 'string') {
    const had = scores.delete(b.k);
    if (had) { saveScores(true); broadcast({ t: 'scores', scores: topScores() }); }
    console.log('[관리자] 기록 삭제:', b.k);
    return json(res, 200, { ok: had });
  }
  if (route === 'kick' && typeof b.id === 'string') {
    const p = players.get(b.id);
    if (!p) return json(res, 404, { error: '접속 중이 아니에요.' });
    if (p.key) banned.set(p.key, Date.now() + 10 * 60 * 1000);   // 10분간 재접속 차단
    p.ws.close(4000, '관리자에 의해 퇴장');
    console.log('[관리자] 강퇴:', p.n);
    return json(res, 200, { ok: true });
  }
  return json(res, 404, { error: '없는 기능이에요.' });
}

// ---------- WebSocket ----------
const wss = new WebSocketServer({
  server,
  maxPayload: 1024,
  verifyClient: ({ origin }) => ALLOWED.length === 0 || ALLOWED.includes(origin),
});

/** id -> player */
const players = new Map();
let seq = 0;
let dirty = false;

const num = (v, lim = 1000) => (typeof v === 'number' && Number.isFinite(v) && Math.abs(v) < lim ? v : null);
const cleanName = s => String(s ?? '')
  .replace(/[\u0000-\u001f\u007f-\u009f\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]/g, '')
  .trim().slice(0, 12) || '익명';
const send = (ws, obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };
const broadcast = obj => {
  const s = JSON.stringify(obj);
  for (const p of players.values()) if (p.ready && p.ws.readyState === 1) p.ws.send(s);
};

wss.on('connection', ws => {
  if (players.size >= MAX_PLAYERS) return ws.close(1013, '서버가 가득 찼어요');
  const id = String(++seq);
  const p = { id, ws, ready: false, key: null, n: '익명', c: 0, x: 0, y: 0, z: 0, a: 0, bucket: 120, refill: Date.now(), alive: true };
  players.set(id, p);

  ws.on('pong', () => { p.alive = true; });

  ws.on('message', data => {
    // 속도 제한: 초당 약 80개, 순간 최대 120개
    const now = Date.now();
    p.bucket = Math.min(120, p.bucket + (now - p.refill) * 0.08);
    p.refill = now;
    if (--p.bucket < 0) return;

    let m;
    try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    switch (m.t) {
      case 'hello': {
        if (typeof m.token !== 'string' || m.token.length < 8 || m.token.length > 64) return;
        p.key = crypto.createHash('sha256').update(m.token).digest('hex').slice(0, 10);
        const ban = banned.get(p.key);
        if (ban && ban > Date.now()) return ws.close(4000, '관리자에 의해 차단됨');
        p.n = cleanName(m.name);
        p.c = (num(m.hue, 100000) ?? 0) % 360;
        p.ready = true;
        send(ws, { t: 'welcome', id, key: p.key, scores: topScores() });
        dirty = true;
        break;
      }
      case 'name': {
        if (!p.ready) return;
        p.n = cleanName(m.name);
        dirty = true;
        break;
      }
      case 'pos': {
        if (!p.ready) return;
        const x = num(m.x), y = num(m.y), z = num(m.z), a = num(m.a, 100);
        if (x === null || y === null || z === null || a === null) return;
        p.x = x; p.y = y; p.z = z; p.a = a;
        dirty = true;
        break;
      }
      case 'score': {
        if (!p.ready) return;
        const time = num(m.time, 36000), falls = num(m.falls, 10000);
        if (time === null || falls === null || time < MIN_TIME) return;
        const t = Math.round(time * 10) / 10;
        const old = scores.get(p.key);
        if (!old || t < old.time) {
          scores.set(p.key, { name: p.n, time: t, falls: Math.max(0, Math.floor(falls)), at: Date.now() });
          saveScores();
          broadcast({ t: 'scores', scores: topScores() });
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    players.delete(id);
    if (p.ready) { broadcast({ t: 'leave', id }); dirty = true; }
  });
  ws.on('error', () => ws.terminate());
});

// 위치 방송
setInterval(() => {
  if (!dirty) return;
  dirty = false;
  const list = [];
  for (const p of players.values()) {
    if (!p.ready) continue;
    list.push({
      id: p.id, n: p.n, c: p.c,
      x: Math.round(p.x * 100) / 100, y: Math.round(p.y * 100) / 100,
      z: Math.round(p.z * 100) / 100, a: Math.round(p.a * 100) / 100,
    });
  }
  broadcast({ t: 'players', p: list });
}, TICK_MS);

// 끊긴 연결 정리
setInterval(() => {
  for (const p of players.values()) {
    if (!p.alive) { p.ws.terminate(); continue; }
    p.alive = false;
    p.ws.ping();
  }
}, 30000);

// ---------- 시작 / 종료 ----------
server.listen(PORT, () => {
  console.log(`서버 실행 중: http://localhost:${PORT}`);
  console.log(`관리자 페이지: http://localhost:${PORT}/admin.html (아이디: ${ADMIN_USER})`);
  if (passwordGenerated) console.log(`임시 관리자 비밀번호: ${ADMIN_PASSWORD}  (고정하려면 ADMIN_PASSWORD 환경변수를 설정하세요)`);
});

function shutdown() {
  saveScores(true);
  for (const p of players.values()) p.ws.close(1001, '서버 종료');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
