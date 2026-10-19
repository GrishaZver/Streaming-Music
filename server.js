'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const ROOT = __dirname;
const APP_NAME = 'telegram_beats_player';
const AUDIUS_NODES = [
  'https://discoveryprovider.audius.co/v1',
  'https://api.audius.co/v1'
];
const AUDIO_EXT = /\.(mp3|m4a|aac|ogg|oga|opus|flac|wav)$/i;

class HttpError extends Error { constructor(status, message) { super(message); this.status = status; } }
function json(res, status, data) {
  const body = Buffer.from(JSON.stringify(data));
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': body.length, 'Cache-Control': 'no-store' });
  res.end(body);
}
function sendText(res, status, text) {
  const body = Buffer.from(text);
  res.writeHead(status, { 'Content-Type': 'text/plain; charset=utf-8', 'Content-Length': body.length });
  res.end(body);
}
async function fetchTimed(url, options = {}, timeoutMs = 7000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal, redirect: 'follow', headers: { 'User-Agent': 'TelegramBeats/1.0 (+self-hosted music player)', ...(options.headers || {}) } });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response;
  } catch (error) {
    if (error.name === 'AbortError') throw new Error(`тайм-аут ${Math.ceil(timeoutMs / 1000)}с`);
    throw error;
  } finally { clearTimeout(timer); }
}
async function fetchJson(url, timeoutMs = 7000) {
  const response = await fetchTimed(url, { headers: { Accept: 'application/json' } }, timeoutMs);
  return response.json();
}
function asTrack(source, item) {
  return {
    id: `${source}:${item.id}`,
    source,
    sourceId: String(item.id),
    fileName: item.fileName || '',
    title: String(item.title || 'Без названия'),
    ch: String(item.ch || 'Неизвестный исполнитель'),
    thumb: item.thumb || '',
    duration: Number(item.duration || 0)
  };
}
function safeWords(query) {
  return String(query || '').normalize('NFKD').replace(/[^\p{L}\p{N}\s-]/gu, ' ').trim().split(/\s+/).filter(Boolean).slice(0, 5);
}
async function searchAudius(query, limit) {
  let lastError;
  for (const base of AUDIUS_NODES) {
    try {
      let url = new URL(base + (query ? '/tracks/search' : '/tracks/trending'));
      url.searchParams.set('app_name', APP_NAME);
      url.searchParams.set('limit', String(Math.max(1, Math.min(limit, 25))));
      if (query) url.searchParams.set('query', query);
      else url.searchParams.set('time', 'week');
      let data = await fetchJson(url.toString(), 6500);
      let items = Array.isArray(data.data) ? data.data : (Array.isArray(data.results) ? data.results : []);
      if (!items.length && query) {
        url = new URL(base + '/tracks/trending');
        url.searchParams.set('app_name', APP_NAME); url.searchParams.set('time', 'week'); url.searchParams.set('limit', String(Math.min(limit, 25)));
        data = await fetchJson(url.toString(), 6500);
        items = Array.isArray(data.data) ? data.data : [];
      }
      const tracks = items.filter(x => x && x.id && x.is_streamable !== false).map(x => {
        const art = x.artwork || {};
        return asTrack('audius', { id: x.id, title: x.title, ch: x.user?.name || x.artist || 'Audius', duration: x.duration, thumb: art['480x480'] || art['150x150'] || art['1000x1000'] || '' });
      });
      if (tracks.length) return tracks;
      lastError = new Error('Audius не вернул треки');
    } catch (error) { lastError = error; }
  }
  throw lastError || new Error('Audius недоступен');
}
async function searchArchive(query, limit) {
  const words = safeWords(query);
  let term = 'mediatype:audio AND (title:music OR subject:music OR title:live OR subject:audio)';
  if (words.length) {
    const clauses = words.map(word => `(title:${word} OR creator:${word} OR subject:${word})`);
    term = 'mediatype:audio AND (' + clauses.join(' OR ') + ')';
  }
  const url = new URL('https://archive.org/advancedsearch.php');
  url.searchParams.set('q', term);
  for (const field of ['identifier', 'title', 'creator']) url.searchParams.append('fl[]', field);
  url.searchParams.set('rows', String(Math.min(Math.max(limit, 4), 12)));
  url.searchParams.set('page', '1'); url.searchParams.set('output', 'json');
  url.searchParams.append('sort[]', 'downloads desc');
  const result = await fetchJson(url.toString(), 8000);
  const docs = result.response?.docs || [];
  const tracks = await Promise.all(docs.slice(0, Math.min(limit, 8)).map(async doc => {
    try {
      const metadata = await fetchJson('https://archive.org/metadata/' + encodeURIComponent(doc.identifier), 7000);
      const files = (metadata.files || []).filter(file => AUDIO_EXT.test(file.name || '') && !String(file.name).includes('/') && file.name !== '__ia_thumb.jpg');
      files.sort((a, b) => {
        const score = file => (/\.mp3$/i.test(file.name) ? 5 : /\.m4a$/i.test(file.name) ? 4 : /\.ogg$/i.test(file.name) ? 3 : /\.flac$/i.test(file.name) ? 2 : 1) + (file.source === 'original' ? 0.2 : 0);
        return score(b) - score(a);
      });
      const file = files[0];
      if (!file) return null;
      const creator = Array.isArray(doc.creator) ? doc.creator.join(', ') : (doc.creator || 'Internet Archive');
      return asTrack('archive', { id: doc.identifier, fileName: file.name, title: doc.title || doc.identifier, ch: creator, duration: file.duration, thumb: `https://archive.org/services/img/${encodeURIComponent(doc.identifier)}` });
    } catch { return null; }
  }));
  return tracks.filter(Boolean);
}
async function searchAll(query, limit, wave) {
  const tasks = [];
  // In wave mode use trending when the client sends no specific search phrase.
  tasks.push(searchAudius(query, limit));
  tasks.push(searchArchive(query, Math.min(limit, 8)));
  const results = await Promise.allSettled(tasks);
  const tracks = results.filter(result => result.status === 'fulfilled').flatMap(result => result.value);
  const errors = results.filter(result => result.status === 'rejected').map(result => result.reason?.message || 'Источник недоступен');
  const seen = new Set();
  const unique = tracks.filter(track => { if (seen.has(track.id)) return false; seen.add(track.id); return true; });
  return { tracks: unique.slice(0, limit), errors };
}
function parseUrl(req) { return new URL(req.url, `http://${req.headers.host || 'localhost'}`); }
async function proxyAudio(req, res, url) {
  const provider = url.searchParams.get('provider');
  const id = url.searchParams.get('id') || '';
  if (!id || id.length > 300 || /[\r\n]/.test(id)) throw new HttpError(400, 'Некорректный ID трека');
  let upstreamUrl;
  if (provider === 'audius') {
    const base = AUDIUS_NODES[0];
    upstreamUrl = new URL(base + '/tracks/' + encodeURIComponent(id) + '/stream');
    upstreamUrl.searchParams.set('app_name', APP_NAME);
  } else if (provider === 'archive') {
    const file = url.searchParams.get('file') || '';
    if (!file || file.length > 255 || file.includes('/') || file.includes('\\') || file === '.' || file === '..' || file.includes('..')) throw new HttpError(400, 'Некорректное имя файла');
    upstreamUrl = new URL('https://archive.org/download/' + encodeURIComponent(id) + '/' + encodeURIComponent(file));
  } else throw new HttpError(400, 'Неизвестный источник');

  const headers = { Accept: req.headers.accept || 'audio/*,*/*;q=0.8' };
  if (req.headers.range) headers.Range = req.headers.range;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15000);
  let upstream;
  try {
    upstream = await fetch(upstreamUrl, { method: req.method === 'HEAD' ? 'HEAD' : 'GET', signal: controller.signal, redirect: 'follow', headers: { ...headers, 'User-Agent': 'TelegramBeats/1.0' } });
  } catch (error) {
    if (!res.headersSent) json(res, 502, { error: error.name === 'AbortError' ? 'Источник аудио не ответил вовремя' : 'Не удалось подключиться к источнику аудио' });
    return;
  } finally { clearTimeout(timer); }
  if (!upstream.ok && upstream.status !== 206) {
    await upstream.body?.cancel().catch(() => {});
    json(res, upstream.status === 404 ? 404 : 502, { error: `Источник аудио ответил HTTP ${upstream.status}` });
    return;
  }
  const responseHeaders = {
    'Content-Type': upstream.headers.get('content-type') || 'audio/mpeg',
    'Accept-Ranges': upstream.headers.get('accept-ranges') || 'bytes',
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*'
  };
  for (const key of ['content-length', 'content-range', 'last-modified', 'etag']) {
    const value = upstream.headers.get(key); if (value) responseHeaders[key] = value;
  }
  res.writeHead(upstream.status, responseHeaders);
  if (req.method === 'HEAD' || !upstream.body) { res.end(); return; }
  try { await pipeline(Readable.fromWeb(upstream.body), res); }
  catch (error) { if (!res.destroyed) res.destroy(error); }
}
async function health() {
  const checks = await Promise.allSettled([
    fetchJson('https://discoveryprovider.audius.co/v1/tracks/trending?app_name=' + APP_NAME + '&time=week&limit=1', 4500),
    fetchJson('https://archive.org/advancedsearch.php?q=mediatype%3Aaudio&rows=1&output=json', 4500)
  ]);
  const providers = { audius: checks[0].status === 'fulfilled', archive: checks[1].status === 'fulfilled' };
  return { ok: Object.values(providers).some(Boolean), providers, message: Object.values(providers).some(Boolean) ? 'Хотя бы один онлайн-источник доступен' : 'Сервер работает, но музыкальные источники не отвечают' };
}
const server = http.createServer(async (req, res) => {
  try {
    const url = parseUrl(req);
    if (req.method === 'OPTIONS') { res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,HEAD,OPTIONS', 'Access-Control-Allow-Headers': 'Range,Content-Type' }); res.end(); return; }
    if ((req.method === 'GET' || req.method === 'HEAD') && url.pathname === '/api/health') { json(res, 200, await health()); return; }
    if (req.method === 'GET' && url.pathname === '/api/search') {
      const query = (url.searchParams.get('query') || '').trim().slice(0, 120);
      const limit = Math.max(1, Math.min(Number(url.searchParams.get('limit') || 20), 30));
      const result = await searchAll(query, limit, url.searchParams.get('wave') === '1');
      json(res, 200, result); return;
    }
    if ((req.method === 'GET' || req.method === 'HEAD') && url.pathname === '/api/stream') { await proxyAudio(req, res, url); return; }
    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
      const body = fs.readFileSync(path.join(ROOT, 'index.html'));
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Content-Length': body.length, 'Cache-Control': 'no-cache' }); res.end(body); return;
    }
    if (req.method === 'GET' && /^\/(manifest\.json|icon-192\.png|sw\.js)$/.test(url.pathname)) {
      const file = path.join(ROOT, url.pathname.slice(1));
      if (!fs.existsSync(file)) { sendText(res, 404, 'Not found'); return; }
      const type = file.endsWith('.js') ? 'text/javascript; charset=utf-8' : file.endsWith('.json') ? 'application/json; charset=utf-8' : 'image/png';
      res.writeHead(200, { 'Content-Type': type }); fs.createReadStream(file).pipe(res); return;
    }
    sendText(res, 404, 'Not found');
  } catch (error) {
    if (!res.headersSent) json(res, error.status || 500, { error: error.message || 'Внутренняя ошибка сервера' });
    else res.destroy(error);
  }
});
server.listen(PORT, HOST, () => console.log(`Telegram Beats listening on http://${HOST}:${PORT}`));
