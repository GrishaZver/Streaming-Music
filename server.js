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
const VEROME_API = process.env.VEROME_API_URL || 'https://verome-api.deno.dev';
let ytMusicApiPromise = null;

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
async function searchAudius(query, limit, offset = 0) {
  let lastError;
  for (const base of AUDIUS_NODES) {
    try {
      let url = new URL(base + (query ? '/tracks/search' : '/tracks/trending'));
      url.searchParams.set('app_name', APP_NAME);
      url.searchParams.set('limit', String(Math.max(1, Math.min(limit, 25))));
      if (offset > 0) url.searchParams.set('offset', String(offset));
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
function pickArray(value) {
  if (Array.isArray(value)) return value;
  if (!value || typeof value !== 'object') return [];
  for (const key of ['songs', 'tracks', 'results', 'items', 'content', 'data']) {
    const candidate = value[key];
    if (Array.isArray(candidate)) return candidate;
    if (candidate && typeof candidate === 'object') {
      const nested = pickArray(candidate);
      if (nested.length) return nested;
    }
  }
  return [];
}
function normalizeVeromeItem(item) {
  const artist = Array.isArray(item.artist) ? item.artist.map(a => typeof a === 'string' ? a : a?.name).filter(Boolean).join(', ') :
    (typeof item.artist === 'object' ? item.artist?.name : item.artist) || item.artistNames || item.author || item.channel || item.uploader || 'YouTube Music';
  const thumbs = item.thumbnails || item.thumbnail || item.thumb || item.artwork || [];
  const thumb = Array.isArray(thumbs) ? (thumbs.at(-1)?.url || thumbs[0]?.url || '') : (typeof thumbs === 'string' ? thumbs : thumbs?.url || thumbs?.['480x480'] || '');
  const id = item.videoId || item.video_id || item.id || item.browseId;
  if (!id) return null;
  return { id: `verome:${id}`, source: 'verome', sourceId: String(id), title: String(item.name || item.title || item.titleText || 'Без названия'), ch: String(artist), thumb, duration: Number(item.duration || item.durationSeconds || 0) > 10000 ? Math.round(Number(item.duration || 0) / 1000) : Number(item.duration || item.durationSeconds || 0), url: item.url || '' };
}
async function searchYoutubeMusicPackage(query, limit) {
  if (!ytMusicApiPromise) ytMusicApiPromise = (async () => {
    const YoutubeMusicApi = require('youtube-music-api');
    const api = new YoutubeMusicApi();
    await api.initalize();
    return api;
  })();
  const api = await ytMusicApiPromise;
  const result = await api.search(query || 'popular music', 'song');
  const items = Array.isArray(result?.content) ? result.content : [];
  return items.slice(0, limit).map(normalizeVeromeItem).filter(Boolean).map(track => ({...track, source:'verome', id:'verome:'+track.sourceId}));
}

async function searchVerome(query, limit, offset = 0) {
  const u = new URL(VEROME_API + '/api/search');
  u.searchParams.set('q', query || 'trending music');
  u.searchParams.set('filter', 'song');
  // Verome currently documents search but not stable offset/continuation paging.
  const data = await fetchJson(u.toString(), 11000);
  const items = pickArray(data).map(normalizeVeromeItem).filter(Boolean);
  const seen = new Set();
  const unique = items.filter(item => { if (seen.has(item.id)) return false; seen.add(item.id); return true; });
  return unique.slice(offset, offset + limit);
}
async function streamVerome(req, res, id) {
  if (!id || !/^[a-zA-Z0-9_-]{5,80}$/.test(id)) throw new HttpError(400, 'Некорректный ID YouTube Music');

  // Verome versions may return either an audio response/redirect or JSON with a stream URL.
  const endpoint = `${VEROME_API}/api/stream?id=${encodeURIComponent(id)}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 18000);
  let response;
  try {
    response = await fetch(endpoint, {
      method: req.method === 'HEAD' ? 'HEAD' : 'GET',
      redirect: 'follow', signal: controller.signal,
      headers: { 'User-Agent': 'Mozilla/5.0 TelegramBeats/1.0', Accept: 'audio/*,application/json;q=0.9,*/*;q=0.8', ...(req.headers.range ? { Range: req.headers.range } : {}) }
    });
  } catch (error) {
    if (error.name === 'AbortError') throw new HttpError(504, 'Verome долго получает аудио. Попробуй ещё раз.');
    throw new HttpError(502, `Не удалось получить аудио от Verome: ${error.message}`);
  } finally { clearTimeout(timer); }

  if (!response.ok && response.status !== 206) {
    const detail = (await response.text().catch(() => '')).slice(0, 180);
    throw new HttpError(502, `Verome не смог получить трек (HTTP ${response.status})${detail ? ': ' + detail : ''}`);
  }

  const contentType = (response.headers.get('content-type') || '').toLowerCase();
  // If the endpoint already streams audio, relay it without trying to parse it as JSON.
  if (contentType.startsWith('audio/') || contentType.includes('application/octet-stream') || contentType.includes('video/webm') || contentType.includes('video/mp4')) {
    const outHeaders = { 'Content-Type': response.headers.get('content-type') || 'audio/mp4', 'Accept-Ranges': response.headers.get('accept-ranges') || 'bytes', 'Cache-Control': 'no-store', 'Access-Control-Allow-Origin': '*' };
    for (const key of ['content-length','content-range','last-modified','etag']) { const value = response.headers.get(key); if (value) outHeaders[key] = value; }
    res.writeHead(response.status, outHeaders);
    if (req.method === 'HEAD' || !response.body) return res.end();
    await pipeline(Readable.fromWeb(response.body), res);
    return;
  }

  let data;
  try { data = await response.json(); }
  catch { throw new HttpError(502, `Verome вернул неподдерживаемый ответ (${contentType || 'неизвестный тип данных'}) вместо аудио`); }

  function findUrl(value) {
    if (typeof value === 'string') return /^https:\/\//i.test(value) ? value : '';
    if (!value || typeof value !== 'object') return '';
    for (const key of ['audioUrl','audio_url','streamUrl','stream_url','proxyUrl','proxy_url','url','src','link']) {
      if (typeof value[key] === 'string' && /^https:\/\//i.test(value[key])) return value[key];
    }
    for (const key of ['audio','stream','streams','streamingUrls','streaming_urls','formats','data','result','results','content','url']) {
      const found = findUrl(value[key]); if (found) return found;
    }
    if (Array.isArray(value)) for (const item of value) { const found = findUrl(item); if (found) return found; }
    return '';
  }

  const audioUrl = findUrl(data);
  if (!audioUrl) throw new HttpError(502, 'Verome не вернул аудиопоток для этого трека. Попробуй другую песню.');
  const parsed = new URL(audioUrl);
  const host = parsed.hostname.toLowerCase();
  const allowed = /(^|\.)googlevideo\.com$|(^|\.)youtube\.com$|(^|\.)youtube-nocookie\.com$|(^|\.)piped\.video$|(^|\.)invidious\.io$|(^|\.)deno\.dev$|(^|\.)ytimg\.com$|(^|\.)kavin\.rocks$|(^|\.)adminforge\.de$|(^|\.)projectsegfau\.lt$|(^|\.)private\.coffee$|(^|\.)darkness\.services$|(^|\.)r4fo\.com$|(^|\.)piped\.yt$|(^|\.)leptons\.xyz$|(^|\.)omada\.cafe$|(^|\.)nadeko\.net$|(^|\.)y\.com\.sb$/i.test(host);
  if (parsed.protocol !== 'https:' || !allowed) throw new HttpError(502, `Verome вернул неподдерживаемый аудиохост: ${host}`);
  const headers = { 'User-Agent': 'Mozilla/5.0 TelegramBeats/1.0', Accept: req.headers.accept || 'audio/*,*/*;q=0.8' };
  if (req.headers.range) headers.Range = req.headers.range;
  const upstream = await fetch(parsed, { method: req.method === 'HEAD' ? 'HEAD' : 'GET', headers, redirect: 'follow' });
  if (!upstream.ok && upstream.status !== 206) { await upstream.body?.cancel().catch(() => {}); throw new HttpError(502, `Аудиосервер ответил HTTP ${upstream.status}`); }
  const outHeaders = { 'Content-Type': upstream.headers.get('content-type') || 'audio/mp4', 'Accept-Ranges': upstream.headers.get('accept-ranges') || 'bytes', 'Cache-Control': 'no-store', 'Access-Control-Allow-Origin': '*' };
  for (const key of ['content-length','content-range','last-modified','etag']) { const value = upstream.headers.get(key); if (value) outHeaders[key] = value; }
  res.writeHead(upstream.status, outHeaders);
  if (req.method === 'HEAD' || !upstream.body) return res.end();
  await pipeline(Readable.fromWeb(upstream.body), res);
}

async function searchAll(query, limit, wave, offset = 0) {
  const tasks = [searchVerome(query, Math.min(limit, 30), 0), searchYoutubeMusicPackage(query, Math.min(limit, 30))];
  // Audius remains a fallback for tracks that can play directly in the app.
  if (offset === 0) tasks.push(searchAudius(query, Math.min(limit, 25), 0));
  const results = await Promise.allSettled(tasks);
  const tracks = results.filter(result => result.status === 'fulfilled').flatMap(result => result.value);
  const errors = results.filter(result => result.status === 'rejected').map(result => result.reason?.message || 'Источник недоступен');
  const seen = new Set();
  const unique = tracks.filter(track => { if (seen.has(track.id)) return false; seen.add(track.id); return true; });
  unique.sort((a,b) => (a.source === 'verome' ? 0 : 1) - (b.source === 'verome' ? 0 : 1));
  return { tracks: unique.slice(offset, offset + limit), errors };
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
    fetchJson(VEROME_API + '/api/search?q=music&filter=song', 5000),
    fetchJson('https://discoveryprovider.audius.co/v1/tracks/trending?app_name=' + APP_NAME + '&time=week&limit=1', 4500)
  ]);
  const providers = { youtubeMusic: checks[0].status === 'fulfilled', audius: checks[1].status === 'fulfilled' };
  return { ok: Object.values(providers).some(Boolean), providers, message: Object.values(providers).some(Boolean) ? 'Хотя бы один музыкальный источник доступен' : 'Сервер работает, но музыкальные источники не отвечают' };
}
const server = http.createServer(async (req, res) => {
  try {
    const url = parseUrl(req);
    if (req.method === 'OPTIONS') { res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,HEAD,OPTIONS', 'Access-Control-Allow-Headers': 'Range,Content-Type' }); res.end(); return; }
    if ((req.method === 'GET' || req.method === 'HEAD') && url.pathname === '/api/health') { json(res, 200, await health()); return; }
    if (req.method === 'GET' && url.pathname === '/api/search') {
      const query = (url.searchParams.get('query') || '').trim().slice(0, 120);
      const limit = Math.max(1, Math.min(Number(url.searchParams.get('limit') || 20), 30));
      const offset = Math.max(0, Math.min(Number(url.searchParams.get('offset') || 0), 10000));
      const result = await searchAll(query, limit, url.searchParams.get('wave') === '1', offset);
      json(res, 200, result); return;
    }
    if ((req.method === 'GET' || req.method === 'HEAD') && url.pathname === '/api/stream') { if (url.searchParams.get('provider') === 'verome') await streamVerome(req, res, url.searchParams.get('id') || ''); else await proxyAudio(req, res, url); return; }
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
