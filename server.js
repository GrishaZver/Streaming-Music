'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');
const { YtDlp } = require('ytdlp-nodejs');

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const ROOT = __dirname;
const APP_NAME = 'telegram_beats_player';
const AUDIUS_NODES = [
  'https://discoveryprovider.audius.co/v1',
  'https://api.audius.co/v1'
];

const ytdlp = new YtDlp();
let ytMusicApiPromise = null;
const audioUrlCache = new Map();

class HttpError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

function json(res, status, data) {
  const body = Buffer.from(JSON.stringify(data));
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': body.length,
    'Cache-Control': 'no-store'
  });
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
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      redirect: 'follow',
      headers: {
        'User-Agent': 'TelegramBeats/1.4 (+self-hosted music player)',
        ...(options.headers || {})
      }
    });
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

/* ---------- Audius ---------- */
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
        url.searchParams.set('app_name', APP_NAME);
        url.searchParams.set('time', 'week');
        url.searchParams.set('limit', String(Math.min(limit, 25)));
        data = await fetchJson(url.toString(), 6500);
        items = Array.isArray(data.data) ? data.data : [];
      }

      const tracks = items
        .filter(x => x && x.id && x.is_streamable !== false)
        .map(x => {
          const art = x.artwork || {};
          return asTrack('audius', {
            id: x.id,
            title: x.title,
            ch: x.user?.name || x.artist || 'Audius',
            duration: x.duration,
            thumb: art['480x480'] || art['150x150'] || art['1000x1000'] || ''
          });
        });

      if (tracks.length) return tracks;
      lastError = new Error('Audius не вернул треки');
    } catch (error) { lastError = error; }
  }
  throw lastError || new Error('Audius недоступен');
}

/* ---------- YouTube Music (только поиск) ---------- */
function normalizeYouTubeItem(item) {
  const artist = Array.isArray(item.artist)
    ? item.artist.map(a => typeof a === 'string' ? a : a?.name).filter(Boolean).join(', ')
    : (typeof item.artist === 'object' ? item.artist?.name : item.artist) ||
      item.artistNames || item.author || item.channel || item.uploader || 'YouTube Music';

  const thumbs = item.thumbnails || item.thumbnail || item.thumb || item.artwork || [];
  const thumb = Array.isArray(thumbs)
    ? (thumbs.at(-1)?.url || thumbs[0]?.url || '')
    : (typeof thumbs === 'string' ? thumbs : thumbs?.url || thumbs?.['480x480'] || '');

  const id = item.videoId || item.video_id || item.id || item.browseId;
  if (!id) return null;

  const rawDuration = Number(item.duration || item.durationSeconds || 0);
  const duration = rawDuration > 10000 ? Math.round(rawDuration / 1000) : rawDuration;

  return {
    id: `ytmusic:${id}`,
    source: 'ytmusic',
    sourceId: String(id),
    title: String(item.name || item.title || item.titleText || 'Без названия'),
    ch: String(artist),
    thumb,
    duration,
    url: item.url || ''
  };
}

async function searchYouTubeMusicPackage(query, limit) {
  if (!ytMusicApiPromise) {
    ytMusicApiPromise = (async () => {
      const YoutubeMusicApi = require('youtube-music-api');
      const api = new YoutubeMusicApi();
      await api.initalize();
      return api;
    })();
  }
  const api = await ytMusicApiPromise;
  const result = await api.search(query || 'popular music', 'song');
  const items = Array.isArray(result?.content) ? result.content : [];
  return items.slice(0, limit).map(normalizeYouTubeItem).filter(Boolean);
}

/* ---------- Склейка результатов ---------- */
function interleaveBySource(tracks) {
  const buckets = new Map();
  for (const t of tracks) {
    if (!buckets.has(t.source)) buckets.set(t.source, []);
    buckets.get(t.source).push(t);
  }
  const sources = [...buckets.keys()];
  const result = [];
  let exhausted = false;
  while (!exhausted) {
    exhausted = true;
    for (const src of sources) {
      const bucket = buckets.get(src);
      if (bucket.length) { result.push(bucket.shift()); exhausted = false; }
    }
  }
  return result;
}

async function searchAll(query, limit, wave, offset = 0) {
  const tasks = [
    searchYouTubeMusicPackage(query, Math.min(limit, 30)),
    searchAudius(query, Math.min(limit, 25), 0)
  ];

  const results = await Promise.allSettled(tasks);
  const tracks = results.filter(r => r.status === 'fulfilled').flatMap(r => r.value);
  const errors = results.filter(r => r.status === 'rejected').map(r => r.reason?.message || 'Источник недоступен');

  const seen = new Set();
  const unique = tracks.filter(track => {
    if (!track || !track.id || seen.has(track.id)) return false;
    seen.add(track.id);
    return true;
  });

  return { tracks: interleaveBySource(unique).slice(offset, offset + limit), errors };
}

/* ---------- Резолв аудио через yt-dlp ---------- */
async function getAudioUrl(videoId) {
  const cached = audioUrlCache.get(videoId);
  if (cached && cached.expires > Date.now()) return cached;

  const videoUrl = `https://www.youtube.com/watch?v=${videoId}`;
  const info = await ytdlp.getInfoAsync(videoUrl, {
    format: 'bestaudio[ext=m4a]/bestaudio[ext=mp4]/bestaudio',
    cookiesFromBrowser: process.env.YT_COOKIES_FROM_BROWSER || undefined,
  });

  let url = info && info.url;
  if (!url && Array.isArray(info?.formats)) {
    const pick = info.formats
      .filter(f => f.acodec && f.acodec !== 'none' && (f.vcodec === 'none' || !f.vcodec))
      .sort((a, b) => (b.abr || b.tbr || 0) - (a.abr || a.tbr || 0))[0];
    if (pick && pick.url) url = pick.url;
  }
  if (!url && Array.isArray(info?.requested_downloads)) {
    const first = info.requested_downloads[0];
    if (first && first.url) url = first.url;
  }

  if (!url || typeof url !== 'string') {
    throw new Error('yt-dlp не вернул URL аудиопотока (возможно, требуется cookies)');
  }

  const entry = { url, expires: Date.now() + 5 * 60 * 1000 };
  audioUrlCache.set(videoId, entry);
  if (audioUrlCache.size > 50) {
    audioUrlCache.delete(audioUrlCache.keys().next().value);
  }
  console.log('[ytmusic] resolved', videoId, '→', url.slice(0, 80) + '…');
  return entry;
}

async function streamYouTubeMusic(req, res, videoId) {
  if (!videoId || !/^[a-zA-Z0-9_-]{5,80}$/.test(videoId)) {
    throw new HttpError(400, 'Некорректный YouTube ID');
  }

  let entry;
  try {
    entry = await getAudioUrl(videoId);
  } catch (error) {
    throw new HttpError(502, `yt-dlp: ${error.message}`);
  }

  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    Accept: req.headers.accept || 'audio/*,*/*;q=0.8'
  };
  if (req.headers.range) headers.Range = req.headers.range;
  if (process.env.YT_COOKIES) headers.Cookie = process.env.YT_COOKIES;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  let upstream;
  try {
    upstream = await fetch(entry.url, {
      method: req.method === 'HEAD' ? 'HEAD' : 'GET',
      headers,
      redirect: 'follow',
      signal: controller.signal
    });
  } catch (error) {
    clearTimeout(timeout);
    throw new HttpError(502, error.name === 'AbortError'
      ? 'googlevideo не ответил за 15 секунд (возможно, IP Render заблокирован)'
      : `Сетевая ошибка googlevideo: ${error.message}`);
  }
  clearTimeout(timeout);

  if (!upstream.ok && upstream.status !== 206) {
    await upstream.body?.cancel().catch(() => {});
    throw new HttpError(502, `Аудиосервер YouTube ответил HTTP ${upstream.status}`);
  }

  const outHeaders = {
    'Content-Type': upstream.headers.get('content-type') || 'audio/mp4',
    'Accept-Ranges': upstream.headers.get('accept-ranges') || 'bytes',
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*'
  };
  for (const key of ['content-length', 'content-range', 'last-modified', 'etag']) {
    const value = upstream.headers.get(key);
    if (value) outHeaders[key] = value;
  }

  console.log('[ytmusic]', videoId, '→', outHeaders['Content-Type']);

  res.writeHead(upstream.status, outHeaders);
  if (req.method === 'HEAD' || !upstream.body) return res.end();
  await pipeline(Readable.fromWeb(upstream.body), res);
}

/* ---------- Прокси Audius / archive ---------- */
async function proxyAudio(req, res, url) {
  const provider = url.searchParams.get('provider');
  const id = url.searchParams.get('id') || '';
  if (!id || id.length > 300 || /[\r\n]/.test(id)) {
    throw new HttpError(400, 'Некорректный ID трека');
  }

  let upstreamUrl;
  if (provider === 'audius') {
    const base = AUDIUS_NODES[0];
    upstreamUrl = new URL(base + '/tracks/' + encodeURIComponent(id) + '/stream');
    upstreamUrl.searchParams.set('app_name', APP_NAME);
  } else if (provider === 'archive') {
    const file = url.searchParams.get('file') || '';
    if (!file || file.length > 255 || file.includes('/') || file.includes('\\') || file === '.' || file === '..' || file.includes('..')) {
      throw new HttpError(400, 'Некорректное имя файла');
    }
    upstreamUrl = new URL('https://archive.org/download/' + encodeURIComponent(id) + '/' + encodeURIComponent(file));
  } else {
    throw new HttpError(400, 'Неизвестный источник');
  }

  const headers = { Accept: req.headers.accept || 'audio/*,*/*;q=0.8' };
  if (req.headers.range) headers.Range = req.headers.range;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15000);
  let upstream;
  try {
    upstream = await fetch(upstreamUrl, {
      method: req.method === 'HEAD' ? 'HEAD' : 'GET',
      signal: controller.signal,
      redirect: 'follow',
      headers: { ...headers, 'User-Agent': 'TelegramBeats/1.4' }
    });
  } catch (error) {
    if (!res.headersSent) {
      json(res, 502, {
        error: error.name === 'AbortError'
          ? 'Источник аудио не ответил вовремя'
          : 'Не удалось подключиться к источнику аудио'
      });
    }
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
    const value = upstream.headers.get(key);
    if (value) responseHeaders[key] = value;
  }

  res.writeHead(upstream.status, responseHeaders);
  if (req.method === 'HEAD' || !upstream.body) { res.end(); return; }
  try { await pipeline(Readable.fromWeb(upstream.body), res); }
  catch (error) { if (!res.destroyed) res.destroy(error); }
}

/* ---------- Health ---------- */
async function health() {
  const providers = {};

  // Audius
  try {
    await fetchJson('https://discoveryprovider.audius.co/v1/tracks/trending?app_name=' + APP_NAME + '&time=week&limit=1', 4500);
    providers.audius = true;
  } catch { providers.audius = false; }

  // YouTube через yt-dlp — резолвим URL и проверяем, что googlevideo отвечает
  try {
    const entry = await getAudioUrl('dQw4w9WgXcQ');
    const r = await fetch(entry.url, {
      method: 'HEAD',
      headers: { 'User-Agent': 'Mozilla/5.0 Chrome/131.0.0.0' }
    });
    providers.youtube = r.ok || r.status === 206;
    if (!providers.youtube) {
      providers.youtubeError = 'googlevideo HTTP ' + r.status;
    } else {
      providers.youtubeFormat = r.headers.get('content-type') || 'unknown';
    }
  } catch (e) {
    providers.youtube = false;
    providers.youtubeError = String(e.message || e).slice(0, 200);
  }

  return {
    ok: providers.audius || providers.youtube,
    providers,
    message: (providers.audius || providers.youtube)
      ? 'Хотя бы один источник доступен'
      : 'Источники не отвечают'
  };
}

/* ---------- HTTP ---------- */
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,HEAD,OPTIONS',
        'Access-Control-Allow-Headers': 'Range,Content-Type'
      });
      res.end();
      return;
    }

    if ((req.method === 'GET' || req.method === 'HEAD') && url.pathname === '/api/health') {
      json(res, 200, await health());
      return;
    }

    if (req.method === 'GET' && url.pathname === '/api/search') {
      const query = (url.searchParams.get('query') || '').trim().slice(0, 120);
      const limit = Math.max(1, Math.min(Number(url.searchParams.get('limit') || 20), 30));
      const offset = Math.max(0, Math.min(Number(url.searchParams.get('offset') || 0), 10000));
      const result = await searchAll(query, limit, url.searchParams.get('wave') === '1', offset);
      json(res, 200, result);
      return;
    }

    if ((req.method === 'GET' || req.method === 'HEAD') && url.pathname === '/api/stream') {
      const provider = url.searchParams.get('provider');
      const id = url.searchParams.get('id') || '';
      if (provider === 'ytmusic') {
        await streamYouTubeMusic(req, res, id);
      } else {
        await proxyAudio(req, res, url);
      }
      return;
    }

    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
      const body = fs.readFileSync(path.join(ROOT, 'index.html'));
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Content-Length': body.length, 'Cache-Control': 'no-cache' });
      res.end(body);
      return;
    }

    if (req.method === 'GET' && /^\/(manifest\.json|icon-192\.png|sw\.js)$/.test(url.pathname)) {
      const file = path.join(ROOT, url.pathname.slice(1));
      if (!fs.existsSync(file)) { sendText(res, 404, 'Not found'); return; }
      const type = file.endsWith('.js') ? 'text/javascript; charset=utf-8'
        : file.endsWith('.json') ? 'application/json; charset=utf-8'
        : 'image/png';
      res.writeHead(200, { 'Content-Type': type });
      fs.createReadStream(file).pipe(res);
      return;
    }

    sendText(res, 404, 'Not found');
  } catch (error) {
    if (!res.headersSent) json(res, error.status || 500, { error: error.message || 'Внутренняя ошибка сервера' });
    else res.destroy(error);
  }
});

server.listen(PORT, HOST, () => console.log(`Telegram Beats listening on http://${HOST}:${PORT}`));
