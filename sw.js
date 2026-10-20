const CACHE = 'telegram-beats-shell-v5';
const SHELL = ['/', '/manifest.json', '/icon-192.png', '/icon-512.png'];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE)
      .then(cache => Promise.all(SHELL.map(u => cache.add(u).catch(() => {}))))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

// Оболочка: сначала кэш (мгновенный запуск, работает офлайн и пока Render спит),
// в фоне обновляем — свежая версия применится при следующем запуске.
self.addEventListener('fetch', event => {
  const request = event.request;
  const url = new URL(request.url);
  if (request.method !== 'GET' || url.origin !== self.location.origin || url.pathname.startsWith('/api/')) return;
  const isShell = request.mode === 'navigate' || SHELL.includes(url.pathname) || url.pathname === '/index.html';
  if (!isShell) return;
  const key = request.mode === 'navigate' || url.pathname === '/index.html' ? '/' : request;
  event.respondWith((async () => {
    const cache = await caches.open(CACHE);
    const cached = await cache.match(key);
    const refresh = fetch(request).then(resp => {
      if (resp && resp.ok) cache.put(key, resp.clone());
      return resp;
    }).catch(() => null);
    if (cached) { event.waitUntil(refresh); return cached; }
    return (await refresh) || new Response('Нет сети', { status: 503, headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
  })());
});
