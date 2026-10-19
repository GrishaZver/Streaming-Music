const CACHE = 'telegram-beats-shell-v3';
self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE).then(cache => cache.add('/')).then(() => self.skipWaiting()));
});
self.addEventListener('activate', event => {
  event.waitUntil(caches.keys().then(keys => Promise.all(keys.filter(key => key !== CACHE).map(key => caches.delete(key)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', event => {
  const request = event.request;
  const url = new URL(request.url);
  if (request.method !== 'GET' || url.origin !== self.location.origin || url.pathname.startsWith('/api/')) return;
  event.respondWith(fetch(request).then(response => {
    if (response.ok && (url.pathname === '/' || url.pathname === '/index.html')) {
      const copy = response.clone(); caches.open(CACHE).then(cache => cache.put('/', copy));
    }
    return response;
  }).catch(async () => (await caches.match(request)) || (await caches.match('/'))));
});
