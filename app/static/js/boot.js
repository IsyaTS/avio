(function () {
  function safeLocation() {
    if (typeof window !== 'undefined' && window.location) {
      return window.location;
    }
    return {
      origin: '',
      protocol: '',
      host: '',
      hostname: '',
      port: '',
      href: '/',
      search: '',
    };
  }

  function resolveOrigin(locationLike) {
    if (locationLike && typeof locationLike.origin === 'string' && locationLike.origin) {
      return locationLike.origin;
    }
    const href = locationLike && typeof locationLike.href === 'string' ? locationLike.href : '/';
    try {
      return new URL(href, 'https://localhost').origin;
    } catch (error) {
      return 'https://localhost';
    }
  }

  function readQueryKey(locationLike) {
    if (!locationLike || typeof locationLike.search !== 'string') {
      return '';
    }
    try {
      const params = new URLSearchParams(locationLike.search);
      const value = params.get('k');
      return value ? value.trim() : '';
    } catch (error) {
      return '';
    }
  }

  function decodeCookieValue(value) {
    if (typeof value !== 'string') return '';
    const trimmed = value.trim();
    if (!trimmed) return '';
    try {
      return decodeURIComponent(trimmed);
    } catch (error) {
      return trimmed;
    }
  }

  function readCookieKey() {
    if (typeof document === 'undefined' || typeof document.cookie !== 'string') {
      return '';
    }
    const rawCookie = document.cookie;
    if (!rawCookie) {
      return '';
    }
    const parts = rawCookie.split(';');
    for (let idx = 0; idx < parts.length; idx += 1) {
      const part = parts[idx];
      if (!part) continue;
      const trimmed = part.trim();
      if (!trimmed) continue;
      if (trimmed.startsWith('client_key=')) {
        return decodeCookieValue(trimmed.slice('client_key='.length));
      }
      const eqIndex = trimmed.indexOf('=');
      if (eqIndex <= 0) continue;
      const name = trimmed.slice(0, eqIndex).trim();
      if (name === 'client_key') {
        return decodeCookieValue(trimmed.slice(eqIndex + 1));
      }
    }
    return '';
  }

  function resolveClientKey() {
    const locationLike = safeLocation();
    const queryValue = readQueryKey(locationLike);
    if (queryValue) {
      return queryValue;
    }
    const cookieValue = readCookieKey();
    if (cookieValue) {
      return cookieValue;
    }
    return '';
  }

  function ensureAbsoluteUrl(raw) {
    const input = raw == null ? '' : String(raw).trim();
    if (!input) {
      return '';
    }
    if (/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(input)) {
      return input;
    }
    const locationLike = safeLocation();
    const origin = resolveOrigin(locationLike);
    if (input.startsWith('//')) {
      const protocol = locationLike && typeof locationLike.protocol === 'string' && locationLike.protocol
        ? locationLike.protocol
        : 'https:';
      return `${protocol}${input}`;
    }
    try {
      return new URL(input, origin).toString();
    } catch (error) {
      try {
        const normalizedOrigin = origin.endsWith('/') ? origin : `${origin}/`;
        const normalizedPath = input.replace(/^\/+/, '');
        return new URL(normalizedPath, normalizedOrigin).toString();
      } catch (_) {
        const safeOrigin = origin.replace(/\/$/, '');
        const safePath = input.replace(/^\/+/, '');
        return `${safeOrigin}/${safePath}`;
      }
    }
  }

  function shouldAttachKey(url, includeKey) {
    if (!includeKey) {
      return false;
    }
    if (!url || typeof url.pathname !== 'string') {
      return false;
    }
    if (!url.pathname.startsWith('/pub/')) {
      return false;
    }
    return !url.searchParams.has('k');
  }

  function buildUrl(pathOrUrl, options = {}) {
    const includeKey = options && Object.prototype.hasOwnProperty.call(options, 'includeKey')
      ? options.includeKey !== false
      : true;

    const absolute = ensureAbsoluteUrl(pathOrUrl);
    if (!absolute) {
      return '';
    }

    if (!includeKey) {
      return absolute;
    }

    const key = resolveClientKey();
    if (!key) {
      return absolute;
    }

    try {
      const url = new URL(absolute);
      if (shouldAttachKey(url, includeKey)) {
        url.searchParams.set('k', key);
      }
      return url.toString();
    } catch (error) {
      return absolute;
    }
  }

  buildUrl.getKey = resolveClientKey;

  if (typeof window !== 'undefined') {
    window.buildUrl = buildUrl;
  }

  function extractVersion(src) {
    if (typeof src !== 'string') return 'unknown';
    try {
      const url = new URL(src, window.location.origin);
      const versionParam = url.searchParams.get('v');
      return versionParam || 'unknown';
    } catch (error) {
      try {
        const match = src.match(/[?&]v=([^&]+)/);
        if (match && match[1]) return match[1];
      } catch (_) {
        /* noop */
      }
      return 'unknown';
    }
  }

  const currentScript = document.currentScript;
  const scriptVersion = extractVersion(currentScript && currentScript.getAttribute('src'));
  const bootStartedAt = Date.now();
  window.__EXPORT_BOOT_TS__ = bootStartedAt;

  document.addEventListener('DOMContentLoaded', () => {
    let button = document.getElementById('export-download');
    if (button && button.dataset && button.dataset.bound) {
      const clone = button.cloneNode(true);
      delete clone.dataset.bound;
      button.replaceWith(clone);
      button = clone;
    }

    if (!button) {
      window.__EXPORT_BIND_OK__ = false;
      window.__EXPORT_LOADED__ = false;
      console.info('boot ok');
      return;
    }

    if (button.dataset) {
      delete button.dataset.bound;
    }

    window.__EXPORT_BIND_OK__ = false;
    window.__EXPORT_LOADED__ = false;

    console.info('boot ok');
  });
})();
