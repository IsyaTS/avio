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

  function resolveOrigin(locationLike) {
    if (!locationLike) {
      return 'https://localhost';
    }
    if (locationLike.origin) {
      return locationLike.origin;
    }
    if (locationLike.protocol && locationLike.host) {
      return `${locationLike.protocol}//${locationLike.host}`;
    }
    if (locationLike.href) {
      try {
        return new URL(locationLike.href, 'https://localhost').origin;
      } catch (error) {
        return 'https://localhost';
      }
    }
    return 'https://localhost';
  }

  function isAbsoluteUrl(path) {
    if (typeof path !== 'string') {
      return false;
    }
    return /^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(path);
  }

  function buildUrl(path, options = {}) {
    const { includeKey = true } = options || {};
    const raw = path == null ? '' : String(path);
    const locationLike = safeLocation();
    const baseOrigin = resolveOrigin(locationLike);
    let url;
    try {
      url = new URL(raw || '', baseOrigin);
    } catch (error) {
      url = new URL(baseOrigin);
      if (raw) {
        url.pathname = raw;
      }
    }

    if (locationLike && locationLike.hostname && url.hostname !== locationLike.hostname) {
      if (url.origin === baseOrigin || !isAbsoluteUrl(raw)) {
        url = new URL(url.pathname + url.search + url.hash, baseOrigin);
      }
    }

    if (locationLike && url.hostname === locationLike.hostname) {
      if (locationLike.protocol && url.protocol !== locationLike.protocol) {
        url.protocol = locationLike.protocol;
      }
      if (locationLike.port && url.port !== locationLike.port) {
        url.port = locationLike.port;
      }
    }

    const alreadyHasKey = url.searchParams.has('k');
    if (includeKey && !alreadyHasKey) {
      const key = resolveClientKey();
      const sameOrigin = !isAbsoluteUrl(raw) || url.origin === baseOrigin;
      if (key && sameOrigin) {
        url.searchParams.set('k', key);
      }
    }

    return url.toString();
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
