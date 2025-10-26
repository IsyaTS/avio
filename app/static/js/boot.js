(function () {
  'use strict';

  const STATE_NODE_ID = 'client-settings-state';
  const TENANT_PATH_REGEX = /\/client\/(\d+)/;
  const DEFAULT_CLIENT_ENDPOINTS = {
    settingsGet: '/pub/settings/get',
    uploadCatalog: '/pub/catalog/upload',
    csvGet: '/pub/catalog/csv',
    csvSave: '/pub/catalog/csv',
    trainingUpload: '/pub/training/upload',
    trainingStatus: '/pub/training/status',
  };
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

  function parseClientStateNode() {
    if (typeof document === 'undefined' || typeof document.getElementById !== 'function') {
      return null;
    }
    try {
      const node = document.getElementById(STATE_NODE_ID);
      if (!node) {
        return null;
      }
      const raw = (node.textContent || '').trim();
      if (!raw) {
        return null;
      }
      return JSON.parse(raw);
    } catch (error) {
      try {
        console.error('[boot] failed to parse client settings state', error);
      } catch (_) {}
      return null;
    }
  }

  function deriveTenantFromPath() {
    const locationLike = safeLocation();
    const pathname = (locationLike && typeof locationLike.pathname === 'string') ? locationLike.pathname : '';
    if (!pathname) {
      return null;
    }
    const match = pathname.match(TENANT_PATH_REGEX);
    if (match && match[1]) {
      const parsed = Number.parseInt(match[1], 10);
      if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
      }
    }
    return null;
  }

  function ensureClientState() {
    if (typeof window === 'undefined') {
      return { tenant: null, key: '' };
    }
    const existing = (window.__client_settings_state && typeof window.__client_settings_state === 'object')
      ? window.__client_settings_state
      : null;
    const fromDom = parseClientStateNode();
    const fallbackTenant = deriveTenantFromPath();
    const fallbackKey = resolveClientKey();

    const base = Object.assign({}, fromDom || {}, existing || {});
    let tenantValue = Number.parseInt(base.tenant, 10);
    if (!Number.isFinite(tenantValue) || tenantValue <= 0) {
      tenantValue = fallbackTenant;
    }
    base.tenant = Number.isFinite(tenantValue) && tenantValue > 0 ? tenantValue : null;

    const keyValue = typeof base.key === 'string' ? base.key.trim() : '';
    base.key = keyValue || fallbackKey || '';

    if (!base.urls || typeof base.urls !== 'object') {
      base.urls = (fromDom && typeof fromDom === 'object' && typeof fromDom.urls === 'object') ? fromDom.urls : {};
    }

    window.__client_settings_state = base;
    window.state = base;

    const existingClient = (window.CLIENT_SETTINGS && typeof window.CLIENT_SETTINGS === 'object')
      ? window.CLIENT_SETTINGS
      : {};
    const clientPayload = Object.assign({}, existingClient, {
      tenant: base.tenant,
      tenant_id: base.tenant,
      key: base.key,
      access_key: base.key,
    });
    if (typeof base.public_key === 'string' && base.public_key.trim()) {
      clientPayload.public_key = base.public_key.trim();
    }
    if (typeof base.webhook_secret === 'string' && base.webhook_secret.trim()) {
      clientPayload.webhook_secret = base.webhook_secret.trim();
    }
    window.CLIENT_SETTINGS = clientPayload;

    return base;
  }

  function ensureClientEndpoints(state) {
    const urls = state && typeof state.urls === 'object' ? state.urls : {};
    const computed = {
      settingsGet: typeof urls.settings_get === 'string' && urls.settings_get ? urls.settings_get : DEFAULT_CLIENT_ENDPOINTS.settingsGet,
      uploadCatalog: typeof urls.upload_catalog === 'string' && urls.upload_catalog ? urls.upload_catalog : DEFAULT_CLIENT_ENDPOINTS.uploadCatalog,
      csvGet: typeof urls.csv_get === 'string' && urls.csv_get ? urls.csv_get : DEFAULT_CLIENT_ENDPOINTS.csvGet,
      csvSave: typeof urls.csv_save === 'string' && urls.csv_save ? urls.csv_save : DEFAULT_CLIENT_ENDPOINTS.csvSave,
      trainingUpload: typeof urls.training_upload === 'string' && urls.training_upload ? urls.training_upload : DEFAULT_CLIENT_ENDPOINTS.trainingUpload,
      trainingStatus: typeof urls.training_status === 'string' && urls.training_status ? urls.training_status : DEFAULT_CLIENT_ENDPOINTS.trainingStatus,
    };

    if (typeof window === 'undefined') {
      return Object.assign({}, DEFAULT_CLIENT_ENDPOINTS, computed);
    }

    const existing = (window.__client_endpoints && typeof window.__client_endpoints === 'object')
      ? window.__client_endpoints
      : {};
    const merged = Object.assign({}, DEFAULT_CLIENT_ENDPOINTS, existing, computed);
    window.__client_endpoints = merged;
    return merged;
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

  const initialClientState = ensureClientState();
  ensureClientEndpoints(initialClientState);

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

  function setExportLoadedFlag(value) {
    if (value === false && window.__EXPORT_LOADED__ === true) {
      return;
    }
    window.__EXPORT_LOADED__ = value;
  }

  let clientSettingsBootCalled = false;

  function callClientSettingsBoot() {
    if (clientSettingsBootCalled) {
      return;
    }
    clientSettingsBootCalled = true;
    try {
      if (typeof window !== 'undefined' && typeof window.__client_settings_boot === 'function') {
        window.__client_settings_boot();
      }
    } catch (error) {
      try {
        console.error('[boot] client settings bootstrap error', error);
      } catch (_) {}
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    try {
      let button = document.getElementById('export-download');
      if (button && button.dataset && button.dataset.bound) {
        const clone = button.cloneNode(true);
        delete clone.dataset.bound;
        button.replaceWith(clone);
        button = clone;
      }

      if (!button) {
        window.__EXPORT_BIND_OK__ = false;
        setExportLoadedFlag(false);
        console.info('boot ok');
        return;
      }

      if (button.dataset) {
        delete button.dataset.bound;
      }

      window.__EXPORT_BIND_OK__ = false;
      setExportLoadedFlag(false);

      console.info('boot ok');
    } finally {
      callClientSettingsBoot();
    }
  });

  if (typeof document !== 'undefined' && document.readyState !== 'loading') {
    setTimeout(callClientSettingsBoot, 0);
  }
})();
