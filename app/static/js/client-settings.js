window.__client_settings_build = '20240518';
window.__cs_loaded = window.__cs_loaded === true;
function getLocation() {
  if (typeof globalThis !== 'undefined' && globalThis.location) {
    return globalThis.location;
  }
  return {
    origin: '',
    protocol: '',
    host: '',
    hostname: '',
    port: '',
    pathname: '/',
    href: '/',
  };
}

(function initClientSettingsWrapper() {
  'use strict';

  console.info('client-settings loaded');
  window.__CATALOG_WIDGET_VERSION__ = '2025-03-20T15:30:00Z';
  window.__EXPORT_ERROR__ = undefined;

  const DEFAULT_CLIENT_ENDPOINTS = {
    settingsGet: '/pub/settings/get',
    uploadCatalog: '/pub/catalog/upload',
    csvGet: '/pub/catalog/csv',
    csvSave: '/pub/catalog/csv',
    trainingUpload: '/pub/training/upload',
    trainingStatus: '/pub/training/status',
  };

  if (typeof window !== 'undefined') {
    const hasEndpoints = window.__client_endpoints && typeof window.__client_endpoints === 'object';
    if (!hasEndpoints) {
      window.__client_endpoints = { ...DEFAULT_CLIENT_ENDPOINTS };
    }
  }

  const SETTINGS_FETCH_MAX_ATTEMPTS = 5;
  const SETTINGS_FETCH_BACKOFF_BASE_MS = 600;

  const STATE_NODE_ID = 'client-settings-state';
  const TENANT_PATH_REGEX = /\/client\/(\d+)(?:\/|$)/;

  const mergeClientState = (payload) => {
    const safePayload = payload && typeof payload === 'object' ? payload : {};
    const base = (typeof window !== 'undefined'
      && window.__client_settings_state
      && typeof window.__client_settings_state === 'object')
      ? window.__client_settings_state
      : {};
    const merged = { ...base, ...safePayload };
    if (typeof window !== 'undefined') {
      window.__client_settings_state = merged;
      window.state = merged;
    }
    return merged;
  };

  const readCookie = (name) => {
    if (typeof document === 'undefined' || !name) {
      return '';
    }
    const source = document.cookie || '';
    if (!source) {
      return '';
    }
    const parts = source.split(';');
    for (let idx = 0; idx < parts.length; idx += 1) {
      const part = parts[idx].trim();
      if (!part) {
        continue;
      }
      if (part.startsWith(`${name}=`)) {
        const value = part.slice(name.length + 1);
        try {
          return decodeURIComponent(value);
        } catch (error) {
          return value;
        }
      }
    }
    return '';
  };

  const deriveFallbackState = () => {
    if (typeof window === 'undefined') {
      return {};
    }
    const fallback = {};
    const locationInfo = getLocation();
    const path = locationInfo && typeof locationInfo.pathname === 'string' ? locationInfo.pathname : '';
    const match = path.match(TENANT_PATH_REGEX);
    if (match && match[1]) {
      const parsed = Number.parseInt(match[1], 10);
      if (Number.isFinite(parsed) && parsed > 0) {
        fallback.tenant = parsed;
      }
    }

    let keyCandidate = '';
    try {
      const origin = locationInfo.origin
        || (locationInfo.protocol && locationInfo.host ? `${locationInfo.protocol}//${locationInfo.host}` : 'https://localhost');
      const hrefBase = locationInfo.href || `${origin}${path || '/'}`;
      const url = new URL(hrefBase, origin || undefined);
      keyCandidate = url.searchParams.get('k') || '';
    } catch (error) {
      keyCandidate = '';
    }

    if (!keyCandidate) {
      const search = typeof locationInfo.search === 'string' ? locationInfo.search : '';
      if (search) {
        try {
          const params = new URLSearchParams(search.startsWith('?') ? search.slice(1) : search);
          keyCandidate = params.get('k') || '';
        } catch (error) {
          keyCandidate = '';
        }
      }
    }

    if (!keyCandidate) {
      keyCandidate = readCookie('client_key');
    }

    if (keyCandidate) {
      fallback.key = keyCandidate;
    }

    return fallback;
  };

  const resolveEndpointOverrides = (urls) => {
    const safeUrls = urls && typeof urls === 'object' ? urls : {};
    return {
      settingsGet: typeof safeUrls.settings_get === 'string' && safeUrls.settings_get
        ? safeUrls.settings_get
        : undefined,
      uploadCatalog: typeof safeUrls.upload_catalog === 'string' && safeUrls.upload_catalog
        ? safeUrls.upload_catalog
        : undefined,
      csvGet: typeof safeUrls.csv_get === 'string' && safeUrls.csv_get
        ? safeUrls.csv_get
        : undefined,
      csvSave: typeof safeUrls.csv_save === 'string' && safeUrls.csv_save
        ? safeUrls.csv_save
        : undefined,
      trainingUpload: typeof safeUrls.training_upload === 'string' && safeUrls.training_upload
        ? safeUrls.training_upload
        : undefined,
      trainingStatus: typeof safeUrls.training_status === 'string' && safeUrls.training_status
        ? safeUrls.training_status
        : undefined,
    };
  };

  const ensureBootstrapGlobals = () => {
    const fallbackState = deriveFallbackState();
    const existingState = (typeof window !== 'undefined'
      && window.__client_settings_state
      && typeof window.__client_settings_state === 'object')
      ? window.__client_settings_state
      : {};
    const payload = Object.assign({}, existingState);

    const fallbackTenant = Number.parseInt(fallbackState.tenant, 10);
    const existingTenant = Number.parseInt(payload.tenant, 10);
    if (!Number.isFinite(existingTenant) || existingTenant <= 0) {
      if (Number.isFinite(fallbackTenant) && fallbackTenant > 0) {
        payload.tenant = fallbackTenant;
      }
    }

    const existingKey = typeof payload.key === 'string' ? payload.key.trim() : '';
    if (existingKey) {
      payload.key = existingKey;
    } else if (fallbackState && typeof fallbackState.key === 'string' && fallbackState.key.trim()) {
      payload.key = fallbackState.key.trim();
    }

    if (!payload.urls || typeof payload.urls !== 'object') {
      payload.urls = (existingState && typeof existingState.urls === 'object') ? existingState.urls : {};
    }

    const state = mergeClientState(payload);
    if (!state.urls || typeof state.urls !== 'object') {
      state.urls = {};
      if (typeof window !== 'undefined') {
        window.__client_settings_state = state;
      }
    }

    const currentEndpoints = (typeof window !== 'undefined'
      && window.__client_endpoints
      && typeof window.__client_endpoints === 'object')
      ? window.__client_endpoints
      : {};
    const resolved = resolveEndpointOverrides(state.urls);
    const merged = Object.assign({}, DEFAULT_CLIENT_ENDPOINTS, currentEndpoints);
    Object.keys(resolved).forEach((key) => {
      if (resolved[key]) {
        merged[key] = resolved[key];
      }
    });
    if (typeof window !== 'undefined') {
      window.__client_endpoints = merged;
    }
    return { state, endpoints: merged };
  };

  const getClientSettings = () => {
    if (typeof window === 'undefined') {
      return {};
    }
    const payload = window.CLIENT_SETTINGS;
    return payload && typeof payload === 'object' ? payload : {};
  };

  const readStateFromDom = () => {
    if (typeof window !== 'undefined') {
      const cached = window.__client_settings_state;
      if (cached && typeof cached === 'object' && Object.keys(cached).length > 0) {
        return cached;
      }
    }

    if (typeof document === 'undefined') {
      return mergeClientState(deriveFallbackState());
    }

    const node = document.getElementById(STATE_NODE_ID);
    if (!node) {
      return mergeClientState(deriveFallbackState());
    }
    const raw = (node.textContent || '').trim();
    if (!raw) {
      return mergeClientState(deriveFallbackState());
    }
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        return mergeClientState(parsed);
      }
    } catch (error) {
      try {
        console.error('[client-settings] failed to parse state JSON', error);
      } catch (_) {}
    }
    return mergeClientState(deriveFallbackState());
  };

  const resolveMaxDays = (state) => {
    if (!state || typeof state !== 'object') {
      return null;
    }
    const value = Number(state.max_days);
    if (Number.isFinite(value) && value > 0) {
      return value;
    }
    return null;
  };

  const determineTenant = (state, options = {}) => {
    const { fallbackDefault = true, globalConfig = null } = options || {};
    let tenant = Number.parseInt(state && state.tenant, 10);
    if (!Number.isFinite(tenant) || tenant <= 0) {
      const config = globalConfig && typeof globalConfig === 'object' ? globalConfig : getClientSettings();
      const candidates = [];
      if (config && Object.prototype.hasOwnProperty.call(config, 'tenant')) {
        candidates.push(config.tenant);
      }
      if (config && Object.prototype.hasOwnProperty.call(config, 'tenant_id')) {
        candidates.push(config.tenant_id);
      }
      for (let idx = 0; idx < candidates.length; idx += 1) {
        const value = candidates[idx];
        if (value == null) {
          continue;
        }
        const parsed = Number.parseInt(value, 10);
        if (Number.isFinite(parsed) && parsed > 0) {
          tenant = parsed;
          break;
        }
      }
    }
    if (!Number.isFinite(tenant) || tenant <= 0) {
      const path = getLocation().pathname || '/';
      const match = path.match(TENANT_PATH_REGEX);
      if (match && match[1]) {
        const parsed = Number.parseInt(match[1], 10);
        tenant = Number.isFinite(parsed) ? parsed : tenant;
      }
    }
    if (Number.isFinite(tenant) && tenant > 0) {
      return tenant;
    }
    return fallbackDefault ? 1 : null;
  };

  const sleep = (ms) => new Promise((resolve) => {
    const safeMs = Math.max(0, Number(ms) || 0);
    setTimeout(resolve, safeMs);
  });

  async function fetchSettingsJsonWithRetry(url) {
    const targetUrl = typeof url === 'string' ? url.trim() : '';
    if (!targetUrl) {
      throw new Error('URL сервиса не задан');
    }
    let attempt = 0;
    let lastError = null;
    while (attempt < SETTINGS_FETCH_MAX_ATTEMPTS) {
      attempt += 1;
      try {
        const response = await fetch(targetUrl, { cache: 'no-store' });
        if (!response.ok) {
          const text = await response.text();
          const status = response.status;
          const message = (text && text.trim()) || `Ошибка загрузки настроек (HTTP ${status})`;
          throw new Error(message);
        }
        return await response.json();
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt >= SETTINGS_FETCH_MAX_ATTEMPTS) {
          break;
        }
        const delayMs = SETTINGS_FETCH_BACKOFF_BASE_MS * (2 ** Math.max(0, attempt - 1));
        await sleep(delayMs);
      }
    }
    throw lastError || new Error('Не удалось загрузить настройки');
  }

  function bindExportClicks() {
    const button = document.getElementById('export-download');
    if (!button) {
      window.__EXPORT_BIND_OK__ = false;
      return false;
    }

    if (button.dataset && button.dataset.bound === '1') {
      window.__EXPORT_BIND_OK__ = true;
      return true;
    }

    const statusNode = document.getElementById('export-status');
    const daysInput = document.getElementById('exp-days');
    const limitInput = document.getElementById('exp-limit');
    const perInput = document.getElementById('exp-per');

    const resolveEndpoint = (raw) => {
      const candidate = resolveEndpointUrl(raw, withTenant(), endpoints.whatsappExport);
      if (candidate) {
        return candidate;
      }
      const fallbackUrl = resolveEndpointUrl(endpoints.whatsappExport, withTenant());
      return fallbackUrl || '/pub/wa/export';
    };

    const updateStatus = (message, variant = 'muted') => {
      if (!statusNode) return;
      statusNode.className = `status-text ${variant}`.trim();
      statusNode.textContent = message || '';
    };

    const parseNumber = (value, { min = null, fallback = 0 } = {}) => {
      const numeric = Number.parseInt((value ?? '').toString().trim(), 10);
      if (!Number.isFinite(numeric)) {
        return fallback;
      }
      if (min !== null && numeric < min) {
        return min;
      }
      return numeric;
    };

    const parseFilename = (headerValue) => {
      if (!headerValue) {
        return '';
      }
      const match = headerValue.match(/filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i);
      if (!match) {
        return '';
      }
      const encoded = (match[1] || match[2] || '').trim();
      if (!encoded) {
        return '';
      }
      try {
        return decodeURIComponent(encoded);
      } catch (error) {
        return encoded;
      }
    };

    const parseCountHeader = (headers, name) => {
      const raw = headers.get(name);
      if (!raw) return null;
      const parsed = Number.parseInt(raw, 10);
      return Number.isFinite(parsed) ? parsed : null;
    };

    const buildDefaultFilename = () => {
      const now = new Date();
      const y = now.getUTCFullYear();
      const m = String(now.getUTCMonth() + 1).padStart(2, '0');
      const d = String(now.getUTCDate()).padStart(2, '0');
      return `whatsapp_export_${y}-${m}-${d}.zip`;
    };

    const requestArchive = async (endpointUrl, payload) => {
      const response = await fetch(endpointUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (response.status === 204) {
        return { status: 204 };
      }

      if (!response.ok) {
        let detail = '';
        let reason = '';
        try {
          const data = await response.clone().json();
          if (data && typeof data === 'object') {
            const { detail: detailValue, reason: reasonValue, message } = data;
            if (typeof reasonValue === 'string') {
              reason = reasonValue;
            }
            if (typeof detailValue === 'string') {
              detail = detailValue;
            } else if (Array.isArray(detailValue)) {
              detail = detailValue.map((item) => (item == null ? '' : String(item))).filter(Boolean).join(', ');
            } else if (detailValue && typeof detailValue === 'object') {
              const parts = [];
              Object.entries(detailValue).forEach(([keyName, value]) => {
                if (value == null) return;
                const text = Array.isArray(value) ? value.join(', ') : String(value);
                parts.push(`${keyName}: ${text}`);
              });
              detail = parts.join('; ');
            }
            if (!detail && typeof message === 'string') {
              detail = message;
            }
          }
        } catch (error) {
          try {
            detail = (await response.text()) || '';
          } catch (_) {
            detail = '';
          }
        }

        const message = (reason || detail || `Ошибка экспорта (HTTP ${response.status})`).trim() || 'Ошибка экспорта';
        const exportError = new Error(message);
        if (detail) exportError.detail = detail;
        if (reason) exportError.reason = reason;
        exportError.status = response.status;
        throw exportError;
      }

      const contentType = (response.headers.get('content-type') || '').toLowerCase();
      if (!contentType.startsWith('application/zip')) {
        let detail = '';
        try {
          if (contentType.includes('application/json')) {
            const data = await response.clone().json();
            if (data && typeof data === 'object') {
              const { detail: detailValue, message } = data;
              if (typeof detailValue === 'string') {
                detail = detailValue;
              } else if (Array.isArray(detailValue)) {
                detail = detailValue.map((item) => (item == null ? '' : String(item))).filter(Boolean).join(', ');
              } else if (detailValue && typeof detailValue === 'object') {
                const parts = [];
                Object.entries(detailValue).forEach(([keyName, value]) => {
                  if (value == null) return;
                  const text = Array.isArray(value) ? value.join(', ') : String(value);
                  parts.push(`${keyName}: ${text}`);
                });
                detail = parts.join('; ');
              }
              if (!detail && typeof message === 'string') {
                detail = message;
              }
            }
          } else {
            detail = (await response.text()) || '';
          }
        } catch (error) {
          try {
            detail = (await response.text()) || '';
          } catch (_) {
            detail = '';
          }
        }
        const exportError = new Error((detail || 'Ответ сервера не является ZIP-архивом').trim());
        exportError.status = response.status;
        if (detail) exportError.detail = detail;
        throw exportError;
      }

      const blob = await response.blob();
      const disposition = response.headers.get('content-disposition') || response.headers.get('Content-Disposition') || '';
      const filename = parseFilename(disposition) || buildDefaultFilename();

      return {
        status: 200,
        blob,
        filename,
        dialogCount: parseCountHeader(response.headers, 'X-Dialog-Count'),
        messageCount: parseCountHeader(response.headers, 'X-Message-Count'),
      };
    };

    button.type = 'button';

    button.addEventListener('click', async (event) => {
      event.preventDefault();
      event.stopPropagation();

      const state = readStateFromDom();
      const urls = state && typeof state === 'object' ? state.urls || {} : {};
      const maxDays = resolveMaxDays(state);
      const tenantValue = determineTenant(state, { fallbackDefault: false });
      if (!Number.isFinite(tenantValue) || tenantValue <= 0) {
        updateStatus('не удалось определить tenant', 'alert');
        return;
      }

      const tenant = tenantValue;
      const key = typeof state.key === 'string' ? state.key : '';
      const endpoint = resolveEndpoint(urls.whatsapp_export);
      if (!endpoint) {
        updateStatus('Ссылка для экспорта недоступна', 'alert');
        return;
      }

      let days = parseNumber(daysInput ? daysInput.value : '', { min: 0, fallback: 0 });
      if (maxDays !== null && days > maxDays) {
        days = maxDays;
      }
      if (daysInput) {
        daysInput.value = String(days);
      }

      const limit = parseNumber(limitInput ? limitInput.value : '', { min: 1, fallback: 200 });
      if (limitInput) {
        limitInput.value = String(limit);
      }

      if (perInput) {
        perInput.value = '0';
      }

      const payload = { tenant, key, days, limit, per: 0 };

      console.info('[client-settings] export tenant=%s', tenant);

      button.disabled = true;
      updateStatus('Готовим архив…', 'muted');

      try {
        const result = await requestArchive(endpoint, payload);
        if (result.status === 204) {
          updateStatus('Нет диалогов за период', 'alert');
          return;
        }

        const blobUrl = URL.createObjectURL(result.blob);
        const anchor = document.createElement('a');
        anchor.href = blobUrl;
        anchor.download = result.filename || 'whatsapp_export.zip';
        document.body.appendChild(anchor);
        anchor.click();
        setTimeout(() => {
          try {
            URL.revokeObjectURL(blobUrl);
          } catch (error) {
            console.warn('Failed to revoke export blob URL', error);
          }
          anchor.remove();
        }, 120);

        if (result.dialogCount != null && result.messageCount != null) {
          updateStatus(`Сформировано: ${result.dialogCount} диалогов, ${result.messageCount} сообщений`, 'muted');
        } else {
          updateStatus('Архив сформирован', 'muted');
        }
      } catch (error) {
        const message = (error && error.message) || 'Не удалось скачать архив';
        updateStatus(message, 'alert');
        try {
          console.error('WhatsApp export failed', error);
        } catch (_) {}
      } finally {
        button.disabled = false;
      }
    });

    if (button.dataset) {
      button.dataset.bound = '1';
    }

    window.__EXPORT_BIND_OK__ = true;

    return true;
  }

  const extractVersion = (src) => {
    if (typeof src !== 'string' || !src) return 'unknown';
    try {
      const baseOrigin = getLocation().origin || undefined;
      const url = new URL(src, baseOrigin);
      return url.searchParams.get('v') || 'unknown';
    } catch (error) {
      const match = src.match(/[?&]v=([^&]+)/);
      if (match && match[1]) {
        return match[1];
      }
      return 'unknown';
    }
  };

  const scriptNode = document.currentScript;
  const scriptVersion = extractVersion(scriptNode ? scriptNode.getAttribute('src') : '');
  const startedAt = Date.now();
  console.info('[client-settings] init version=%s started=%s', scriptVersion, new Date(startedAt).toISOString());

  (function () {
    const domState = readStateFromDom();
    const globalState = typeof window !== 'undefined' && window.state && typeof window.state === 'object' ? window.state : {};
    const hasDomState = domState && typeof domState === 'object' && Object.keys(domState).length > 0;
    const state = hasDomState ? domState : (globalState && typeof globalState === 'object' ? globalState : {});
    const clientConfig = getClientSettings();
    const tenant = determineTenant(state, { fallbackDefault: true, globalConfig: clientConfig });
    const stateAccessKey = typeof state.key === 'string' ? state.key.trim() : '';
    const statePublicKey = typeof state.public_key === 'string' ? state.public_key.trim() : '';
    const configKey = (() => {
      if (clientConfig && typeof clientConfig.key === 'string') {
        const trimmed = clientConfig.key.trim();
        if (trimmed) {
          return trimmed;
        }
      }
      return '';
    })();

    const configPublicKey = (() => {
      if (clientConfig && typeof clientConfig.public_key === 'string') {
        const trimmed = clientConfig.public_key.trim();
        if (trimmed) {
          return trimmed;
        }
      }
      return '';
    })();

    const resolveEffectiveAccessKey = () => {
      if (stateAccessKey) {
        return stateAccessKey;
      }
      if (configKey) {
        return configKey;
      }
      if (statePublicKey) {
        return statePublicKey;
      }
      if (configPublicKey) {
        return configPublicKey;
      }
      return '';
    };

    const fallbackBuildUrl = (path, options = {}) => {
      const { includeKey = true } = options || {};
      const locationInfo = getLocation();
      let origin = locationInfo.origin || '';
      if (!origin) {
        try {
          origin = new URL(locationInfo.href || '/', 'https://localhost').origin;
        } catch (_) {
          origin = 'https://localhost';
        }
      }
      const protocol = locationInfo.protocol || '';
      const host = locationInfo.host || '';
      const hostname = locationInfo.hostname || '';
      const port = locationInfo.port || '';
      let url;
      try {
        url = new URL(path, origin);
      } catch (error) {
        try {
          console.error('Failed to resolve URL', path, error);
        } catch (_) {}
        url = new URL(origin);
      }

      if (url.hostname !== hostname) {
        url = new URL(url.pathname + url.search + url.hash, origin);
      }

      if (url.hostname === hostname && url.protocol !== protocol) {
        url.protocol = protocol;
        url.port = port;
      } else if (url.host === host && url.protocol !== protocol) {
        url.protocol = protocol;
      }

      if (includeKey) {
        const keyValue = resolveEffectiveAccessKey();
        if (keyValue) {
          url.searchParams.set('k', keyValue);
        }
      }
      return url.toString();
    };

    fallbackBuildUrl.getKey = resolveEffectiveAccessKey;

    const buildUrl = (typeof window !== 'undefined' && typeof window.buildUrl === 'function')
      ? window.buildUrl
      : fallbackBuildUrl;

    const builderAccessKey = typeof buildUrl.getKey === 'function' ? (buildUrl.getKey() || '') : '';
    const accessKey = (resolveEffectiveAccessKey() || builderAccessKey || '').trim();
    const urls = state && typeof state === 'object' ? state.urls || {} : {};
    const initialQrId = typeof state.qr_id === 'string' ? state.qr_id.trim() : '';
    const resolvedMaxDays = resolveMaxDays(state);
    const maxDays = resolvedMaxDays != null ? resolvedMaxDays : 30;

    const tenantCandidates = [];
    if (Number.isFinite(tenant) && tenant > 0) {
      tenantCandidates.push(String(tenant));
    }
    if (clientConfig && Object.prototype.hasOwnProperty.call(clientConfig, 'tenant')) {
      tenantCandidates.push(clientConfig.tenant);
    }
    if (clientConfig && Object.prototype.hasOwnProperty.call(clientConfig, 'tenant_id')) {
      tenantCandidates.push(clientConfig.tenant_id);
    }
    let tenantString = '';
    for (let idx = 0; idx < tenantCandidates.length; idx += 1) {
      const candidate = tenantCandidates[idx];
      if (candidate == null) {
        continue;
      }
      const normalized = String(candidate).trim();
      if (normalized && normalized !== 'undefined' && normalized !== 'null') {
        tenantString = normalized;
        break;
      }
    }
    const withTenant = (params = {}) => {
      const baseParams = params && typeof params === 'object' ? { ...params } : {};
      if (tenantString && baseParams.tenant == null) {
        baseParams.tenant = tenantString;
      }
      return baseParams;
    };

    const ABSOLUTE_URL_RE = /^[a-zA-Z][a-zA-Z0-9+.-]*:/;

    const normalizeEndpointPath = (value, fallback = '') => {
      const raw = typeof value === 'string' ? value.trim() : '';
      const fallbackRaw = typeof fallback === 'string' ? fallback.trim() : '';
      if (raw) {
        if (fallbackRaw && fallbackRaw.startsWith('/pub/') && raw.startsWith('/client/')) {
          return fallbackRaw;
        }
        if (fallbackRaw && fallbackRaw.startsWith('/pub/') && !ABSOLUTE_URL_RE.test(raw)) {
          const candidatePath = raw.startsWith('/') ? raw : `/${raw}`;
          if (!candidatePath.startsWith('/pub/')) {
            return fallbackRaw;
          }
        }
        if (ABSOLUTE_URL_RE.test(raw)) {
          return raw;
        }
        if (raw.startsWith('//')) {
          const protocol = getLocation().protocol || 'https:';
          return `${protocol}${raw}`;
        }
        if (raw.startsWith('/')) {
          return raw;
        }
        if (raw.startsWith('pub/')) {
          return `/${raw}`;
        }
        if (raw.startsWith('client/')) {
          return `/${raw}`;
        }
        if (raw.startsWith('./') || raw.startsWith('../')) {
          return raw;
        }
        return `/${raw}`;
      }
      if (!fallbackRaw) {
        return '';
      }
      return normalizeEndpointPath(fallbackRaw, '');
    };

    function resolveEndpointUrl(raw, extraParams = {}, fallback = '') {
      const base = normalizeEndpointPath(raw, fallback);
      if (!base) {
        return '';
      }
      const built = buildUrl(base);
      const candidate = built || base;
      const locationInfo = getLocation();
      let url;
      try {
        url = new URL(candidate, locationInfo.origin || 'https://localhost');
      } catch (error) {
        url = new URL(candidate, locationInfo.href || 'https://localhost');
      }
      const pathName = url.pathname || '';
      Object.entries(extraParams || {}).forEach(([key, value]) => {
        if (value === undefined || value === null) return;
        if (key === 'tenant' && !pathName.startsWith('/pub/')) {
          return;
        }
        url.searchParams.set(key, String(value));
      });
      const baseIsAbsolute = ABSOLUTE_URL_RE.test(base);
      const candidateIsAbsolute = ABSOLUTE_URL_RE.test(candidate);
      const isRootRelative = !baseIsAbsolute && base.startsWith('/');
      if (isRootRelative && !candidateIsAbsolute) {
        return `${url.pathname}${url.search}${url.hash}`;
      }
      if (!isRootRelative && !baseIsAbsolute && base.startsWith('pub/')) {
        return `${url.pathname}${url.search}${url.hash}`;
      }
      return url.toString();
    }

    const endpoints = {
      saveSettings: normalizeEndpointPath(urls.save_settings, '/pub/settings/save'),
      savePersona: normalizeEndpointPath(urls.save_persona, `/client/${tenant}/persona`),
      uploadCatalog: normalizeEndpointPath(urls.upload_catalog, '/pub/catalog/upload'),
      csvGet: normalizeEndpointPath(urls.csv_get, '/pub/catalog/csv'),
      csvSave: normalizeEndpointPath(urls.csv_save, '/pub/catalog/csv'),
      trainingUpload: normalizeEndpointPath(urls.training_upload, '/pub/training/upload'),
      trainingStatus: normalizeEndpointPath(urls.training_status, '/pub/training/status'),
      whatsappExport: normalizeEndpointPath(urls.whatsapp_export, '/pub/wa/export'),
    };

    const telegram = {
      startUrl: normalizeEndpointPath(urls.tg_start, '/pub/tg/start'),
      statusUrl: normalizeEndpointPath(urls.tg_status, '/pub/tg/status'),
      logoutUrl: normalizeEndpointPath(urls.tg_logout, '/pub/tg/logout'),
      qrUrl: normalizeEndpointPath(urls.tg_qr_png || urls.tg_qr, '/pub/tg/qr.png'),
      qrTxtUrl: normalizeEndpointPath(urls.tg_qr_txt, '/pub/tg/qr.txt'),
      passwordUrl: normalizeEndpointPath(urls.tg_2fa_url || urls.tg_password, '/pub/tg/2fa'),
    };

  const dom = {
    settingsForm: document.getElementById('settings-form'),
    saveSettings: document.getElementById('save-settings'),
    settingsMessage: document.getElementById('settings-message'),
    personaTextarea: document.getElementById('persona-text'),
    savePersona: document.getElementById('save-persona'),
    personaMessage: document.getElementById('persona-message'),
    downloadConfig: document.getElementById('download-config'),
    catalogForm: document.getElementById('catalogForm') || document.getElementById('catalog-upload-form'),
    uploadInput: document.getElementById('catalogFile') || document.getElementById('catalog-file'),
    catalogUploadButton: document.getElementById('catalogUploadBtn') || document.getElementById('catalog-upload-btn'),
    catalogUploadStatus: document.getElementById('catalogUploadStatus') || document.getElementById('catalog-upload-status'),
    catalogUploadProgress: document.getElementById('catalogUploadProgress') || document.getElementById('catalog-upload-progress'),
    catalogUploadProgressBar: document.getElementById('catalogUploadProgressBar') || document.getElementById('catalog-upload-progress'),
    csvTable: document.getElementById('csv-table'),
    csvEmpty: document.getElementById('csv-empty'),
    csvContainer: document.getElementById('csv-container'),
    csvSection: document.getElementById('csv-section'),
    csvMessage: document.getElementById('csv-message'),
    csvAddRow: document.getElementById('csv-add-row'),
    csvSave: document.getElementById('csv-save'),
    csvRefresh: document.getElementById('csv-refresh'),
    trainingUploadForm: document.getElementById('training-upload-form'),
    trainingUploadInput: document.querySelector('#training-upload-form input[name="file"]'),
    trainingUploadSubmit: document.querySelector('#training-upload-form [data-role="training-upload-submit"], #training-upload-submit'),
    trainingUploadMessage: document.getElementById('training-upload-message'),
    trainingCheckStatus: document.getElementById('training-check-status'),
    trainingStatus: document.getElementById('training-status'),
    expDays: document.getElementById('exp-days'),
    expLimit: document.getElementById('exp-limit'),
    expPer: document.getElementById('exp-per'),
    exportDownload: document.getElementById('export-download'),
    exportStatus: document.getElementById('export-status'),
    tgIntegrationCard: document.querySelector('.integration-card[data-tg-initial-status]'),
    tgStatus: document.getElementById('tg-integration-status'),
    tgRefresh: document.getElementById('tg-integration-refresh'),
    tgDisconnect: document.getElementById('tg-integration-disconnect'),
    tgConnect: document.getElementById('tg-integration-connect'),
    tgQrBlock: document.getElementById('tg-qr-block'),
    tgQrImage: document.getElementById('tg-qr-image'),
    tgQrFallback: document.getElementById('tg-qr-fallback'),
    tgQrLink: document.getElementById('tg-qr-link'),
    tgQrRefresh: document.getElementById('tg-qr-refresh'),
    tg2faBlock: document.getElementById('tg-2fa-block'),
    tgPasswordForm: document.getElementById('tg-password-form'),
    tgPasswordInput: document.getElementById('tg-password-input'),
    tgPasswordSubmit: document.getElementById('tg-password-submit'),
    tgPasswordError: document.getElementById('tg-password-error'),
  };

  let currentTelegramQrId = initialQrId;
  let telegramStatusPollTimer = null;
  const waitingPollInterval = (() => {
    if (dom.tgIntegrationCard && dom.tgIntegrationCard.dataset) {
      const raw = Number(dom.tgIntegrationCard.dataset.tgPollInterval || '0');
      if (Number.isFinite(raw) && raw > 0) {
        return Math.max(500, raw);
      }
    }
    return 2500;
  })();
  const fallbackPollInterval = Math.max(500, waitingPollInterval + 500);
  let passwordPromptVisible = false;
  let qrImageReloadPending = false;
  let catalogUploadInFlight = false;
  let catalogStatusPollTimer = null;
  let catalogStatusContext = null;
  const CATALOG_STATUS_POLL_INTERVAL = 2000;
  const HIDDEN_CLASS = 'hidden';
  const TELEGRAM_STATUS_MAX_ERROR_ATTEMPTS = 5;
  const TELEGRAM_STATUS_RETRY_BASE_DELAY = 4000;
  const TELEGRAM_STATUS_RETRY_MAX_DELAY = 60000;
  let telegramStatusErrorAttempts = 0;

  function toBoolean(value) {
    if (value === true) return true;
    if (value === false || value == null) return false;
    if (typeof value === 'number') {
      if (Number.isNaN(value)) return false;
      return value !== 0;
    }
    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      if (!normalized) return false;
      return ['1', 'true', 'yes', 'on'].includes(normalized);
    }
    return false;
  }

  function showElement(element) {
    if (!element) return;
    element.classList.remove(HIDDEN_CLASS);
  }

  function hideElement(element) {
    if (!element) return;
    if (!element.classList.contains(HIDDEN_CLASS)) {
      element.classList.add(HIDDEN_CLASS);
    }
  }

  function buildTelegramTenantUrl(base, extraParams = {}) {
    if (!base) return '';
    return resolveEndpointUrl(base, withTenant(extraParams), base);
  }

  function buildTelegramQrUrl(qrId) {
    if (!telegram.qrUrl || !qrId) return '';
    return resolveEndpointUrl(telegram.qrUrl, withTenant({ qr_id: qrId, t: Date.now() }), telegram.qrUrl);
  }

  function buildTelegramQrTextUrl(qrId) {
    if (!telegram.qrTxtUrl || !qrId) return '';
    return resolveEndpointUrl(telegram.qrTxtUrl, withTenant({ qr_id: qrId }), telegram.qrTxtUrl);
  }

  function updateTelegramQrLink(qrId) {
    if (!dom.tgQrLink) return;
    if (qrId) {
      const href = buildTelegramQrTextUrl(qrId);
      if (href) {
        dom.tgQrLink.href = href;
        showElement(dom.tgQrLink);
        return;
      }
    }
    dom.tgQrLink.removeAttribute('href');
    hideElement(dom.tgQrLink);
  }

  function showTelegramQr(qrId) {
    if (!dom.tgQrBlock || !dom.tgQrImage) return;
    const safeId = qrId ? String(qrId).trim() : '';
    if (!safeId) {
      hideTelegramQr();
      return;
    }
    const src = buildTelegramQrUrl(safeId);
    if (!src) {
      hideTelegramQr();
      return;
    }
    currentTelegramQrId = safeId;
    if (dom.tgQrBlock.dataset) {
      dom.tgQrBlock.dataset.qrId = safeId;
    }
    showElement(dom.tgQrBlock);
    if (dom.tgQrImage.dataset) {
      dom.tgQrImage.dataset.cachebuster = String(Date.now());
    }
    dom.tgQrImage.src = src;
    showElement(dom.tgQrImage);
    if (dom.tgQrFallback) {
      dom.tgQrFallback.textContent = '';
      hideElement(dom.tgQrFallback);
    }
    updateTelegramQrLink(safeId);
    qrImageReloadPending = false;
  }

  function hideTelegramQr(message = '', options = {}) {
    currentTelegramQrId = '';
    if (dom.tgQrImage) {
      if (dom.tgQrImage.dataset) {
        dom.tgQrImage.dataset.cachebuster = '';
      }
      dom.tgQrImage.removeAttribute('src');
      hideElement(dom.tgQrImage);
    }
    const keepContainer = Boolean(options && options.keepContainer);
    if (dom.tgQrBlock) {
      if (dom.tgQrBlock.dataset) {
        dom.tgQrBlock.dataset.qrId = '';
      }
      const text = message ? String(message).trim() : '';
      if (keepContainer || text) {
        showElement(dom.tgQrBlock);
      } else {
        hideElement(dom.tgQrBlock);
      }
    }
    if (dom.tgQrFallback) {
      const text = message ? String(message) : '';
      dom.tgQrFallback.textContent = text;
      if (text) {
        showElement(dom.tgQrFallback);
      } else {
        hideElement(dom.tgQrFallback);
      }
    }
    updateTelegramQrLink('');
    qrImageReloadPending = false;
  }

  function handleTelegramQrError() {
    if (!currentTelegramQrId || qrImageReloadPending) {
      return;
    }
    qrImageReloadPending = true;
    if (dom.tgQrFallback) {
      dom.tgQrFallback.textContent = 'Обновляем QR…';
      showElement(dom.tgQrFallback);
    }
    if (dom.tgQrBlock) {
      showElement(dom.tgQrBlock);
    }
    if (!fromPoll) {
      telegramStatusErrorAttempts = 0;
    }
    stopTelegramPolling();
    refreshTelegramStatus({ fromPoll: true });
  }

  function setQrRefreshVisibility(visible, label) {
    const button = dom.tgQrRefresh;
    if (!button) return;
    if (button.dataset && !button.dataset.defaultLabel) {
      button.dataset.defaultLabel = button.textContent || 'Обновить QR';
    }
    const allow = Boolean(visible) && Boolean(accessKey);
    const defaultLabel = button.dataset ? button.dataset.defaultLabel : '';
    if (allow) {
      button.textContent = label || defaultLabel || 'Обновить QR';
      showElement(button);
      if (dom.tgQrBlock) {
        showElement(dom.tgQrBlock);
      }
    } else {
      if (defaultLabel) {
        button.textContent = defaultLabel;
      }
      hideElement(button);
    }
  }

  function setQrRefreshDisabled(disabled) {
    if (!dom.tgQrRefresh) return;
    dom.tgQrRefresh.disabled = Boolean(disabled);
  }

  function updatePasswordStatus(message, variant = 'muted') {
    if (!dom.tgPasswordError) return;
    setStatus(dom.tgPasswordError, message, variant);
    if (message) {
      dom.tgPasswordError.classList.remove(HIDDEN_CLASS);
    } else {
      dom.tgPasswordError.classList.add(HIDDEN_CLASS);
    }
  }

  function showTwoFactorPrompt(message) {
    if (dom.tg2faBlock) {
      showElement(dom.tg2faBlock);
    }
    if (dom.tgPasswordForm) {
      showElement(dom.tgPasswordForm);
    }
    if (!passwordPromptVisible && dom.tgPasswordInput) {
      dom.tgPasswordInput.value = '';
    }
    passwordPromptVisible = true;
    updatePasswordStatus(message || '', 'muted');
    setQrRefreshDisabled(true);
    if (dom.tgPasswordInput) {
      try {
        dom.tgPasswordInput.focus();
      } catch (_) {}
    }
  }

  function hideTwoFactorPrompt() {
    if (dom.tg2faBlock) {
      hideElement(dom.tg2faBlock);
    }
    if (dom.tgPasswordForm) {
      hideElement(dom.tgPasswordForm);
    }
    passwordPromptVisible = false;
    if (dom.tgPasswordInput) {
      dom.tgPasswordInput.value = '';
    }
    updatePasswordStatus('', 'muted');
    setQrRefreshDisabled(false);
  }

  function applyTelegramStatus(data) {
    const rawStatus = data && typeof data.status === 'string' ? data.status.trim() : '';
    const normalized = rawStatus.toLowerCase();
    const qrId = data && data.qr_id ? String(data.qr_id) : '';
    const needsTwoFactor = Boolean(
      rawStatus === 'needs_2fa'
      || toBoolean(data && data.needs_2fa)
      || toBoolean(data && data.twofa_pending),
    );
    const lastError = data && typeof data.last_error === 'string' ? data.last_error : '';
    const showNewQrButton = normalized === 'disconnected'
      && (lastError === 'qr_login_timeout' || lastError === 'twofa_timeout');

    if (needsTwoFactor) {
      hideTelegramQr();
      showTwoFactorPrompt('Введите пароль двухфакторной аутентификации в Telegram.');
      setQrRefreshVisibility(false);
      setQrRefreshDisabled(true);
      return { status: 'needs_2fa', needsTwoFactor: true, lastError };
    }

    hideTwoFactorPrompt();
    setQrRefreshDisabled(false);

    if (normalized === 'waiting_qr') {
      if (qrId) {
        showTelegramQr(qrId);
      } else {
        hideTelegramQr('', { keepContainer: true });
      }
      setQrRefreshVisibility(true);
      setQrRefreshDisabled(false);
      return { status: 'waiting_qr', needsTwoFactor: false, lastError };
    }

    if (normalized === 'authorized') {
      hideTelegramQr();
      setQrRefreshVisibility(false);
      setQrRefreshDisabled(true);
      return { status: 'authorized', needsTwoFactor: false, lastError };
    }

    if (showNewQrButton) {
      hideTelegramQr('', { keepContainer: true });
      setQrRefreshVisibility(true, 'Обновить QR');
      setQrRefreshDisabled(false);
    } else {
      hideTelegramQr();
      setQrRefreshVisibility(false);
      setQrRefreshDisabled(true);
    }

    return { status: normalized || rawStatus, needsTwoFactor: false, lastError };
  }

  function stopTelegramPolling() {
    if (telegramStatusPollTimer !== null) {
      window.clearTimeout(telegramStatusPollTimer);
      telegramStatusPollTimer = null;
    }
  }

  function scheduleTelegramPolling(delayMs, options = {}) {
    const { resetErrors = false } = options || {};
    stopTelegramPolling();
    if (resetErrors) {
      telegramStatusErrorAttempts = 0;
    }
    telegramStatusPollTimer = window.setTimeout(() => {
      telegramStatusPollTimer = null;
      refreshTelegramStatus({ fromPoll: true });
    }, Math.max(500, Number(delayMs) || 0));
  }

  function scheduleTelegramErrorRetry() {
    if (telegramStatusErrorAttempts >= TELEGRAM_STATUS_MAX_ERROR_ATTEMPTS) {
      try {
        console.warn('[client-settings] telegram status retry limit reached', TELEGRAM_STATUS_MAX_ERROR_ATTEMPTS);
      } catch (_) {}
      stopTelegramPolling();
      return;
    }
    telegramStatusErrorAttempts += 1;
    const exponent = telegramStatusErrorAttempts - 1;
    const delay = Math.min(
      TELEGRAM_STATUS_RETRY_BASE_DELAY * (2 ** Math.max(0, exponent)),
      TELEGRAM_STATUS_RETRY_MAX_DELAY,
    );
    try {
      const attemptInfo = `${telegramStatusErrorAttempts}/${TELEGRAM_STATUS_MAX_ERROR_ATTEMPTS}`;
      const retryMessage = `[client-settings] telegram status retry in ${delay}ms (attempt ${attemptInfo})`;
      console.warn(retryMessage);
    } catch (_) {}
    scheduleTelegramPolling(delay);
  }

  function evaluateTelegramPolling(statusValue) {
    if (!accessKey) {
      stopTelegramPolling();
      return;
    }
    const normalized = (statusValue || '').toLowerCase();
    if (normalized === 'authorized') {
      stopTelegramPolling();
      return;
    }
    const delay = normalized === 'waiting_qr' ? waitingPollInterval : fallbackPollInterval;
    scheduleTelegramPolling(delay, { resetErrors: true });
  }

  function setStatus(element, message, variant = 'muted') {
    if (!element) return;
    element.className = `status-text ${variant}`.trim();
    element.textContent = message || '';
  }

  async function postJSON(targetUrl, payload) {
    const resolvedUrl = typeof targetUrl === 'string' ? targetUrl.trim() : '';
    if (!resolvedUrl) {
      throw new Error('URL сервиса не задан');
    }
    const response = await fetch(resolvedUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error((await response.text()) || 'Ошибка запроса');
    }
    const data = await response.json();
    if (data && data.ok === false && data.error) {
      throw new Error(data.error);
    }
    return data;
  }

  function parseIntOrNull(value) {
    if (typeof value !== 'string') {
      return null;
    }
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    const parsed = Number.parseInt(trimmed, 10);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function normalizeDays(raw, maxAllowed) {
    const parsed = parseIntOrNull(raw);
    if (parsed == null || parsed < 0) {
      return 0;
    }
    if (Number.isFinite(maxAllowed) && maxAllowed > 0 && parsed > maxAllowed) {
      return maxAllowed;
    }
    return parsed;
  }

  function normalizeLimit(raw) {
    const parsed = parseIntOrNull(raw);
    if (parsed == null || parsed <= 0) {
      return 10000;
    }
    return parsed;
  }

  if (dom.saveSettings && dom.settingsForm) {
    dom.saveSettings.addEventListener('click', async () => {
      const formData = new FormData(dom.settingsForm);
      const payload = Object.fromEntries(formData.entries());
      try {
        const targetUrl = resolveEndpointUrl(endpoints.saveSettings, withTenant());
        await postJSON(targetUrl, payload);
        setStatus(dom.settingsMessage, 'Паспорт сохранён', 'muted');
      } catch (error) {
        setStatus(dom.settingsMessage, `Не удалось сохранить: ${error.message}`, 'alert');
      }
    });
  }

  if (dom.savePersona && dom.personaTextarea) {
    dom.savePersona.addEventListener('click', async () => {
      try {
        const targetUrl = resolveEndpointUrl(endpoints.savePersona, withTenant());
        await postJSON(targetUrl, { text: dom.personaTextarea.value });
        setStatus(dom.personaMessage, 'Персона обновлена', 'muted');
      } catch (error) {
        setStatus(dom.personaMessage, `Не удалось обновить: ${error.message}`, 'alert');
      }
    });
  }

  if (dom.downloadConfig) {
    dom.downloadConfig.addEventListener('click', async () => {
      try {
        const settingsUrl = resolveEndpointUrl('/pub/settings/get', withTenant());
        if (!settingsUrl) {
          throw new Error('Ссылка недоступна');
        }
        const data = await fetchSettingsJsonWithRetry(settingsUrl);
        const uploadedMeta = extractUploadedCatalogMeta(data);
        if (uploadedMeta && typeof uploadedMeta.csv_path === 'string' && uploadedMeta.csv_path.trim()) {
          fetchCsvAndRender({ quiet: true });
        }
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = `tenant-${tenant}-config.json`;
        anchor.click();
        URL.revokeObjectURL(url);
        setStatus(dom.personaMessage, 'JSON конфигурация скачана', 'muted');
      } catch (error) {
        setStatus(dom.personaMessage, `Не удалось скачать JSON: ${error.message}`, 'alert');
      }
    });
  }

  function disableCatalogUploadButton(disabled) {
    if (!dom.catalogUploadButton) return;
    dom.catalogUploadButton.disabled = Boolean(disabled);
  }

  function resetCatalogUploadUi() {
    const progressEl = dom.catalogUploadProgress;
    const barEl = dom.catalogUploadProgressBar;
    if (progressEl && progressEl.tagName === 'PROGRESS') {
      progressEl.value = 0;
      progressEl.hidden = true;
      return;
    }
    if (barEl && barEl !== progressEl) {
      barEl.style.width = '0%';
    }
    if (progressEl) {
      progressEl.style.display = 'none';
    }
  }

  function updateCatalogProgress(percent) {
    const progressEl = dom.catalogUploadProgress;
    const barEl = dom.catalogUploadProgressBar;
    const numeric = Number(percent);
    const bounded = Number.isFinite(numeric) ? Math.max(0, Math.min(100, numeric)) : 0;
    if (progressEl && progressEl.tagName === 'PROGRESS') {
      progressEl.hidden = false;
      progressEl.value = bounded;
      return;
    }
    if (!progressEl || !barEl || barEl === progressEl) return;
    progressEl.style.display = 'block';
    barEl.style.width = `${bounded}%`;
  }

  function setCatalogStatus(message, variant = 'muted', options = {}) {
    if (!dom.catalogUploadStatus) return;
    const link = options && options.link ? options.link : null;
    dom.catalogUploadStatus.className = `status-text ${variant}`.trim();
    dom.catalogUploadStatus.textContent = '';
    if (message) {
      dom.catalogUploadStatus.appendChild(document.createTextNode(message));
    }
    if (link && link.href) {
      if (message) {
        dom.catalogUploadStatus.appendChild(document.createTextNode(' '));
      }
      const anchor = document.createElement('a');
      anchor.href = link.href;
      anchor.textContent = link.label || link.href;
      anchor.target = '_blank';
      anchor.rel = 'noopener noreferrer';
      dom.catalogUploadStatus.appendChild(anchor);
    }
  }

  function stopCatalogStatusPolling(options = {}) {
    const { reset = false } = options || {};
    if (catalogStatusPollTimer) {
      clearTimeout(catalogStatusPollTimer);
      catalogStatusPollTimer = null;
    }
    catalogStatusContext = null;
    if (reset) {
      resetCatalogUploadUi();
    }
  }

  function resolveTenantForCatalog(clientConfig) {
    if (clientConfig && clientConfig.tenant != null) {
      const candidate = String(clientConfig.tenant).trim();
      if (candidate) return candidate;
    }
    if (clientConfig && clientConfig.tenant_id != null) {
      const candidate = String(clientConfig.tenant_id).trim();
      if (candidate) return candidate;
    }
    if (tenantString) return tenantString;
    if (Number.isFinite(tenant) && tenant > 0) {
      return String(tenant);
    }
    return '';
  }

  function resolvePublicKeyForCatalog(clientConfig) {
    if (clientConfig && typeof clientConfig.public_key === 'string' && clientConfig.public_key.trim()) {
      return clientConfig.public_key.trim();
    }
    if (clientConfig && typeof clientConfig.key === 'string' && clientConfig.key.trim()) {
      return clientConfig.key.trim();
    }
    const fallbackKey = resolveEffectiveAccessKey();
    return fallbackKey ? String(fallbackKey).trim() : '';
  }

  function resolveWebhookSecret(clientConfig) {
    if (clientConfig && typeof clientConfig.webhook_secret === 'string') {
      const trimmed = clientConfig.webhook_secret.trim();
      if (trimmed) return trimmed;
    }
    return '';
  }

  function buildCatalogUploadUrl(publicKey, tenantValue) {
    if (!publicKey || !tenantValue) return '';
    const locationInfo = getLocation();
    let url;
    try {
      url = new URL('/pub/catalog/upload', locationInfo.origin || 'https://localhost');
    } catch (error) {
      url = new URL('/pub/catalog/upload', locationInfo.href || 'https://localhost');
    }
    url.searchParams.set('k', publicKey);
    url.searchParams.set('tenant', String(tenantValue));
    return url.toString();
  }

  function buildCatalogStatusUrl(jobId, context) {
    if (!jobId || !context || !context.publicKey || !context.tenant) {
      return '';
    }
    const locationInfo = getLocation();
    let url;
    try {
      url = new URL('/pub/catalog/status', locationInfo.origin || 'https://localhost');
    } catch (error) {
      url = new URL('/pub/catalog/status', locationInfo.href || 'https://localhost');
    }
    url.searchParams.set('k', context.publicKey);
    url.searchParams.set('tenant', String(context.tenant));
    url.searchParams.set('job', String(jobId));
    return url.toString();
  }

  function buildInternalFileUrl(pathValue, context) {
    if (!context || !context.tenant || !context.webhookSecret) return '';
    const safePath = typeof pathValue === 'string' ? pathValue.replace(/\\/g, '/') : '';
    if (!safePath) return '';
    const tenantId = encodeURIComponent(String(context.tenant).trim());
    const locationInfo = getLocation();
    let url;
    try {
      url = new URL(`/internal/tenant/${tenantId}/catalog-file`, locationInfo.origin || 'https://localhost');
    } catch (error) {
      url = new URL(`/internal/tenant/${tenantId}/catalog-file`, locationInfo.href || 'https://localhost');
    }
    url.searchParams.set('path', safePath);
    url.searchParams.set('token', context.webhookSecret);
    return url.toString();
  }

  async function refreshSettingsSnapshot(context) {
    if (!context || !context.publicKey || !context.tenant) {
      throw new Error('invalid_context');
    }
    const locationInfo = getLocation();
    let url;
    try {
      url = new URL('/pub/settings/get', locationInfo.origin || 'https://localhost');
    } catch (error) {
      url = new URL('/pub/settings/get', locationInfo.href || 'https://localhost');
    }
    url.searchParams.set('k', context.publicKey);
    url.searchParams.set('tenant', String(context.tenant));
    return fetchSettingsJsonWithRetry(url.toString());
  }

  function handleCatalogStatusPayload(payload, context) {
    const stateRaw = typeof payload.state === 'string' ? payload.state.toLowerCase() : '';
    if (!stateRaw) {
      scheduleCatalogStatusPoll();
      return;
    }
    if (stateRaw === 'done') {
      stopCatalogStatusPolling();
      updateCatalogProgress(100);
      handleCatalogCompleted(context, payload);
      return;
    }
    if (stateRaw === 'failed') {
      stopCatalogStatusPolling();
      catalogUploadInFlight = false;
      disableCatalogUploadButton(false);
      const message = payload.error || payload.message || 'Ошибка обработки каталога';
      setCatalogStatus(`Ошибка: ${message}`, 'alert');
      resetCatalogUploadUi();
      return;
    }
    updateCatalogProgress(100);
    const details = [];
    const humanState = describeCatalogState(stateRaw || payload.state || '');
    if (humanState) {
      details.push(humanState);
    }
    const formattedTs = formatCatalogTimestamp(payload.updated_at);
    if (formattedTs) {
      details.push(`обновлено ${formattedTs}`);
    }
    const suffix = details.length ? ` (${details.join(' · ')})` : '';
    setCatalogStatus(`Идёт обработка…${suffix}`, 'muted');
    scheduleCatalogStatusPoll();
  }

  function formatCatalogTimestamp(value) {
    if (value == null) return '';
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric <= 0) return '';
    const date = new Date(numeric * 1000);
    if (Number.isNaN(date.getTime())) return '';
    try {
      return date.toLocaleTimeString();
    } catch (error) {
      return '';
    }
  }

  function describeCatalogState(state) {
    const normalized = (state || '').toLowerCase();
    switch (normalized) {
      case 'pending':
      case 'queued':
        return 'В очереди';
      case 'received':
        return 'Получен';
      case 'processing':
        return 'Обработка';
      case 'done':
        return 'Готово';
      case 'failed':
        return 'Ошибка';
      default:
        return state || 'Неизвестно';
    }
  }

  function extractUploadedCatalogMeta(settings) {
    if (!settings || typeof settings !== 'object') {
      return null;
    }
    const cfg = settings.cfg && typeof settings.cfg === 'object' ? settings.cfg : {};
    const integrations = cfg.integrations && typeof cfg.integrations === 'object' ? cfg.integrations : {};
    if (integrations.uploaded_catalog && typeof integrations.uploaded_catalog === 'object') {
      return integrations.uploaded_catalog;
    }
    const catalogs = Array.isArray(cfg.catalogs) ? cfg.catalogs : [];
    if (catalogs.length) {
      const entry = catalogs[0];
      if (entry && typeof entry === 'object') {
        return entry;
      }
    }
    return null;
  }

  function resolveCatalogDownloadLink(settings, context, payload) {
    const meta = extractUploadedCatalogMeta(settings);
    const candidates = [];
    if (payload && typeof payload.csv_path === 'string' && payload.csv_path) {
      candidates.push(payload.csv_path);
    }
    if (meta && typeof meta.csv_path === 'string' && meta.csv_path) {
      candidates.push(meta.csv_path);
    }
    if (meta && typeof meta.path === 'string' && meta.path) {
      candidates.push(meta.path);
    }
    for (let idx = 0; idx < candidates.length; idx += 1) {
      const href = buildInternalFileUrl(candidates[idx], context);
      if (href) {
        return href;
      }
    }
    return '';
  }

  function resolveCatalogSuccessLabel(settings, payload) {
    const meta = extractUploadedCatalogMeta(settings);
    const csvPath = (meta && typeof meta.csv_path === 'string' && meta.csv_path)
      || (payload && typeof payload.csv_path === 'string' && payload.csv_path)
      || '';
    const original = (meta && typeof meta.original === 'string' && meta.original)
      || (payload && typeof payload.original === 'string' && payload.original)
      || (payload && typeof payload.filename === 'string' && payload.filename)
      || '';
    let message = 'Готово';
    if (csvPath) {
      message += `. CSV: ${csvPath}`;
    } else if (original) {
      message += `. Файл: ${original}`;
    }
    return message;
  }

  async function handleCatalogCompleted(context, payload) {
    try {
      const settings = await refreshSettingsSnapshot(context);
      const linkHref = resolveCatalogDownloadLink(settings, context, payload);
      const message = resolveCatalogSuccessLabel(settings, payload);
      if (dom.uploadInput) {
        dom.uploadInput.value = '';
      }
      if (linkHref) {
        setCatalogStatus(message, 'success', { link: { href: linkHref, label: 'Скачать CSV' } });
      } else {
        setCatalogStatus(message, 'success');
      }
      await fetchCsvAndRender({ quiet: true });
    } catch (error) {
      setCatalogStatus(`Каталог обновлён, но не удалось получить данные: ${error.message}`, 'alert');
    } finally {
      disableCatalogUploadButton(false);
      catalogUploadInFlight = false;
      resetCatalogUploadUi();
    }
  }

  function scheduleCatalogStatusPoll(delay = CATALOG_STATUS_POLL_INTERVAL) {
    if (!catalogStatusContext || !catalogStatusContext.jobId) return;
    if (catalogStatusPollTimer) {
      clearTimeout(catalogStatusPollTimer);
      catalogStatusPollTimer = null;
    }
    const safeDelay = Math.max(500, Number(delay) || CATALOG_STATUS_POLL_INTERVAL);
    catalogStatusPollTimer = setTimeout(() => {
      pollCatalogStatusOnce().catch((error) => {
        try {
          console.error('[client-settings] catalog status poll failed', error);
        } catch (_) {}
        catalogUploadInFlight = false;
        disableCatalogUploadButton(false);
        setCatalogStatus(`Ошибка статуса: ${error.message}`, 'alert');
        stopCatalogStatusPolling({ reset: true });
      });
    }, safeDelay);
  }

  async function pollCatalogStatusOnce() {
    if (!catalogStatusContext || !catalogStatusContext.jobId) {
      return;
    }
    const context = catalogStatusContext;
    const url = buildCatalogStatusUrl(context.jobId, context);
    if (!url) {
      throw new Error('status_url_unavailable');
    }
    const response = await fetch(url, { cache: 'no-store' });
    let data = null;
    try {
      data = await response.clone().json();
    } catch (error) {
      data = null;
    }
    if (response.status === 404) {
      throw new Error('not_found');
    }
    if (!response.ok) {
      const detail = (data && (data.message || data.error)) || `HTTP ${response.status}`;
      throw new Error(detail);
    }
    if (!data || typeof data !== 'object') {
      throw new Error('invalid_status_payload');
    }
    if (data.ok === false) {
      throw new Error(data.error || 'status_failed');
    }
    handleCatalogStatusPayload(data, context);
  }

  function startCatalogStatusPolling(jobId, context) {
    if (!jobId || !context) {
      return;
    }
    stopCatalogStatusPolling();
    catalogStatusContext = {
      jobId: String(jobId),
      tenant: context.tenant,
      publicKey: context.publicKey,
      webhookSecret: context.webhookSecret || '',
    };
    scheduleCatalogStatusPoll(250);
  }

  function performCatalogUpload(event) {
    if (event && typeof event.preventDefault === 'function') {
      event.preventDefault();
    }
    if (catalogUploadInFlight) return;

    const file = dom.uploadInput && dom.uploadInput.files && dom.uploadInput.files[0];
    if (!file) {
      setCatalogStatus('Выберите файл перед загрузкой', 'alert');
      return;
    }

    const clientConfig = getClientSettings();
    const tenantValue = resolveTenantForCatalog(clientConfig);
    if (!tenantValue) {
      setCatalogStatus('Не удалось определить tenant', 'alert');
      return;
    }

    const publicKey = resolvePublicKeyForCatalog(clientConfig);
    if (!publicKey) {
      setCatalogStatus('Нет публичного ключа клиента', 'alert');
      return;
    }

    const webhookSecret = resolveWebhookSecret(clientConfig);
    const uploadUrl = buildCatalogUploadUrl(publicKey, tenantValue);
    if (!uploadUrl) {
      setCatalogStatus('Не найден адрес загрузки каталога', 'alert');
      return;
    }

    const formData = new FormData();
    formData.append('file', file);

    catalogUploadInFlight = true;
    stopCatalogStatusPolling({ reset: true });
    disableCatalogUploadButton(true);
    updateCatalogProgress(0);
    setCatalogStatus('Загрузка 0%', 'muted');

    const xhr = new XMLHttpRequest();
    xhr.open('POST', uploadUrl, true);
    xhr.responseType = 'json';

    xhr.upload.onprogress = (progressEvent) => {
      if (!progressEvent) {
        return;
      }
      if (!progressEvent.lengthComputable) {
        updateCatalogProgress(10);
        setCatalogStatus('Загрузка 0%', 'muted');
        return;
      }
      const percent = progressEvent.total > 0
        ? Math.round((progressEvent.loaded / progressEvent.total) * 100)
        : 0;
      updateCatalogProgress(percent);
      setCatalogStatus(`Загрузка ${percent}%`, 'muted');
    };

    const fail = (message) => {
      catalogUploadInFlight = false;
      disableCatalogUploadButton(false);
      setCatalogStatus(message, 'alert');
      resetCatalogUploadUi();
    };

    xhr.onerror = () => {
      fail('Не удалось загрузить файл. Проверьте соединение.');
    };

    xhr.onabort = () => {
      fail('Загрузка прервана.');
    };

    xhr.onload = () => {
      const { status } = xhr;
      let responseData = null;
      if (xhr.response && typeof xhr.response === 'object') {
        responseData = xhr.response;
      } else if (xhr.responseText) {
        try {
          responseData = JSON.parse(xhr.responseText);
        } catch (error) {
          responseData = null;
        }
      }

      if (status >= 400) {
        const errorMessage = (responseData && (responseData.message || responseData.error))
          || `Ошибка загрузки (HTTP ${status})`;
        fail(errorMessage);
        return;
      }

      if (!responseData || responseData.ok === false) {
        const errorMessage = (responseData && (responseData.error || responseData.message))
          || 'Не удалось загрузить файл';
        fail(errorMessage);
        return;
      }

      updateCatalogProgress(100);
      const context = { tenant: tenantValue, publicKey, webhookSecret };
      const finishUploadPhase = () => {
        catalogUploadInFlight = false;
        disableCatalogUploadButton(false);
      };

      if (responseData.job_id) {
        finishUploadPhase();
        setCatalogStatus('Файл принят. Обработка…', 'muted');
        startCatalogStatusPolling(String(responseData.job_id), context);
        if (dom.uploadInput) {
          dom.uploadInput.value = '';
        }
        return;
      }

      finishUploadPhase();
      stopCatalogStatusPolling();
      handleCatalogCompleted(context, responseData);
      return;
    };

    try {
      xhr.send(formData);
    } catch (error) {
      fail(error && error.message ? error.message : 'Не удалось отправить файл');
    }
  }

  function bindCatalogUpload() {
    if (typeof window !== 'undefined' && window.__catalogUploadV2) {
      return;
    }
    if (dom.catalogUploadButton) {
      dom.catalogUploadButton.addEventListener('click', (event) => {
        if (event && typeof event.preventDefault === 'function') {
          event.preventDefault();
        }
        if (event && typeof event.stopPropagation === 'function') {
          event.stopPropagation();
        }
        performCatalogUpload(event);
      });
    }
    if (dom.uploadInput) {
      dom.uploadInput.addEventListener('change', (event) => {
        if (event) {
          event.preventDefault?.();
          event.stopPropagation?.();
        }
        const selected = dom.uploadInput.files && dom.uploadInput.files[0];
        if (selected) {
          setCatalogStatus(`Выбран файл ${selected.name}`, 'muted');
        } else {
          setCatalogStatus('', 'muted');
        }
      });
    }
    if (dom.catalogForm) {
      dom.catalogForm.addEventListener('submit', (event) => {
        if (event && typeof event.preventDefault === 'function') {
          event.preventDefault();
        }
        if (event && typeof event.stopPropagation === 'function') {
          event.stopPropagation();
        }
        performCatalogUpload(event);
      });
    }
  }

  // -------- Обучение: загрузка диалогов --------
  async function refreshTrainingStatus() {
    if (!dom.trainingStatus) return;
    try {
      const url = resolveEndpointUrl(endpoints.trainingStatus, withTenant());
      if (!url) {
        throw new Error('Сервис недоступен');
      }
      const response = await fetch(url, { cache: 'no-store' });
      if (!response.ok) throw new Error(await response.text());
      const data = await response.json();
      const info = data.info || {};
      const manifest = data.manifest || {};
      const pairs = manifest.pairs || info.pairs || 0;
      const ts = manifest.created_at || info.indexed_at || 0;
      const when = ts ? new Date(ts * 1000).toLocaleString() : '';
      const stats = data.export_stats || {};
      const parts = [];
      if (pairs) parts.push(`Индекс: ${pairs} пар · ${when}`);
      if (stats && (stats.total_found != null)) {
        parts.push(`Экспорт: в БД ${stats.total_found}, после аноним. ${stats.after_anonymize}, к выгрузке ${stats.after_filters}`);
      }
      dom.trainingStatus.textContent = parts.length ? parts.join(' · ') : 'Данные об обучении пока не загружены';
      dom.trainingStatus.className = 'status-text muted';
    } catch (error) {
      dom.trainingStatus.textContent = `Не удалось получить статус: ${error.message}`;
      dom.trainingStatus.className = 'status-text alert';
    }
  }

  function bindTrainingUpload() {
    if (!dom.trainingUploadForm) return;

    const sendTraining = async (event) => {
      if (event) event.preventDefault();
      if (dom.trainingUploadForm.dataset.state === 'uploading') return;
      const file = dom.trainingUploadInput && dom.trainingUploadInput.files && dom.trainingUploadInput.files[0];
      if (!file) {
        setStatus(dom.trainingUploadMessage, 'Выберите файл перед загрузкой', 'alert');
        return;
      }
      const formData = new FormData();
      formData.append('file', file);
      const targetUrlRaw = (dom.trainingUploadForm.dataset.uploadUrl || '').trim();
      const targetUrl = resolveEndpointUrl(targetUrlRaw || endpoints.trainingUpload, withTenant(), endpoints.trainingUpload);
      if (!targetUrl) {
        setStatus(dom.trainingUploadMessage, 'Не найдён адрес загрузки данных', 'alert');
        return;
      }
      dom.trainingUploadForm.dataset.state = 'uploading';
      try {
        const response = await fetch(targetUrl, {
          method: 'POST',
          body: formData,
          headers: {
            'X-Requested-With': 'XMLHttpRequest',
            Accept: 'application/json, text/plain, */*',
          },
          redirect: 'manual',
        });

        if (response.type === 'opaqueredirect' || (response.status >= 300 && response.status < 400)) {
          setStatus(dom.trainingUploadMessage, 'Файл принят, обновляем статус…', 'muted');
          await refreshTrainingStatus();
          delete dom.trainingUploadForm.dataset.state;
          return;
        }

        if (!response.ok) {
          const text = await response.text();
          throw new Error(text || `Ошибка загрузки (HTTP ${response.status})`);
        }

        const data = await response.json();
        if (!data.ok) throw new Error(data.error || 'Не удалось загрузить');
        setStatus(dom.trainingUploadMessage, `Загружено примеров: ${data.pairs || ''}`, 'muted');
        if (dom.trainingUploadInput) dom.trainingUploadInput.value = '';
        await refreshTrainingStatus();
      } catch (error) {
        setStatus(dom.trainingUploadMessage, `Ошибка загрузки: ${error.message}`, 'alert');
      } finally {
        delete dom.trainingUploadForm.dataset.state;
      }
    };

    dom.trainingUploadForm.addEventListener('submit', (event) => {
      event.preventDefault();
      sendTraining(event);
    });
    if (dom.trainingUploadSubmit) {
      dom.trainingUploadSubmit.addEventListener('click', (event) => {
        event.preventDefault();
        sendTraining(event);
      });
    }
    if (dom.trainingUploadInput) {
      dom.trainingUploadInput.addEventListener('change', (event) => {
        event.preventDefault();
        event.stopPropagation();
        const file = dom.trainingUploadInput.files && dom.trainingUploadInput.files[0];
        if (file) {
          setStatus(dom.trainingUploadMessage, `Выбран файл ${file.name}`, 'muted');
        }
      });
    }
  }

  if (dom.trainingCheckStatus) {
    dom.trainingCheckStatus.addEventListener('click', refreshTrainingStatus);
  }

  if (dom.expDays) {
    dom.expDays.setAttribute('max', String(maxDays));
    dom.expDays.addEventListener('input', (event) => {
      const target = event && event.target ? event.target : dom.expDays;
      const value = normalizeDays(target.value, maxDays);
      if (String(value) !== target.value) {
        target.value = String(value);
      }
    });
  }

  const csvState = {
    columns: [],
    rows: [],
    loading: false,
  };

  function setCsvMessage(message, variant = 'muted') {
    setStatus(dom.csvMessage, message, variant);
  }

  function updateCsvControls() {
    const hasData = csvState.columns.length > 0;
    if (dom.csvAddRow) {
      dom.csvAddRow.disabled = !hasData;
    }
    if (dom.csvSave) {
      dom.csvSave.disabled = !hasData;
    }
  }

  function ensureTableVisible(show) {
    if (!dom.csvTable || !dom.csvEmpty) return;
    const table = dom.csvTable;
    const emptyState = dom.csvEmpty;
    const container = dom.csvContainer || table.parentElement;
    const section = dom.csvSection;

    table.style.display = show ? 'table' : 'none';
    emptyState.style.display = show ? 'none' : 'block';

    if (show) {
      showElement(table);
      if (container) {
        showElement(container);
      }
      if (section) {
        showElement(section);
      }
      hideElement(emptyState);
    } else {
      hideElement(table);
      if (container) {
        hideElement(container);
      }
      if (section) {
        hideElement(section);
      }
      showElement(emptyState);
    }
  }

  function normalizeColumns(cols) {
    const seen = Object.create(null);
    const out = [];
    (Array.isArray(cols) ? cols : []).forEach((raw, idx) => {
      let name = (raw == null ? '' : String(raw)).trim();
      if (!name) name = `column_${idx + 1}`;
      if (seen[name] == null) {
        seen[name] = 0;
        out.push(name);
      } else {
        seen[name] += 1;
        out.push(`${name}_${seen[name]}`);
      }
    });
    return out;
  }

  function renderCsvTable() {
    if (!dom.csvTable) return;
    const thead = dom.csvTable.querySelector('thead');
    const tbody = dom.csvTable.querySelector('tbody');
    if (!thead || !tbody) return;

    if (!csvState.columns.length) {
      thead.innerHTML = '';
      tbody.innerHTML = '';
      ensureTableVisible(false);
      updateCsvControls();
      return;
    }

    ensureTableVisible(true);

    thead.innerHTML = '';
    const headerRow = document.createElement('tr');
    csvState.columns.forEach((column) => {
      const th = document.createElement('th');
      th.textContent = column;
      th.title = column; // show full header on hover
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    tbody.innerHTML = '';
    csvState.rows.forEach((row, rowIndex) => {
      const tr = document.createElement('tr');
      csvState.columns.forEach((_, colIndex) => {
        const td = document.createElement('td');
        td.contentEditable = 'true';
        const value = Array.isArray(row) ? row[colIndex] : row[csvState.columns[colIndex]];
        const text = value == null ? '' : String(value);
        td.textContent = text;
        td.title = text; // show full cell on hover
        td.dataset.rowIndex = String(rowIndex);
        td.dataset.colIndex = String(colIndex);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });

    if (dom.csvTable) {
      dom.csvTable.style.display = '';
    }
    if (dom.csvEmpty) {
      dom.csvEmpty.style.display = 'none';
    }
  }

  function refreshCsvDomElements() {
    dom.csvTable = document.getElementById('csv-table');
    dom.csvEmpty = document.getElementById('csv-empty');
    dom.csvContainer = document.getElementById('csv-container');
    dom.csvSection = document.getElementById('csv-section');
    dom.csvMessage = document.getElementById('csv-message');
    dom.csvAddRow = document.getElementById('csv-add-row');
    dom.csvSave = document.getElementById('csv-save');
    dom.csvRefresh = document.getElementById('csv-refresh');
  }

  function resolveCsvKey(state) {
    const fromState = state && typeof state === 'object' ? state : {};
    const clientConfig = getClientSettings();
    const candidates = [
      typeof fromState.key === 'string' ? fromState.key.trim() : '',
      clientConfig && typeof clientConfig.key === 'string' ? clientConfig.key.trim() : '',
      typeof fromState.public_key === 'string' ? fromState.public_key.trim() : '',
    ];
    for (let idx = 0; idx < candidates.length; idx += 1) {
      const value = candidates[idx];
      if (value) {
        return value;
      }
    }
    return '';
  }

  async function fetchCsvAndRender({ quiet = false } = {}) {
    console.info('[client-settings] csv fetch start', { quiet });

    refreshCsvDomElements();

    if (!dom.csvTable) {
      dom.csvTable = document.getElementById('csv-table');
    }
    if (!dom.csvEmpty) {
      dom.csvEmpty = document.getElementById('csv-empty');
    }

    if (!dom.csvTable || !dom.csvEmpty) {
      console.info('[client-settings] csv fetch fail', { quiet, reason: 'missing-elements' });
      return;
    }

    const thead = dom.csvTable.querySelector('thead');
    const tbody = dom.csvTable.querySelector('tbody');
    if (!thead || !tbody) {
      console.info('[client-settings] csv fetch fail', { quiet, reason: 'table-missing-thead-tbody' });
      return;
    }

    if (csvState.loading) {
      console.info('[client-settings] csv fetch skip', { quiet, reason: 'in-flight' });
      return;
    }

    const state = readStateFromDom() || {};
    const urls = state && typeof state === 'object' ? state.urls || {} : {};
    const fallbackTenant = state && Object.prototype.hasOwnProperty.call(state, 'tenant') ? state.tenant : '';
    const tenantSegment = fallbackTenant == null ? '' : String(fallbackTenant).trim();
    const basePath = (typeof urls.csv_get === 'string' && urls.csv_get)
      || (tenantSegment ? `/client/${tenantSegment}/catalog/csv` : '/client/catalog/csv');

    const locationInfo = getLocation();
    const origin = locationInfo.origin || locationInfo.href || 'https://localhost';

    let requestUrl;
    try {
      requestUrl = new URL(basePath, origin);
    } catch (error) {
      console.info('[client-settings] csv fetch fail', { quiet, reason: 'invalid-url', url: basePath });
      csvState.columns = [];
      csvState.rows = [];
      thead.innerHTML = '';
      tbody.innerHTML = '';
      ensureTableVisible(false);
      updateCsvControls();
      setCsvMessage('Не удалось загрузить CSV: некорректный адрес', 'alert');
      return;
    }

    const key = resolveCsvKey(state);
    if (key) {
      requestUrl.searchParams.set('k', key);
    }

    csvState.loading = true;
    try {
      const response = await fetch(requestUrl.toString(), {
        headers: {
          Accept: 'application/json',
        },
      });
      if (!response.ok) {
        let message = `status ${response.status}`;
        try {
          const text = await response.text();
          if (text && text.trim()) {
            message = text.trim();
          }
        } catch (_) {}
        throw new Error(message);
      }

      const payload = await response.json();
      const columns = normalizeColumns(payload && payload.columns);
      const rows = Array.isArray(payload && payload.rows) ? payload.rows : [];

      csvState.columns = columns;
      csvState.rows = rows;
      thead.innerHTML = '';
      tbody.innerHTML = '';
      renderCsvTable();
      ensureTableVisible(csvState.columns.length > 0);
      if (dom.csvSection) {
        showElement(dom.csvSection);
      }
      if (dom.csvContainer) {
        showElement(dom.csvContainer);
      }
      updateCsvControls();

      console.info('[client-settings] csv fetch ok', {
        quiet,
        rows: rows.length,
        columns: columns.length,
        url: requestUrl.toString(),
      });
      if (!quiet) {
        setCsvMessage(`CSV загружен (${rows.length} строк)`, 'muted');
      }
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      console.info('[client-settings] csv fetch fail', {
        quiet,
        message: err.message,
        url: requestUrl && requestUrl.toString(),
      });
      csvState.columns = [];
      csvState.rows = [];
      thead.innerHTML = '';
      tbody.innerHTML = '';
      ensureTableVisible(false);
      updateCsvControls();
      setCsvMessage(`Не удалось загрузить CSV: ${err.message}`, 'alert');
    } finally {
      csvState.loading = false;
    }
  }

  if (typeof window !== 'undefined') {
    window.fetchCsvAndRender = fetchCsvAndRender;
  }

  function collectCsvRows() {
    if (!dom.csvTable) return [];
    const tbody = dom.csvTable.querySelector('tbody');
    if (!tbody) return [];
    const rows = [];
    Array.from(tbody.querySelectorAll('tr')).forEach((tr) => {
      const row = [];
      Array.from(tr.querySelectorAll('td')).forEach((td) => {
        row.push(td.textContent?.trim() || '');
      });
      rows.push(row);
    });
    return rows;
  }

  function bindCsvControls() {
    refreshCsvDomElements();

    if (dom.csvAddRow) {
      dom.csvAddRow.addEventListener('click', () => {
        if (!csvState.columns.length) {
          setCsvMessage('CSV ещё не загружен — нажмите "Обновить данные" после загрузки каталога', 'alert');
          return;
        }
        const blank = csvState.columns.map(() => '');
        csvState.rows.push(blank);
        renderCsvTable();
        setCsvMessage('Добавлена новая строка', 'muted');
      });
    }

    if (dom.csvSave) {
      dom.csvSave.addEventListener('click', async () => {
        if (!csvState.columns.length) {
          setCsvMessage('Нет данных для сохранения', 'alert');
          return;
        }
        const rows = collectCsvRows();
        try {
          const stateForSave = readStateFromDom() || {};
          const urls = stateForSave && typeof stateForSave === 'object' ? stateForSave.urls || {} : {};
          const rawSavePath = typeof urls.csv_save === 'string' && urls.csv_save ? urls.csv_save : endpoints.csvSave;
          let targetUrl = resolveEndpointUrl(rawSavePath, withTenant());
          const keyForSave = resolveCsvKey(stateForSave);
          if (targetUrl && keyForSave) {
            try {
              const locationInfo = getLocation();
              const origin = locationInfo.origin || locationInfo.href || 'https://localhost';
              const urlObj = new URL(targetUrl, origin);
              urlObj.searchParams.set('k', keyForSave);
              const isAbsolute = ABSOLUTE_URL_RE.test(targetUrl);
              const isRootRelative = !isAbsolute && targetUrl.startsWith('/');
              if (isRootRelative) {
                targetUrl = `${urlObj.pathname}${urlObj.search}${urlObj.hash}`;
              } else {
                targetUrl = urlObj.toString();
              }
            } catch (appendError) {
              try {
                console.error('[client-settings] csv save url append key failed', appendError);
              } catch (_) {}
            }
          }
          const result = await postJSON(targetUrl, {
            columns: csvState.columns,
            rows,
          });
          setCsvMessage(`CSV сохранён (${result.rows || rows.length} строк)`, 'muted');
        } catch (error) {
          setCsvMessage(`Не удалось сохранить CSV: ${error.message}`, 'alert');
        }
      });
    }

    const refreshButton = dom.csvRefresh || document.getElementById('csv-refresh');
    if (refreshButton) {
      refreshButton.addEventListener('click', () => {
        fetchCsvAndRender();
      });
    } else {
      console.info('[client-settings] csv fetch fail', { reason: 'missing-refresh-button' });
    }

    updateCsvControls();
    if (!csvState.columns.length) {
      setTimeout(fetchCsvAndRender, 0);
    }
    ensureTableVisible(csvState.columns.length > 0);
  }

  function updateTelegramStatus(message, variant = 'muted') {
    if (!dom.tgStatus) return;
    dom.tgStatus.className = `status-text ${variant}`.trim();
    dom.tgStatus.textContent = message || '';
  }

  async function refreshTelegramStatus(options = {}) {
    const { fromPoll = false } = options || {};
    if (!dom.tgStatus || !telegram.statusUrl) return;
    if (!accessKey) {
      updateTelegramStatus('Нет ключа доступа', 'alert');
      hideTelegramQr();
      hideTwoFactorPrompt();
      stopTelegramPolling();
      setQrRefreshVisibility(false);
      setQrRefreshDisabled(true);
      return;
    }
    stopTelegramPolling();
    const url = buildTelegramTenantUrl(telegram.statusUrl, { t: Date.now() });
    if (!url) return;
    try {
      const response = await fetch(url, { cache: 'no-store' });
      let data = null;
      try {
        data = await response.clone().json();
      } catch (jsonError) {
        data = null;
      }
      let applied = null;
      if (data) {
        applied = applyTelegramStatus(data);
      }
      if (!data && !response.ok) {
        throw new Error(`status failed: ${response.status}`);
      }
      const status = (data && typeof data.status === 'string') ? data.status.trim() : '';
      const normalized = applied && typeof applied.status === 'string'
        ? applied.status
        : status.toLowerCase();
      const needsTwoFactor = Boolean(applied && applied.needsTwoFactor === true);
      const lastError = applied && typeof applied.lastError === 'string'
        ? applied.lastError
        : (data && typeof data.last_error === 'string' ? data.last_error : '');
      let message = 'Статус неизвестен';
      let variant = 'muted';
      if (needsTwoFactor) {
        message = 'Введите пароль двухфакторной аутентификации в Telegram.';
        variant = 'alert';
      } else if (normalized === 'authorized') {
        message = 'Подключено';
        variant = 'success';
      } else if (normalized === 'waiting_qr') {
        message = 'Сканируйте QR в Telegram → Settings → Devices.';
        variant = 'warning';
      } else if (normalized === 'disconnected' && lastError === 'qr_login_timeout') {
        message = 'QR-код истёк. Получите новый, чтобы продолжить.';
        variant = 'warning';
      } else if (normalized === 'disconnected' && lastError === 'twofa_timeout') {
        message = 'Время ожидания пароля истекло. Получите новый QR.';
        variant = 'warning';
      } else if (status) {
        message = `Статус: ${status}`;
        variant = 'alert';
      } else if (!response.ok) {
        message = 'Не удалось получить статус Telegram';
        variant = 'alert';
      }
      updateTelegramStatus(message, variant);
      telegramStatusErrorAttempts = 0;
      evaluateTelegramPolling(normalized);
    } catch (error) {
      console.error('[client-settings] telegram status error', error);
      updateTelegramStatus('Не удалось получить статус Telegram', 'alert');
      hideTelegramQr();
      setQrRefreshVisibility(false);
      setQrRefreshDisabled(true);
      scheduleTelegramErrorRetry();
    }
  }

  async function disconnectTelegram() {
    if (!telegram.logoutUrl || !accessKey) return;
    const url = buildTelegramTenantUrl(telegram.logoutUrl);
    if (!url) return;
    try {
      stopTelegramPolling();
      await fetch(url, { method: 'GET', cache: 'no-store' });
    } catch (error) {
      console.error('[client-settings] telegram logout error', error);
    } finally {
      hideTelegramQr();
      hideTwoFactorPrompt();
      setQrRefreshVisibility(false);
      refreshTelegramStatus();
    }
  }

  async function startTelegramSession(options = {}) {
    if (!dom.tgConnect || !telegram.startUrl) return;
    if (!accessKey) {
      updateTelegramStatus('Нет ключа доступа', 'alert');
      hideTelegramQr();
      hideTwoFactorPrompt();
      setQrRefreshVisibility(false);
      return;
    }
    const params = { t: Date.now() };
    if (options && options.force) {
      params.force = 1;
    }
    const url = buildTelegramTenantUrl(telegram.startUrl, params);
    if (!url) return;
    dom.tgConnect.disabled = true;
    if (options && options.force) {
      setQrRefreshDisabled(true);
    }
    stopTelegramPolling();
    hideTelegramQr('Готовим QR-код…', { keepContainer: true });
    hideTwoFactorPrompt();
    setQrRefreshVisibility(false);
    updateTelegramStatus('Запрашиваем QR-код…', 'muted');
    try {
      const response = await fetch(url, { method: 'GET', cache: 'no-store' });
      if (response.ok) {
        const data = await response.json().catch(() => null);
        if (data) {
          const applied = applyTelegramStatus(data);
          const status = applied && applied.status ? applied.status : (typeof data.status === 'string' ? data.status.toLowerCase() : '');
          evaluateTelegramPolling(status);
        }
      } else {
        throw new Error(`start failed: ${response.status}`);
      }
    } catch (error) {
      console.error('[client-settings] telegram start error', error);
      updateTelegramStatus('Не удалось запросить QR. Попробуйте позже.', 'alert');
      hideTelegramQr('QR недоступен. Попробуйте ещё раз.', { keepContainer: true });
    } finally {
      dom.tgConnect.disabled = false;
      if (options && options.force) {
        setQrRefreshDisabled(false);
      }
    }
    try {
      await refreshTelegramStatus({ fromPoll: true });
    } catch (error) {
      console.error('[client-settings] telegram start refresh failed', error);
    }
  }

  async function submitTelegramPassword(event) {
    if (event && typeof event.preventDefault === 'function') {
      event.preventDefault();
    }
    if (event && typeof event.stopPropagation === 'function') {
      event.stopPropagation();
    }
    if (!dom.tgPasswordInput) return;

    const rawValue = dom.tgPasswordInput.value || '';
    if (!rawValue.trim()) {
      updatePasswordStatus('Введите пароль 2FA', 'alert');
      dom.tgPasswordInput.focus();
      return;
    }

    if (!telegram.passwordUrl) {
      updatePasswordStatus('Сервис недоступен', 'alert');
      return;
    }

    if (!accessKey) {
      updatePasswordStatus('Нет ключа доступа', 'alert');
      hideTwoFactorPrompt();
      return;
    }

    const url = buildTelegramTenantUrl(telegram.passwordUrl, { t: Date.now() });
    if (!url) {
      updatePasswordStatus('Сервис недоступен', 'alert');
      return;
    }

    stopTelegramPolling();
    if (dom.tgPasswordSubmit) dom.tgPasswordSubmit.disabled = true;
    updatePasswordStatus('Отправляем пароль…', 'muted');
    let passwordAccepted = false;

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: rawValue }),
      });

      if (response.ok) {
        updatePasswordStatus('Пароль принят, завершаем вход…', 'muted');
        if (dom.tgPasswordInput) {
          dom.tgPasswordInput.value = '';
        }
        passwordAccepted = true;
      } else {
        let detail = '';
        try {
          const data = await response.clone().json();
          if (data && typeof data === 'object') {
            const { detail: detailValue, error: errorValue, message } = data;
            if (typeof detailValue === 'string') {
              detail = detailValue;
            } else if (typeof errorValue === 'string') {
              detail = errorValue;
            } else if (typeof message === 'string') {
              detail = message;
            }
          }
        } catch (_) {
          try {
            detail = (await response.text()) || '';
          } catch (__) {
            detail = '';
          }
        }
        const normalizedDetail = (detail || '').trim().toLowerCase();
        let message = (detail || `Ошибка подтверждения (HTTP ${response.status})`).trim() || 'Не удалось подтвердить пароль';
        if (
          (response.status === 400 || response.status === 401) &&
          (normalizedDetail === 'invalid_password' || normalizedDetail === 'invalid_2fa_password')
        ) {
          message = 'Неверный пароль';
        }
        updatePasswordStatus(message, 'alert');
        if (dom.tgPasswordInput) {
          try {
            dom.tgPasswordInput.focus();
          } catch (_) {}
        }
      }

      await refreshTelegramStatus({ fromPoll: true });
    } catch (error) {
      try {
        console.error('[client-settings] telegram password request failed', error?.message || error);
      } catch (_) {}
      updatePasswordStatus('Не удалось отправить пароль. Попробуйте ещё раз.', 'alert');
      scheduleTelegramPolling(6000, { resetErrors: true });
    } finally {
      if (dom.tgPasswordSubmit) dom.tgPasswordSubmit.disabled = false;
      if (passwordAccepted && dom.tgPasswordInput) {
        dom.tgPasswordInput.value = '';
      }
    }
  }

  if (initialQrId) {
    updateTelegramQrLink(initialQrId);
  }

  if (dom.tgConnect) {
    dom.tgConnect.addEventListener('click', (event) => {
      if (event && typeof event.preventDefault === 'function') {
        event.preventDefault();
      }
      startTelegramSession();
    });
  }

  if (dom.tgQrImage) {
    dom.tgQrImage.addEventListener('error', () => {
      handleTelegramQrError();
    });
    dom.tgQrImage.addEventListener('load', () => {
      qrImageReloadPending = false;
      if (dom.tgQrFallback) {
        dom.tgQrFallback.textContent = '';
        hideElement(dom.tgQrFallback);
      }
    });
  }
  if (dom.tgQrRefresh) {
    dom.tgQrRefresh.addEventListener('click', (event) => {
      if (event && typeof event.preventDefault === 'function') {
        event.preventDefault();
      }
      startTelegramSession({ force: true });
    });
  }
  if (dom.tgIntegrationCard && dom.tgIntegrationCard.dataset) {
    const initialStatusRaw = dom.tgIntegrationCard.dataset.tgInitialStatus || '';
    const initialStatus = initialStatusRaw.toLowerCase();
    const initialError = dom.tgIntegrationCard.dataset.tgInitialError || '';
    const initialTwofa = dom.tgIntegrationCard.dataset.tgInitialTwofa === '1';
    if (initialTwofa) {
      showTwoFactorPrompt('Введите пароль двухфакторной аутентификации в Telegram.');
    }
    if (initialStatus === 'waiting_qr' && initialQrId && !initialTwofa) {
      showTelegramQr(initialQrId);
      setQrRefreshDisabled(false);
    }
    if (
      initialStatus === 'disconnected'
      && (initialError === 'qr_login_timeout' || initialError === 'twofa_timeout')
    ) {
      setQrRefreshVisibility(true, 'Обновить QR');
      setQrRefreshDisabled(false);
    }
  }
  if (dom.tgRefresh) {
    dom.tgRefresh.addEventListener('click', () => refreshTelegramStatus());
  }
  if (dom.tgDisconnect) {
    dom.tgDisconnect.addEventListener('click', (event) => {
      if (event && typeof event.preventDefault === 'function') {
        event.preventDefault();
      }
      disconnectTelegram();
    });
  }
  if (dom.tgPasswordForm) {
    dom.tgPasswordForm.addEventListener('submit', (event) => submitTelegramPassword(event));
  }

  function safeInvoke(label, fn) {
    if (typeof fn !== 'function') return;
    try {
      fn();
    } catch (error) {
      try {
        console.error(`[client-settings] ${label} failed`, error);
      } catch (_) {}
    }
  }

  function bootstrapClientSettings() {
    safeInvoke('export-init', bindExportClicks);
    safeInvoke('catalog-init', bindCatalogUpload);
    safeInvoke('training-init', bindTrainingUpload);
    safeInvoke('csv-controls', bindCsvControls);
    setTimeout(fetchCsvAndRender, 0);
    try {
      const trainingPromise = refreshTrainingStatus();
      if (trainingPromise && typeof trainingPromise.catch === 'function') {
        trainingPromise.catch((error) => {
          try {
            console.error('[client-settings] training status init failed', error);
          } catch (_) {}
        });
      }
    } catch (error) {
      try {
        console.error('[client-settings] training status init failed', error);
      } catch (_) {}
    }
    try {
      const telegramPromise = refreshTelegramStatus();
      if (telegramPromise && typeof telegramPromise.catch === 'function') {
        telegramPromise.catch((error) => {
          try {
            console.error('[client-settings] telegram status init failed', error);
          } catch (_) {}
        });
      }
    } catch (error) {
      try {
        console.error('[client-settings] telegram status init failed', error);
      } catch (_) {}
    }
    window.__cs_loaded = true;
  }

  function init() {
    if (window.__cs_loaded === true) {
      return;
    }
    window.__EXPORT_ERROR__ = undefined;
    try {
      ensureBootstrapGlobals();
      bootstrapClientSettings();
    } catch (error) {
      window.__cs_loaded = false;
      window.__EXPORT_ERROR__ = error;
      try {
        console.error('client-settings init error', error);
      } catch (_) {}
    } finally {
      window.__EXPORT_LOADED__ = true;
    }
  }

  window.__client_settings_boot = init;

  if (typeof document !== 'undefined' && document.readyState !== 'loading') {
    init();
  } else if (typeof document !== 'undefined') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  }
})();
