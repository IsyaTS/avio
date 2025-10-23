window.__cs_loaded = false;
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

try {
  (function initClientSettings() {
  console.info('client-settings loaded');
  window.__CATALOG_WIDGET_VERSION__ = '2025-03-20T15:30:00Z';
  window.__client_settings_build = '20250320';

  const SETTINGS_FETCH_MAX_ATTEMPTS = 5;
  const SETTINGS_FETCH_BACKOFF_BASE_MS = 600;

  window.__EXPORT_ERROR__ = undefined;

  const STATE_NODE_ID = 'client-settings-state';
  const TENANT_PATH_REGEX = /\/client\/(\d+)(?:\/|$)/;

  const readStateFromDom = () => {
    const node = document.getElementById(STATE_NODE_ID);
    if (!node) {
      return {};
    }
    const raw = (node.textContent || '').trim();
    if (!raw) {
      return {};
    }
    try {
      return JSON.parse(raw);
    } catch (error) {
      try {
        console.error('[client-settings] failed to parse state JSON', error);
      } catch (_) {}
      return {};
    }
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

  const determineTenant = (state, { fallbackDefault = true } = {}) => {
    let tenant = Number.parseInt(state && state.tenant, 10);
    if (!Number.isFinite(tenant) || tenant <= 0) {
      const path = getLocation().pathname || '/';
      const match = path.match(TENANT_PATH_REGEX);
      if (match && match[1]) {
        const parsed = Number.parseInt(match[1], 10);
        tenant = Number.isFinite(parsed) ? parsed : NaN;
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
    const tenant = determineTenant(state, { fallbackDefault: true });
    const stateAccessKey = typeof state.key === 'string' ? state.key.trim() : '';
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

      if (includeKey && stateAccessKey) {
        url.searchParams.set('k', stateAccessKey);
      }
      return url.toString();
    };

    const buildUrl = (typeof window !== 'undefined' && typeof window.buildUrl === 'function')
      ? window.buildUrl
      : fallbackBuildUrl;

    const builderAccessKey = typeof buildUrl.getKey === 'function' ? (buildUrl.getKey() || '') : '';
    const accessKey = (stateAccessKey || builderAccessKey || '').trim();
    const urls = state && typeof state === 'object' ? state.urls || {} : {};
    const initialQrId = typeof state.qr_id === 'string' ? state.qr_id.trim() : '';
    const resolvedMaxDays = resolveMaxDays(state);
    const maxDays = resolvedMaxDays != null ? resolvedMaxDays : 30;

    const tenantString = Number.isFinite(tenant) && tenant > 0 ? String(tenant) : '';
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
    uploadForm: document.getElementById('upload-form'),
    uploadInput: document.querySelector('#upload-form input[name="file"]'),
    uploadMessage: document.getElementById('upload-message'),
    uploadSubmit: document.querySelector('#upload-form [data-role="upload-submit"], #upload-submit'),
    progress: document.getElementById('upload-progress'),
    progressBar: document.getElementById('upload-progress-bar'),
    csvTable: document.getElementById('csv-table'),
    csvEmpty: document.getElementById('csv-empty'),
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

  function resetProgress() {
    if (dom.progress) dom.progress.hidden = true;
    if (dom.progressBar) dom.progressBar.style.width = '0%';
  }

function performCatalogUpload(event) {
  if (event) event.preventDefault();
  if (!dom.uploadForm) return;
  if (dom.uploadForm.dataset.state === 'uploading') return;

  const file = dom.uploadInput && dom.uploadInput.files && dom.uploadInput.files[0];
  if (!file) {
    setStatus(dom.uploadMessage, 'Выберите файл перед загрузкой', 'alert');
    return;
  }

  const formData = new FormData();
  formData.append('file', file);

  const targetUrlRaw = (dom.uploadForm.dataset.uploadUrl || '').trim();
  const targetUrl = resolveEndpointUrl(targetUrlRaw || endpoints.uploadCatalog, withTenant(), endpoints.uploadCatalog);
  if (!targetUrl) {
    setStatus(dom.uploadMessage, 'Не найдён адрес загрузки каталога', 'alert');
    return;
  }

  setStatus(dom.uploadMessage, `Загрузка ${file.name}...`, 'muted');
  if (dom.progress) dom.progress.hidden = false;
  if (dom.progressBar) dom.progressBar.style.width = '0%';
  dom.uploadForm.dataset.state = 'uploading';

  const xhr = new XMLHttpRequest();
  xhr.open('POST', targetUrl);
  xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');

  if (dom.uploadSubmit) dom.uploadSubmit.disabled = true;

  xhr.upload.onprogress = (progressEvent) => {
    if (!dom.progressBar || !progressEvent.lengthComputable) return;
    const percent = Math.round((progressEvent.loaded / progressEvent.total) * 100);
    dom.progressBar.style.width = `${percent}%`;
  };

  xhr.onerror = () => {
    resetProgress();
    delete dom.uploadForm.dataset.state;
    if (dom.uploadSubmit) dom.uploadSubmit.disabled = false;
    setStatus(dom.uploadMessage, 'Ошибка сети при загрузке файла', 'alert');
  };

  xhr.onload = async () => {
    resetProgress();
    delete dom.uploadForm.dataset.state;
    if (dom.uploadSubmit) dom.uploadSubmit.disabled = false;

    try {
      if (xhr.status >= 300 && xhr.status < 400) {
        setStatus(dom.uploadMessage, 'Каталог принят, обновляем данные…', 'muted');
        await loadCsv({ quiet: true });
        setStatus(dom.uploadMessage, 'Каталог обновлён', 'muted');
        if (dom.uploadInput) dom.uploadInput.value = '';
        return;
      }

      if (xhr.status < 200 || xhr.status >= 300) {
        const text = xhr.responseText || '';
        throw new Error(text || `Ошибка загрузки (HTTP ${xhr.status})`);
      }

      const data = JSON.parse(xhr.responseText || '{}');
      if (!data.ok) {
        throw new Error(data.error || 'Не удалось загрузить файл');
      }
      setStatus(dom.uploadMessage, `Файл ${data.filename || file.name} загружен`, 'muted');
      if (dom.uploadInput) dom.uploadInput.value = '';
      await loadCsv({ quiet: true });
    } catch (error) {
      setStatus(dom.uploadMessage, `Ошибка загрузки: ${error.message}`, 'alert');
    }
  };

  try {
    xhr.send(formData);
  } catch (error) {
    resetProgress();
    delete dom.uploadForm.dataset.state;
    if (dom.uploadSubmit) dom.uploadSubmit.disabled = false;
    setStatus(dom.uploadMessage, `Ошибка загрузки: ${error.message}`, 'alert');
  }
}

  function bindUploadWidget() {
    if (!dom.uploadForm) return;
    dom.uploadForm.addEventListener('submit', (event) => {
      event.preventDefault();
      performCatalogUpload(event);
    });
    if (dom.uploadSubmit) {
      dom.uploadSubmit.addEventListener('click', (event) => {
        event.preventDefault();
        performCatalogUpload(event);
      });
    }
    if (dom.uploadInput) {
      dom.uploadInput.addEventListener('change', (event) => {
        event.preventDefault();
        event.stopPropagation();
        const file = dom.uploadInput.files && dom.uploadInput.files[0];
        if (file) {
          setStatus(dom.uploadMessage, `Выбран файл ${file.name}`, 'muted');
        }
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
  };

  function setCsvMessage(message, variant = 'muted') {
    setStatus(dom.csvMessage, message, variant);
  }

  function ensureTableVisible(show) {
    if (!dom.csvTable || !dom.csvEmpty) return;
    if (show) {
      dom.csvTable.style.display = '';
      dom.csvEmpty.style.display = 'none';
    } else {
      dom.csvTable.style.display = 'none';
      dom.csvEmpty.style.display = '';
    }
  }

  function renderCsvTable() {
    if (!dom.csvTable) return;
    const thead = dom.csvTable.querySelector('thead');
    const tbody = dom.csvTable.querySelector('tbody');
    if (!thead || !tbody) return;

    if (!csvState.columns.length) {
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
  }

  async function loadCsv({ quiet = false } = {}) {
    try {
      const url = resolveEndpointUrl(endpoints.csvGet, withTenant());
      if (!url) {
        throw new Error('Сервис недоступен');
      }
      const response = await fetch(url, { cache: 'no-store' });
      if (response.status === 404) {
        csvState.columns = [];
        csvState.rows = [];
        ensureTableVisible(false);
        if (!quiet) setCsvMessage('CSV ещё не готов — загрузите PDF или CSV файл', 'muted');
        return;
      }
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      // Normalize headers to avoid visual duplicates in the editor
      const normalizeColumns = (cols) => {
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
      };
      csvState.columns = normalizeColumns(data.columns);
      csvState.rows = Array.isArray(data.rows) ? data.rows : [];
      renderCsvTable();
      setCsvMessage(`Загружено ${csvState.rows.length} строк`, 'muted');
    } catch (error) {
      setCsvMessage(`Не удалось загрузить CSV: ${error.message}`, 'alert');
    }
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

  if (dom.csvAddRow) {
    dom.csvAddRow.addEventListener('click', () => {
      if (!csvState.columns.length) {
        setCsvMessage('CSV ещё не загружен — нажмите \"Обновить данные\" после загрузки каталога', 'alert');
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
        const targetUrl = resolveEndpointUrl(endpoints.csvSave, withTenant());
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

  if (dom.csvRefresh) {
    dom.csvRefresh.addEventListener('click', () => loadCsv({ quiet: true }));
  }

  // Автоподгрузка CSV при доступности
  loadCsv({ quiet: true });

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

  function bootstrapClientSettings() {
    bindExportClicks();
    bindUploadWidget();
    bindTrainingUpload();
    loadCsv({ quiet: true });
    refreshTrainingStatus();
    refreshTelegramStatus();
    window.__cs_loaded = true;
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bootstrapClientSettings, { once: true });
  } else {
    bootstrapClientSettings();
  }
})();
} catch (error) {
  window.__EXPORT_ERROR__ = error;
  try {
    console.error('[client-settings] init failed', error);
  } catch (_) {
    /* noop */
  }
} finally {
  window.__EXPORT_LOADED__ = true;
}
