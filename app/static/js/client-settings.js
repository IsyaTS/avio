console.log('[client-settings] script loaded');

(function () {
  const stateScript = document.getElementById('client-settings-state');
  const globalState = typeof window !== 'undefined' ? window.state : undefined;
  if (!stateScript && (!globalState || typeof globalState !== 'object')) return;

  let state = {};
  if (globalState && typeof globalState === 'object') {
    state = globalState;
  } else if (stateScript) {
    const scriptType = (stateScript.getAttribute('type') || '').toLowerCase();
    const raw = stateScript.textContent || '';
    const parseJson = (text) => {
      try {
        return JSON.parse(text);
      } catch (error) {
        console.error('Failed to parse client settings state', error);
        return {};
      }
    };

    if (scriptType === 'application/json') {
      state = parseJson(raw || '{}');
    } else {
      const start = raw.indexOf('{');
      const end = raw.lastIndexOf('}');
      if (start !== -1 && end !== -1 && end >= start) {
        const jsonCandidate = raw.slice(start, end + 1);
        state = parseJson(jsonCandidate || '{}');
      }
    }
  }

  if (!state || typeof state !== 'object') {
    state = {};
  }

  const tenant = Number.parseInt(state.tenant, 10) || 1;
  const accessKey = state.key || '';
  const urls = state.urls || {};
  const maxDays = Number.isFinite(state.max_days) && state.max_days > 0 ? Number(state.max_days) : 30;

  function buildUrl(path, options = {}) {
    const { includeKey = true } = options || {};
    const { origin, protocol, host, hostname, port } = window.location;
    let url;
    try {
      url = new URL(path, origin);
    } catch (error) {
      console.error('Failed to resolve URL', path, error);
      url = new URL(origin);
    }

    // Force same-origin for app routes: some servers generate absolute URLs like
    // http://app:8000/... that are unreachable from the browser. Preserve
    // pathname/search/hash but pin to the current page origin.
    if (url.hostname !== hostname) {
      url = new URL(url.pathname + url.search + url.hash, origin);
    }

    // Normalize protocol/port if anything still differs on same host
    if (url.hostname === hostname && url.protocol !== protocol) {
      url.protocol = protocol;
      url.port = port;
    } else if (url.host === host && url.protocol !== protocol) {
      url.protocol = protocol;
    }

    if (includeKey && accessKey) {
      url.searchParams.set('k', accessKey);
    }
    return url.toString();
  }

  const endpoints = {
    saveSettings: urls.save_settings || `/client/${tenant}/settings/save`,
    savePersona: urls.save_persona || `/client/${tenant}/persona`,
    uploadCatalog: urls.upload_catalog || `/client/${tenant}/catalog/upload`,
    csvGet: urls.csv_get || `/client/${tenant}/catalog/csv`,
    csvSave: urls.csv_save || `/client/${tenant}/catalog/csv`,
    trainingUpload: urls.training_upload || `/client/${tenant}/training/upload`,
    trainingStatus: urls.training_status || `/client/${tenant}/training/status`,
    whatsappExport: urls.whatsapp_export || `/export/whatsapp`,
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
    uploadMessage: document.getElementById('upload-message'),
    progress: document.getElementById('upload-progress'),
    progressBar: document.getElementById('upload-progress-bar'),
    csvTable: document.getElementById('csv-table'),
    csvEmpty: document.getElementById('csv-empty'),
    csvMessage: document.getElementById('csv-message'),
    csvAddRow: document.getElementById('csv-add-row'),
    csvSave: document.getElementById('csv-save'),
    csvRefresh: document.getElementById('csv-refresh'),
    trainingUploadForm: document.getElementById('training-upload-form'),
    trainingUploadMessage: document.getElementById('training-upload-message'),
    trainingCheckStatus: document.getElementById('training-check-status'),
    trainingStatus: document.getElementById('training-status'),
    expDays: document.getElementById('exp-days'),
    expLimit: document.getElementById('exp-limit'),
    expPer: document.getElementById('exp-per'),
    exportDownload: document.getElementById('export-download'),
    exportStatus: document.getElementById('export-status'),
  };

  function setStatus(element, message, variant = 'muted') {
    if (!element) return;
    element.className = `status-text ${variant}`.trim();
    element.textContent = message || '';
  }

  async function postJSON(endpoint, payload) {
    const response = await fetch(buildUrl(endpoint), {
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

  function downloadBlob(blob, filename) {
    const anchor = document.createElement('a');
    anchor.style.display = 'none';
    anchor.download = filename;
    anchor.href = URL.createObjectURL(blob);
    document.body.appendChild(anchor);
    anchor.click();
    setTimeout(() => {
      URL.revokeObjectURL(anchor.href);
      document.body.removeChild(anchor);
    }, 250);
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

  function parseHeaderCount(headers, name) {
    const raw = headers.get(name);
    if (!raw) return null;
    const parsed = Number.parseInt(raw, 10);
    return Number.isFinite(parsed) ? parsed : null;
  }

  async function requestWhatsappExport({ days, limit, exportState }) {
    const effectiveState = exportState && typeof exportState === 'object' ? exportState : state;
    const stateTenant = Number.parseInt(effectiveState.tenant, 10);
    const requestTenant = Number.isFinite(stateTenant) ? stateTenant : tenant;
    const stateKey = typeof effectiveState.key === 'string' ? effectiveState.key : accessKey;
    const targetUrl = effectiveState?.urls?.whatsapp_export || endpoints.whatsappExport;

    const normalizedDays = Number.isFinite(days) && days >= 0 ? Math.min(days, maxDays) : 0;
    const normalizedLimit = Number.isFinite(limit) && limit > 0 ? limit : 10000;

    const payload = {
      tenant: requestTenant,
      key: stateKey,
      days: normalizedDays,
      limit: normalizedLimit,
      per: 0,
    };

    const response = await fetch(buildUrl(targetUrl, { includeKey: false }), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (response.status === 204) {
      return { empty: true };
    }

    if (!response.ok) {
      let detail = '';
      let reason = '';
      let fallbackMessage = '';
      try {
        const data = await response.clone().json();
        if (data && typeof data === 'object') {
          const detailValue = data.detail;
          if (typeof detailValue === 'string') {
            detail = detailValue;
          } else if (Array.isArray(detailValue)) {
            detail = detailValue.map((item) => (item == null ? '' : String(item))).filter(Boolean).join(', ');
          } else if (detailValue && typeof detailValue === 'object') {
            const detailParts = [];
            Object.entries(detailValue).forEach(([key, value]) => {
              if (value == null) return;
              const text = Array.isArray(value) ? value.join(', ') : String(value);
              detailParts.push(`${key}: ${text}`);
            });
            detail = detailParts.join('; ');
          }

          if (typeof data.reason === 'string') {
            reason = data.reason;
          }

          if (!detail && !reason && typeof data.message === 'string') {
            fallbackMessage = data.message;
          }
        }
      } catch (error) {
        try {
          const rawText = await response.text();
          fallbackMessage = (rawText || '').trim();
        } catch (readError) {
          try { console.debug('Failed to read export error text', readError); } catch (_) {}
        }
      }

      const message = (reason || detail || fallbackMessage || `HTTP ${response.status}`).trim() || 'Ошибка экспорта';
      const error = new Error(message);
      if (detail) error.detail = detail;
      if (reason) error.reason = reason;
      if (!detail && fallbackMessage) error.detail = fallbackMessage;
      error.status = response.status;
      throw error;
    }

    const contentTypeHeader = response.headers.get('content-type') || response.headers.get('Content-Type') || '';
    const normalizedContentType = contentTypeHeader.toLowerCase();
    if (!normalizedContentType.startsWith('application/zip')) {
      let detail = '';
      try {
        if (normalizedContentType.includes('application/json')) {
          const data = await response.clone().json();
          if (data && typeof data === 'object') {
            const detailValue = data.detail;
            if (typeof detailValue === 'string') {
              detail = detailValue;
            } else if (Array.isArray(detailValue)) {
              detail = detailValue.map((item) => (item == null ? '' : String(item))).filter(Boolean).join(', ');
            } else if (detailValue && typeof detailValue === 'object') {
              const detailParts = [];
              Object.entries(detailValue).forEach(([key, value]) => {
                if (value == null) return;
                const text = Array.isArray(value) ? value.join(', ') : String(value);
                detailParts.push(`${key}: ${text}`);
              });
              detail = detailParts.join('; ');
            }

            if (!detail && typeof data.message === 'string') {
              detail = data.message;
            }
          }
        }
      } catch (error) {
        try {
          const rawText = await response.text();
          detail = (rawText || '').trim();
        } catch (readError) {
          try { console.debug('Failed to read non-zip response body', readError); } catch (_) {}
        }
      }

      const error = new Error((detail || 'Ответ сервера не является ZIP-архивом').trim());
      error.status = response.status;
      if (detail) error.detail = detail;
      throw error;
    }

    const blob = await response.blob();
    const header = response.headers.get('content-disposition') || response.headers.get('Content-Disposition');
    let filename = '';
    if (header && header.includes('filename=')) {
      const match = header.match(/filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i);
      if (match) {
        const encoded = (match[1] || match[2] || '').trim();
        if (encoded) {
          try {
            filename = decodeURIComponent(encoded);
          } catch (error) {
            try { console.warn('Failed to decode filename', encoded, error); } catch (_) {}
            filename = encoded;
          }
        }
      }
    }

    if (!filename) {
      const today = new Date();
      const y = today.getUTCFullYear();
      const m = String(today.getUTCMonth() + 1).padStart(2, '0');
      const d = String(today.getUTCDate()).padStart(2, '0');
      filename = `whatsapp_export_${y}-${m}-${d}.zip`;
    }

    return {
      blob,
      filename,
      dialogCount: parseHeaderCount(response.headers, 'X-Dialog-Count'),
      messageCount: parseHeaderCount(response.headers, 'X-Message-Count'),
    };
  }

  if (dom.saveSettings && dom.settingsForm) {
    dom.saveSettings.addEventListener('click', async () => {
      const formData = new FormData(dom.settingsForm);
      const payload = Object.fromEntries(formData.entries());
      try {
        await postJSON(endpoints.saveSettings, payload);
        setStatus(dom.settingsMessage, 'Паспорт сохранён', 'muted');
      } catch (error) {
        setStatus(dom.settingsMessage, `Не удалось сохранить: ${error.message}`, 'alert');
      }
    });
  }

  if (dom.savePersona && dom.personaTextarea) {
    dom.savePersona.addEventListener('click', async () => {
      try {
        await postJSON(endpoints.savePersona, { text: dom.personaTextarea.value });
        setStatus(dom.personaMessage, 'Персона обновлена', 'muted');
      } catch (error) {
        setStatus(dom.personaMessage, `Не удалось обновить: ${error.message}`, 'alert');
      }
    });
  }

  if (dom.downloadConfig) {
    dom.downloadConfig.addEventListener('click', async () => {
      try {
        const response = await fetch(buildUrl(`/pub/settings/get?tenant=${tenant}`));
        if (!response.ok) throw new Error(await response.text());
        const data = await response.json();
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

  if (dom.uploadForm) {
    dom.uploadForm.addEventListener('submit', (event) => {
      event.preventDefault();
      const formData = new FormData(dom.uploadForm);
      const xhr = new XMLHttpRequest();
      xhr.open('POST', buildUrl(endpoints.uploadCatalog));

      if (dom.progress) dom.progress.hidden = false;
      if (dom.progressBar) dom.progressBar.style.width = '0%';

      xhr.upload.onprogress = (progressEvent) => {
        if (!dom.progressBar || !progressEvent.lengthComputable) return;
        const percent = Math.round((progressEvent.loaded / progressEvent.total) * 100);
        dom.progressBar.style.width = `${percent}%`;
      };

      xhr.onerror = () => {
        resetProgress();
        setStatus(dom.uploadMessage, 'Ошибка сети при загрузке файла', 'alert');
      };

      xhr.onload = () => {
        resetProgress();
        try {
          if (xhr.status >= 200 && xhr.status < 300) {
            const data = JSON.parse(xhr.responseText || '{}');
            if (!data.ok) {
              throw new Error(data.error || 'Не удалось загрузить файл');
            }
            setStatus(dom.uploadMessage, `Файл ${data.filename} загружен`, 'muted');
            dom.uploadForm.reset();
            if (data.csv_path) {
              loadCsv();
            }
          } else {
            throw new Error(xhr.responseText || 'Ошибка загрузки');
          }
        } catch (error) {
          setStatus(dom.uploadMessage, `Ошибка загрузки: ${error.message}`, 'alert');
        }
      };

      xhr.send(formData);
    });
  }

  // -------- Обучение: загрузка диалогов --------
  async function refreshTrainingStatus() {
    if (!dom.trainingStatus) return;
    try {
      const response = await fetch(buildUrl(endpoints.trainingStatus));
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
    dom.trainingUploadForm.addEventListener('submit', (event) => {
      event.preventDefault();
      const formData = new FormData(dom.trainingUploadForm);
      const xhr = new XMLHttpRequest();
      xhr.open('POST', buildUrl(endpoints.trainingUpload));
      xhr.onerror = () => {
        setStatus(dom.trainingUploadMessage, 'Ошибка сети при загрузке', 'alert');
      };
      xhr.onload = () => {
        try {
          if (xhr.status >= 200 && xhr.status < 300) {
            const data = JSON.parse(xhr.responseText || '{}');
            if (!data.ok) throw new Error(data.error || 'Не удалось загрузить');
            setStatus(dom.trainingUploadMessage, `Загружено примеров: ${data.pairs || ''}`, 'muted');
            dom.trainingUploadForm.reset();
            refreshTrainingStatus();
          } else {
            throw new Error(xhr.responseText || 'Ошибка загрузки');
          }
        } catch (error) {
          setStatus(dom.trainingUploadMessage, `Ошибка загрузки: ${error.message}`, 'alert');
        }
      };
      xhr.send(formData);
    });
  }

  if (dom.trainingCheckStatus) {
    dom.trainingCheckStatus.addEventListener('click', refreshTrainingStatus);
  }

  bindTrainingUpload();
  refreshTrainingStatus();

  function bindExportClicks(initialState) {
    if (!dom.exportDownload) return;

    let pending = false;
    dom.exportDownload.addEventListener('click', async (event) => {
      if (event && typeof event.preventDefault === 'function') {
        event.preventDefault();
      }
      if (event && typeof event.stopPropagation === 'function') {
        event.stopPropagation();
      }
      if (pending) return;
      pending = true;
      dom.exportDownload.disabled = true;

      const days = normalizeDays(dom.expDays ? dom.expDays.value : '0', maxDays);
      if (dom.expDays) {
        dom.expDays.value = String(days);
      }
      const limit = normalizeLimit(dom.expLimit ? dom.expLimit.value : '10000');

      if (dom.expPer) {
        dom.expPer.value = '0';
      }

      setStatus(dom.exportStatus, 'Готовим архив…', 'muted');

      try {
        const exportState = initialState && typeof initialState === 'object' ? initialState : state;
        const result = await requestWhatsappExport({ days, limit, exportState });
        if (result && result.empty) {
          setStatus(dom.exportStatus, 'Нет диалогов', 'alert');
          return;
        }

        downloadBlob(result.blob, result.filename);

        if (result.dialogCount != null || result.messageCount != null) {
          const dialogs = result.dialogCount != null ? result.dialogCount : 0;
          const messages = result.messageCount != null ? result.messageCount : 0;
          setStatus(dom.exportStatus, `Сформировано: ${dialogs} диалогов, ${messages} сообщений`, 'muted');
        } else {
          setStatus(dom.exportStatus, 'Архив сформирован', 'muted');
        }
      } catch (error) {
        try { console.error('WhatsApp export failed', error); } catch (_) {}
        const detail = error && (error.detail || error.reason);
        const message = detail || (error && error.message) || 'Ошибка экспорта';
        setStatus(dom.exportStatus, message, 'alert');
      } finally {
        pending = false;
        dom.exportDownload.disabled = false;
      }
    });
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
      const response = await fetch(buildUrl(endpoints.csvGet));
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
        const result = await postJSON(endpoints.csvSave, {
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

  bindExportClicks(state);
})();
