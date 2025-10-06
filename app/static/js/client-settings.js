(function () {
  const stateScript = document.getElementById('client-settings-state');
  if (!stateScript) return;

  let state = {};
  try {
    state = JSON.parse(stateScript.textContent || '{}');
  } catch (error) {
    console.error('Failed to parse client settings state', error);
    state = {};
  }

  const tenant = Number.parseInt(state.tenant, 10) || 1;
  const accessKey = state.key || '';
  const urls = state.urls || {};

  function buildUrl(path) {
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

    if (accessKey) {
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
    trainingExport: urls.training_export || `/client/${tenant}/training/export`,
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
    expMin: document.getElementById('exp-min'),
    expAnon: document.getElementById('exp-anon'),
    expStrict: document.getElementById('exp-strict'),
    expProvider: document.getElementById('exp-provider'),
    expFormat: document.getElementById('exp-format'),
    expBundle: document.getElementById('exp-bundle'),
    expBtnGo: document.getElementById('export-dialogs-go'),
    // Inline export panel wrapper (optional)
    exportInline: document.getElementById('export-inline'),
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

  // -------- Экспорт: скачивание через fetch() --------
  function bindExportClicks() {
    if (!dom.expBtnGo) return;
    let pending = false;
    dom.expBtnGo.addEventListener('click', async (e) => {
      try { e.preventDefault(); } catch (_) {}
      if (pending) return; // debounce while pending
      pending = true;
      dom.expBtnGo.disabled = true;
      const base = buildUrl(endpoints.trainingExport);
      const qs = new URL(base);
      const days = Number.parseInt(dom.expDays?.value || '30', 10) || 0;
      const parsedLimit = Number.parseInt(dom.expLimit?.value || '10000', 10);
      const limit = Number.isFinite(parsedLimit) && parsedLimit > 0 ? parsedLimit : 10000;
      const min = Number.parseInt(dom.expMin?.value || '0', 10) || 0;
      const anon = String((dom.expAnon?.value || '1')).trim();
      const strict = String((dom.expStrict?.value || '0')).trim();
      const provider = (dom.expProvider?.value || '').trim();
      const fmt = (dom.expFormat?.value || 'jsonl').trim();
      const bundle = (dom.expBundle?.value || 'single').trim();
      qs.searchParams.set('format', fmt);
      qs.searchParams.set('days', String(days));
      qs.searchParams.set('limit', String(limit));
      qs.searchParams.set('per', '0');
      qs.searchParams.set('min_turns', String(min));
      qs.searchParams.set('anonymize', anon);
      qs.searchParams.set('strict', strict);
      qs.searchParams.set('bundle', bundle);
      if (provider) qs.searchParams.set('provider', provider);
      const href = qs.toString();
      try { console.debug('[training] export click', href); } catch (_) {}
      setStatus(dom.trainingStatus, 'Готовлю файл…', 'muted');
      try {
        const resp = await fetch(href);
        try { console.debug('[training] export response', resp.status, resp.headers.get('X-Debug-Stage') || ''); } catch (_) {}
        const stage = resp.headers.get('X-Debug-Stage') || '';
        if (stage) setStatus(dom.trainingStatus, stage, 'muted');
        if (resp.status === 204) {
          setStatus(dom.trainingStatus, 'Нет диалогов под фильтры', 'alert');
          return;
        }
        if (!resp.ok) {
          const txt = await resp.text().catch(() => '');
          setStatus(dom.trainingStatus, txt || 'Ошибка экспорта, см. логи сервера', 'alert');
          return;
        }
        const blob = await resp.blob();
        let filename = '';
        const cd = resp.headers.get('content-disposition') || resp.headers.get('Content-Disposition');
        if (cd && cd.includes('filename=')) {
          const match = cd.match(/filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i);
          filename = decodeURIComponent((match && (match[1] || match[2])) || '').trim();
        }
        if (!filename) {
          const today = new Date();
          const y = today.getUTCFullYear();
          const m = String(today.getUTCMonth() + 1).padStart(2, '0');
          const d = String(today.getUTCDate()).padStart(2, '0');
          const defExt = fmt === 'json' ? 'json' : (fmt === 'zip' ? 'zip' : 'jsonl');
          filename = `training_${tenant}_${y}${m}${d}.${defExt}`;
        }
        const a = document.createElement('a');
        a.style.display = 'none';
        a.download = filename;
        a.href = URL.createObjectURL(blob);
        document.body.appendChild(a);
        a.click();
        setTimeout(() => {
          URL.revokeObjectURL(a.href);
          document.body.removeChild(a);
        }, 250);
        setStatus(dom.trainingStatus, `Файл ${filename} загружен`, 'muted');
      } catch (error) {
        try { console.debug('[training] export failed', error); } catch (_) {}
        setStatus(dom.trainingStatus, `Ошибка экспорта: ${error.message}`, 'alert');
      } finally {
        pending = false;
        dom.expBtnGo.disabled = false;
      }
    });
  }
}

  // Bind export buttons
  bindExportClicks();

  // Also support split export buttons used in settings.html
  if (dom.expBtnJsonl) {
    dom.expBtnJsonl.addEventListener('click', async (e) => {
      try { e.preventDefault(); } catch (_) {}
      dom.expBtnJsonl.disabled = true;
      try {
        const base = buildUrl(endpoints.trainingExport);
        const qs = new URL(base);
        const days = Number.parseInt(dom.expDays?.value || '30', 10) || 0;
        const limit = Number.parseInt(dom.expLimit?.value || '1000', 10) || 1;
        qs.searchParams.set('format', 'jsonl');
        qs.searchParams.set('days', String(days));
        qs.searchParams.set('limit', String(limit));
        const href = qs.toString();
        setStatus(dom.trainingStatus, 'Готовлю файл…', 'muted');
        const resp = await fetch(href);
        if (resp.status === 204) {
          setStatus(dom.trainingStatus, 'Нет диалогов под фильтры', 'alert');
          return;
        }
        if (!resp.ok) {
          const txt = await resp.text().catch(() => '');
          setStatus(dom.trainingStatus, txt || 'Ошибка экспорта, см. логи сервера', 'alert');
          return;
        }
        const blob = await resp.blob();
        const a = document.createElement('a');
        a.style.display = 'none';
        a.download = `training_${tenant}_${new Date().toISOString().slice(0,10).replace(/-/g,'')}.jsonl`;
        a.href = URL.createObjectURL(blob);
        document.body.appendChild(a);
        a.click();
        setTimeout(() => { URL.revokeObjectURL(a.href); document.body.removeChild(a); }, 250);
        setStatus(dom.trainingStatus, 'Файл скачан', 'muted');
      } catch (error) {
        setStatus(dom.trainingStatus, `Ошибка экспорта: ${error.message}`, 'alert');
      } finally {
        dom.expBtnJsonl.disabled = false;
      }
    });
  }
  if (dom.expBtnJson) {
    dom.expBtnJson.addEventListener('click', async (e) => {
      try { e.preventDefault(); } catch (_) {}
      dom.expBtnJson.disabled = true;
      try {
        const base = buildUrl(endpoints.trainingExport);
        const qs = new URL(base);
        const days = Number.parseInt(dom.expDays?.value || '30', 10) || 0;
        const limit = Number.parseInt(dom.expLimit?.value || '1000', 10) || 1;
        qs.searchParams.set('format', 'json');
        qs.searchParams.set('days', String(days));
        qs.searchParams.set('limit', String(limit));
        const href = qs.toString();
        setStatus(dom.trainingStatus, 'Готовлю файл…', 'muted');
        const resp = await fetch(href);
        if (resp.status === 204) {
          setStatus(dom.trainingStatus, 'Нет диалогов под фильтры', 'alert');
          return;
        }
        if (!resp.ok) {
          const txt = await resp.text().catch(() => '');
          setStatus(dom.trainingStatus, txt || 'Ошибка экспорта, см. логи сервера', 'alert');
          return;
        }
        const blob = await resp.blob();
        const a = document.createElement('a');
        a.style.display = 'none';
        a.download = `training_${tenant}_${new Date().toISOString().slice(0,10).replace(/-/g,'')}.json`;
        a.href = URL.createObjectURL(blob);
        document.body.appendChild(a);
        a.click();
        setTimeout(() => { URL.revokeObjectURL(a.href); document.body.removeChild(a); }, 250);
        setStatus(dom.trainingStatus, 'Файл скачан', 'muted');
      } catch (error) {
        setStatus(dom.trainingStatus, `Ошибка экспорта: ${error.message}`, 'alert');
      } finally {
        dom.expBtnJson.disabled = false;
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
})();
