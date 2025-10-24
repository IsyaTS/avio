(function () {
  'use strict';

  if (typeof window !== 'undefined') {
    window.__catalogUploadV2 = true;
  }

  function onDomReady(callback) {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', callback, { once: true });
    } else {
      callback();
    }
  }

  function resolveClientSettings() {
    const settings = (window.CLIENT_SETTINGS && typeof window.CLIENT_SETTINGS === 'object')
      ? window.CLIENT_SETTINGS
      : {};
    return {
      publicKey: settings.public_key || '',
      tenant: settings.tenant || settings.tenant_id || 1,
    };
  }

  function buildUploadUrl(publicKey, tenant) {
    const key = encodeURIComponent(publicKey || '');
    const tenantId = encodeURIComponent(String(Number.parseInt(tenant, 10) || 1));
    return `https://api.avio.website/pub/catalog/upload?k=${key}&tenant=${tenantId}`;
  }

  function buildStatusUrl(jobId, publicKey, tenant) {
    const encodedJob = encodeURIComponent(jobId);
    const key = encodeURIComponent(publicKey || '');
    const tenantId = encodeURIComponent(String(Number.parseInt(tenant, 10) || 1));
    return `https://api.avio.website/pub/catalog/status/${encodedJob}?k=${key}&tenant=${tenantId}`;
  }

  function setStatus(node, message, variant) {
    if (!node) {
      return;
    }
    const normalized = (message == null ? '' : String(message)).trim();
    node.textContent = normalized;
    if (variant) {
      node.dataset.variant = variant;
    } else {
      delete node.dataset.variant;
    }
  }

  function toggleProgress(progressNode, isVisible) {
    if (!progressNode) {
      return;
    }
    if (isVisible) {
      progressNode.hidden = false;
    } else {
      progressNode.hidden = true;
      progressNode.value = 0;
    }
  }

  function updateProgress(progressNode, value) {
    if (!progressNode) {
      return;
    }
    const numeric = Number(value);
    const bounded = Number.isFinite(numeric) ? Math.max(0, Math.min(100, numeric)) : 0;
    progressNode.value = bounded;
  }

  onDomReady(() => {
    const form = document.getElementById('catalog-upload-form');
    const fileInput = document.getElementById('catalog-file');
    const uploadButton = document.getElementById('catalog-upload-btn');
    const statusNode = document.getElementById('catalog-upload-status');
    const progressNode = document.getElementById('catalog-upload-progress');

    if (!form || !fileInput || !uploadButton) {
      return;
    }

    let pollingTimer = null;
    let uploading = false;

    const { publicKey, tenant } = resolveClientSettings();

    if (!publicKey) {
      setStatus(statusNode, 'Нет публичного ключа клиента', 'error');
      uploadButton.disabled = true;
      return;
    }

    function stopPolling() {
      if (pollingTimer) {
        clearTimeout(pollingTimer);
        pollingTimer = null;
      }
    }

    function scheduleNextPoll(jobId) {
      stopPolling();
      pollingTimer = window.setTimeout(() => pollStatus(jobId), 1000);
    }

    function handlePollingError(error) {
      setStatus(statusNode, error && error.message ? error.message : 'Не удалось получить статус', 'error');
      stopPolling();
    }

    function pollStatus(jobId) {
      const statusUrl = buildStatusUrl(jobId, publicKey, tenant);
      fetch(statusUrl, { cache: 'no-store', headers: { Accept: 'application/json' } })
        .then((response) => {
          if (response.status === 404) {
            throw new Error('Статус обработки не найден');
          }
          if (response.status === 401) {
            throw new Error('Ключ доступа недействителен');
          }
          if (!response.ok) {
            throw new Error(`Ошибка статуса (HTTP ${response.status})`);
          }
          return response.json();
        })
        .then((payload) => {
          if (!payload || typeof payload !== 'object') {
            throw new Error('Некорректный ответ статуса');
          }
          const state = typeof payload.state === 'string' ? payload.state.toLowerCase() : '';
          if (state === 'done') {
            setStatus(statusNode, 'Готово', 'success');
            stopPolling();
            window.setTimeout(() => {
              window.location.reload();
            }, 300);
            return;
          }
          if (state === 'failed') {
            const errorMessage = payload.error || payload.message || payload.detail || 'Обработка завершилась с ошибкой';
            setStatus(statusNode, errorMessage, 'error');
            stopPolling();
            toggleProgress(progressNode, false);
            uploading = false;
            uploadButton.disabled = false;
            return;
          }
          const statusText = payload.status_text || payload.status || '';
          if (statusText) {
            setStatus(statusNode, statusText, 'muted');
          }
          scheduleNextPoll(jobId);
        })
        .catch((error) => {
          handlePollingError(error);
          toggleProgress(progressNode, false);
          uploading = false;
          uploadButton.disabled = false;
        });
    }

    function resetUploadState() {
      uploading = false;
      uploadButton.disabled = false;
      toggleProgress(progressNode, false);
    }

    function startUpload(file) {
      const uploadUrl = buildUploadUrl(publicKey, tenant);
      const xhr = new XMLHttpRequest();
      const formData = new FormData();
      formData.append('file', file);

      uploading = true;
      uploadButton.disabled = true;
      toggleProgress(progressNode, true);
      updateProgress(progressNode, 0);
      setStatus(statusNode, 'Загрузка 0%', 'muted');

      xhr.open('POST', uploadUrl, true);
      xhr.responseType = 'json';

      xhr.upload.onprogress = (event) => {
        if (!event) {
          return;
        }
        if (!event.lengthComputable) {
          setStatus(statusNode, 'Загрузка…', 'muted');
          return;
        }
        const percent = event.total > 0 ? Math.round((event.loaded / event.total) * 100) : 0;
        updateProgress(progressNode, percent);
        setStatus(statusNode, `Загрузка ${percent}%`, 'muted');
      };

      const fail = (message) => {
        resetUploadState();
        setStatus(statusNode, message || 'Не удалось загрузить файл', 'error');
      };

      xhr.onerror = () => fail('Не удалось загрузить файл. Проверьте подключение.');
      xhr.onabort = () => fail('Загрузка прервана.');

      xhr.onload = () => {
        const status = xhr.status;
        let payload = null;
        if (xhr.response && typeof xhr.response === 'object') {
          payload = xhr.response;
        } else if (xhr.responseText) {
          try {
            payload = JSON.parse(xhr.responseText);
          } catch (error) {
            payload = null;
          }
        }

        if (status >= 400) {
          const errorMessage = payload && (payload.message || payload.error || payload.detail);
          fail(errorMessage || `Ошибка загрузки (HTTP ${status})`);
          return;
        }

        updateProgress(progressNode, 100);

        if (!payload || typeof payload !== 'object') {
          fail('Некорректный ответ сервера');
          return;
        }

        const state = typeof payload.state === 'string' ? payload.state.toLowerCase() : '';
        const jobId = payload.job_id || payload.jobId;
        if (!jobId) {
          if (state === 'done') {
            setStatus(statusNode, 'Готово', 'success');
            window.setTimeout(() => {
              window.location.reload();
            }, 300);
            return;
          }
          if (state === 'failed') {
            const errorMessage = payload.error || payload.message || payload.detail || 'Обработка завершилась с ошибкой';
            fail(errorMessage);
            return;
          }
          fail('Не удалось получить идентификатор задачи');
          return;
        }

        toggleProgress(progressNode, false);
        setStatus(statusNode, 'Файл принят. Обработка…', 'muted');
        fileInput.value = '';
        scheduleNextPoll(String(jobId));
      };

      try {
        xhr.send(formData);
      } catch (error) {
        fail(error && error.message ? error.message : 'Не удалось отправить файл');
      }
    }

    uploadButton.addEventListener('click', (event) => {
      event.preventDefault();
      if (uploading) {
        return;
      }
      const file = fileInput.files && fileInput.files[0];
      if (!file) {
        setStatus(statusNode, 'Выберите файл перед загрузкой', 'error');
        return;
      }
      startUpload(file);
    });
  });
})();
