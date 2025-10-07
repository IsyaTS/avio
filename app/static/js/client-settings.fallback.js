(function () {
  function resolveUrl(raw) {
    const origin = window.location.origin;
    try {
      const url = new URL(raw || '/export/whatsapp', origin);
      if (url.hostname !== window.location.hostname) {
        url.hostname = window.location.hostname;
        url.protocol = window.location.protocol;
        url.port = window.location.port;
      }
      return url.toString();
    } catch (error) {
      return raw || '/export/whatsapp';
    }
  }

  function parseNumber(value, { min = null, fallback = 0 } = {}) {
    const numeric = Number.parseInt(value, 10);
    if (!Number.isFinite(numeric)) return fallback;
    if (min !== null && numeric < min) return min;
    return numeric;
  }

  async function performExport({ endpoint, payload }) {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (response.status === 204) {
      return { status: 204 };
    }

    if (!response.ok) {
      let detail = '';
      try {
        const data = await response.clone().json();
        if (data && typeof data === 'object') {
          const candidate = data.detail || data.message || data.reason;
          if (typeof candidate === 'string') {
            detail = candidate;
          }
        }
      } catch (error) {
        try {
          detail = (await response.text()) || '';
        } catch (_) {
          detail = '';
        }
      }
      const message = detail || `Ошибка экспорта (HTTP ${response.status})`;
      const error = new Error(message);
      error.status = response.status;
      error.detail = detail;
      throw error;
    }

    const contentType = (response.headers.get('content-type') || '').toLowerCase();
    if (!contentType.startsWith('application/zip')) {
      let detail = '';
      try {
        detail = (await response.text()) || '';
      } catch (_) {
        detail = '';
      }
      const error = new Error(detail || 'Ответ сервера не является ZIP-архивом');
      error.status = response.status;
      error.detail = detail;
      throw error;
    }

    const blob = await response.blob();
    const disposition = response.headers.get('content-disposition') || '';
    let filename = '';
    const match = disposition.match(/filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i);
    if (match) {
      const encoded = (match[1] || match[2] || '').trim();
      if (encoded) {
        try {
          filename = decodeURIComponent(encoded);
        } catch (_) {
          filename = encoded;
        }
      }
    }

    if (!filename) {
      const now = new Date();
      const y = now.getUTCFullYear();
      const m = String(now.getUTCMonth() + 1).padStart(2, '0');
      const d = String(now.getUTCDate()).padStart(2, '0');
      filename = `whatsapp_export_${y}-${m}-${d}.zip`;
    }

    const dialogCount = Number.parseInt(response.headers.get('X-Dialog-Count') || '', 10);
    const messageCount = Number.parseInt(response.headers.get('X-Message-Count') || '', 10);

    return {
      status: 200,
      blob,
      filename,
      dialogCount: Number.isFinite(dialogCount) ? dialogCount : null,
      messageCount: Number.isFinite(messageCount) ? messageCount : null,
    };
  }

  function bindFallbackExport(state) {
    const button = document.getElementById('export-download');
    if (!button || (button.dataset && button.dataset.bound === '1')) {
      return false;
    }

    const statusNode = document.getElementById('export-status');
    const daysInput = document.getElementById('exp-days');
    const limitInput = document.getElementById('exp-limit');
    const perInput = document.getElementById('exp-per');

    const maxDays = Number.isFinite(state.max_days) && state.max_days > 0 ? Number(state.max_days) : null;
    const endpoint = resolveUrl(state?.urls?.whatsapp_export);
    const tenant = Number.parseInt(state.tenant, 10) || 0;
    const key = typeof state.key === 'string' ? state.key : '';

    const updateStatus = (message, variant = 'muted') => {
      if (!statusNode) return;
      statusNode.className = `status-text ${variant}`.trim();
      statusNode.textContent = message || '';
    };

    button.type = 'button';

    button.addEventListener('click', async (event) => {
      event.preventDefault();
      event.stopPropagation();

      const daysRaw = daysInput ? daysInput.value : '';
      const limitRaw = limitInput ? limitInput.value : '';

      let days = parseNumber(daysRaw, { min: 0, fallback: 0 });
      if (maxDays !== null && days > maxDays) {
        days = maxDays;
      }
      if (daysInput) {
        daysInput.value = String(days);
      }

      const limit = parseNumber(limitRaw, { min: 1, fallback: 200 });
      if (limitInput) {
        limitInput.value = String(limit);
      }

      if (perInput) {
        perInput.value = '0';
      }

      const payload = { tenant, key, days, limit, per: 0 };

      button.disabled = true;
      updateStatus('Готовим архив…', 'muted');

      try {
        const result = await performExport({ endpoint, payload });
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
          URL.revokeObjectURL(blobUrl);
          anchor.remove();
        }, 100);

        if (result.dialogCount != null && result.messageCount != null) {
          updateStatus(`Сформировано: ${result.dialogCount} диалогов, ${result.messageCount} сообщений`, 'muted');
        } else {
          updateStatus('Архив сформирован', 'muted');
        }
        window.__EXPORT_BIND_OK__ = true;
      } catch (error) {
        const message = (error && error.message) || 'Не удалось скачать архив';
        updateStatus(message, 'alert');
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

  window.addEventListener('DOMContentLoaded', () => {
    if (window.__EXPORT_BIND_OK__ === true) {
      return;
    }

    console.warn('[client-settings.fallback] primary bind missing; applying fallback handler');
    const state = (window.state && typeof window.state === 'object') ? window.state : {};
    bindFallbackExport(state);
  });
})();
