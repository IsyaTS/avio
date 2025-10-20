(function () {
  const stateScript = document.getElementById('admin-dashboard-state');
  if (!stateScript) return;

  let state = {};
  try {
    state = JSON.parse(stateScript.textContent || '{}');
  } catch (err) {
    console.error('Failed to parse admin dashboard state', err);
    state = {};
  }

  const rawBase = typeof state.public_base === 'string' ? state.public_base.trim() : '';
  const windowOrigin = (typeof window !== 'undefined' && window.location && window.location.origin) ? window.location.origin : '';
  const publicBase = (rawBase || windowOrigin || '').replace(/\/$/, '');

  function absoluteUrl(input) {
    if (!input) return '';
    if (/^[a-z]+:\/\//i.test(input)) return input;
    const leading = input.startsWith('/') ? input : `/${input}`;
    if (!publicBase) return leading;
    return `${publicBase}${leading}`;
  }

  const tenant = Number.parseInt(state.tenant, 10) || 1;
  const tableBody = document.querySelector('#keys-table tbody');
  const emptyState = document.getElementById('keys-empty');
  const messageBox = document.getElementById('keys-message');
  const waStatusBox = document.getElementById('wa-status');
  const waLink = document.getElementById('wa-open');
  const tenantInput = document.getElementById('tenant-input');
  const tenantForm = document.getElementById('tenant-form');
  const manualForm = document.getElementById('manual-key-form');
  const generateBtn = document.getElementById('generate-key');
  const refreshBtn = document.getElementById('refresh-keys');
  const refreshWAButton = document.getElementById('wa-refresh');

  const dashboardState = {
    currentTenant: tenant,
    keys: Array.isArray(state.keys) ? state.keys : []
  };

  function buildConnectLink(key) {
    return absoluteUrl(`/connect/wa?tenant=${dashboardState.currentTenant}&k=${encodeURIComponent(key || '')}`);
  }

  function buildSettingsLink(key) {
    return absoluteUrl(`/client/${dashboardState.currentTenant}/settings?k=${encodeURIComponent(key || '')}`);
  }

  function setMessage(text, variant = 'muted', allowHTML = false) {
    if (!messageBox) return;
    messageBox.className = `status-text ${variant}`.trim();
    if (allowHTML) {
      messageBox.innerHTML = text || '';
    } else {
      messageBox.textContent = text || '';
    }
  }

  function renderKeys() {
    if (!tableBody) return;
    tableBody.innerHTML = '';
    const keys = Array.isArray(dashboardState.keys) ? dashboardState.keys : [];
    if (!keys.length) {
      if (emptyState) emptyState.style.display = 'block';
      return;
    }
    if (emptyState) emptyState.style.display = 'none';

    keys.forEach((item) => {
      const tr = document.createElement('tr');

      const labelTd = document.createElement('td');
      labelTd.textContent = item.label || '—';
      tr.appendChild(labelTd);

      const keyTd = document.createElement('td');
      keyTd.textContent = item.key || '';
      tr.appendChild(keyTd);

      const linkTd = document.createElement('td');
      const connectLink = absoluteUrl(item.link) || buildConnectLink(item.key);
      const settingsLink = absoluteUrl(item.settings_link) || buildSettingsLink(item.key);

      const anchor = document.createElement('a');
      anchor.href = connectLink;
      anchor.target = '_blank';
      anchor.rel = 'noopener';
      anchor.textContent = 'Открыть ссылку';
      linkTd.appendChild(anchor);
      tr.appendChild(linkTd);

      const actionsTd = document.createElement('td');
      actionsTd.style.display = 'flex';
      actionsTd.style.flexWrap = 'wrap';
      actionsTd.style.gap = '8px';

      const copyKeyBtn = document.createElement('button');
      copyKeyBtn.type = 'button';
      copyKeyBtn.className = 'btn btn--secondary';
      copyKeyBtn.textContent = 'Ключ';
      copyKeyBtn.addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(item.key || '');
          setMessage('Ключ скопирован в буфер обмена');
        } catch (error) {
          setMessage('Не удалось скопировать ключ', 'alert');
        }
      });
      actionsTd.appendChild(copyKeyBtn);

      const copyLinkBtn = document.createElement('button');
      copyLinkBtn.type = 'button';
      copyLinkBtn.className = 'btn btn--secondary';
      copyLinkBtn.textContent = 'Ссылка';
      copyLinkBtn.addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(connectLink);
          setMessage('Ссылка скопирована');
        } catch (error) {
          setMessage('Не удалось скопировать ссылку', 'alert');
        }
      });
      actionsTd.appendChild(copyLinkBtn);

      const openSettingsBtn = document.createElement('a');
      openSettingsBtn.href = settingsLink;
      openSettingsBtn.target = '_blank';
      openSettingsBtn.rel = 'noopener';
      openSettingsBtn.className = 'btn btn--secondary';
      openSettingsBtn.textContent = 'Настройки';
      actionsTd.appendChild(openSettingsBtn);

      const deleteBtn = document.createElement('button');
      deleteBtn.type = 'button';
      deleteBtn.className = 'btn btn--danger';
      deleteBtn.textContent = 'Удалить';
      deleteBtn.addEventListener('click', () => deleteKey(item.key));
      actionsTd.appendChild(deleteBtn);

      tr.appendChild(actionsTd);
      tableBody.appendChild(tr);
    });
  }

  async function fetchJSON(url, options) {
    const opts = Object.assign({}, options || {});
    opts.headers = Object.assign({}, opts.headers || {});
    if (opts.body && typeof opts.body !== 'string') {
      opts.body = JSON.stringify(opts.body);
    }
    if (opts.body && !opts.headers['Content-Type']) {
      opts.headers['Content-Type'] = 'application/json';
    }
    const response = await fetch(url, opts);
    const contentType = response.headers.get('Content-Type') || '';
    let payload = null;
    if (contentType.includes('application/json')) {
      try {
        payload = await response.json();
      } catch (err) {
        payload = null;
      }
    } else if (response.status !== 204) {
      const text = await response.text();
      payload = text ? { error: text } : null;
    }
    if (!response.ok) {
      const message = payload && typeof payload === 'object' && payload.error ? payload.error : 'Ошибка запроса';
      const error = new Error(message);
      if (payload && typeof payload === 'object' && payload.error) {
        error.code = payload.error;
      }
      error.status = response.status;
      throw error;
    }
    if (payload && payload.error && typeof payload.error === 'string' && Object.keys(payload).length === 1) {
      return {};
    }
    return payload || {};
  }

  async function refreshKeys() {
    try {
      const data = await fetchJSON(`/admin/keys/list?tenant=${dashboardState.currentTenant}`);
      dashboardState.keys = data.items || [];
      renderKeys();
      setMessage('Список ключей обновлён');
    } catch (error) {
      setMessage(error.message || 'Не удалось обновить список', 'alert');
    }
  }

  async function generateKey() {
    try {
      const data = await fetchJSON(`/admin/key/get?tenant=${dashboardState.currentTenant}`);
      await refreshKeys();
      const keyItem = Array.isArray(dashboardState.keys) && dashboardState.keys.length ? dashboardState.keys[0] : null;
      const keyValue = (keyItem && keyItem.key) || (data && data.key) || '';
      const connectLink = buildConnectLink(keyValue);
      const settingsLink = buildSettingsLink(keyValue);
      setMessage(
        `Ключ готов · <a href="${connectLink}" target="_blank" rel="noopener">подключение</a> · <a href="${settingsLink}" target="_blank" rel="noopener">настройки</a>`,
        'muted',
        true
      );
    } catch (error) {
      if (error && error.code === 'key_already_exists') {
        setMessage('Ключ уже существует для этого арендатора', 'alert');
      } else {
        setMessage((error && error.message) || 'Не удалось создать ключ', 'alert');
      }
    }
  }

  async function deleteKey(key) {
    if (!key) return;
    if (!window.confirm('Удалить этот ключ?')) {
      return;
    }
    try {
      await fetchJSON('/admin/keys/delete', {
        method: 'POST',
        body: { tenant: dashboardState.currentTenant, key }
      });
      await refreshKeys();
      setMessage('Ключ удалён');
    } catch (error) {
      setMessage(error.message || 'Не удалось удалить ключ', 'alert');
    }
  }

  async function submitManualKey(event) {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = {
      tenant: dashboardState.currentTenant,
      key: form.key.value.trim(),
      label: form.label.value.trim(),
    };
    if (!payload.key) {
      setMessage('Укажите ключ перед сохранением', 'alert');
      return;
    }
    try {
      const data = await fetchJSON('/admin/keys/save', {
        method: 'POST',
        body: payload
      });
      form.reset();
      await refreshKeys();
      const keyItem = Array.isArray(dashboardState.keys) && dashboardState.keys.length ? dashboardState.keys[0] : null;
      const keyValue = (keyItem && keyItem.key) || payload.key;
      const connectLink = buildConnectLink(keyValue);
      const settingsLink = buildSettingsLink(keyValue);
      setMessage(
        `Ключ сохранён · <a href="${connectLink}" target="_blank" rel="noopener">подключение</a> · <a href="${settingsLink}" target="_blank" rel="noopener">настройки</a>`,
        'muted',
        true
      );
    } catch (error) {
      if (error && error.code === 'key_already_exists') {
        setMessage('Перед сохранением удалите текущий ключ', 'alert');
      } else {
        setMessage((error && error.message) || 'Не удалось сохранить ключ', 'alert');
      }
    }
  }

  async function refreshWA() {
    if (!waStatusBox) return;
    waStatusBox.textContent = 'Проверяем статус…';
    waStatusBox.className = 'badge tag-neutral';
    try {
      const response = await fetch(`/admin/wa/status?tenant=${dashboardState.currentTenant}`);
      if (!response.ok) throw new Error('Статус недоступен');
      const data = await response.json();
      const stateValue = String(data.state || '').toLowerCase();
      if (stateValue === 'ready' || stateValue === 'connected') {
        waStatusBox.textContent = 'Подключено';
        waStatusBox.className = 'badge';
      } else if (stateValue === 'qr' || stateValue === 'disconnected') {
        waStatusBox.textContent = 'Нужна авторизация';
        waStatusBox.className = 'badge tag-neutral';
      } else {
        waStatusBox.textContent = data.state || 'Неизвестный статус';
        waStatusBox.className = 'badge tag-neutral';
      }
    } catch (error) {
      waStatusBox.textContent = error.message || 'Не удалось получить статус';
      waStatusBox.className = 'badge tag-neutral';
    }
  }

  if (tenantInput) {
    tenantInput.value = dashboardState.currentTenant;
  }

  if (tenantForm) {
    tenantForm.addEventListener('submit', (event) => {
      event.preventDefault();
      const value = Number.parseInt(tenantInput.value || '1', 10);
      if (!Number.isNaN(value) && value > 0) {
        window.location.href = `/admin?tenant=${value}`;
      }
    });
  }

  if (manualForm) {
    manualForm.addEventListener('submit', submitManualKey);
  }
  if (generateBtn) {
    generateBtn.addEventListener('click', generateKey);
  }
  if (refreshBtn) {
    refreshBtn.addEventListener('click', refreshKeys);
  }
  if (refreshWAButton) {
    refreshWAButton.addEventListener('click', refreshWA);
  }

  if (waLink) {
    waLink.href = absoluteUrl(`/admin/wa/qr.svg?tenant=${dashboardState.currentTenant}`);
  }

  renderKeys();
  refreshWA();
})();
