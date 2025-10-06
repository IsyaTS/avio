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
    keys: Array.isArray(state.keys) ? state.keys : [],
    primary: state.primary || null
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

  function markPrimaryCell(td, key) {
    const primaryKey = ((dashboardState.primary || {}).key || '').toLowerCase();
    if (primaryKey && key && primaryKey === key.toLowerCase()) {
      const badge = document.createElement('span');
      badge.className = 'badge';
      badge.style.marginLeft = '8px';
      badge.textContent = 'primary';
      td.appendChild(badge);
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
      markPrimaryCell(keyTd, item.key);
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

      const primaryBtn = document.createElement('button');
      primaryBtn.type = 'button';
      primaryBtn.className = 'btn btn--secondary';
      primaryBtn.textContent = 'Сделать основным';
      primaryBtn.addEventListener('click', () => updatePrimary(item.key));
      actionsTd.appendChild(primaryBtn);

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
    const response = await fetch(url, Object.assign({
      headers: { 'Content-Type': 'application/json' }
    }, options || {}));
    if (!response.ok) {
      const detail = await response.text();
      throw new Error(detail || 'Ошибка запроса');
    }
    return await response.json();
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
      const data = await fetchJSON('/admin/keys/generate', {
        method: 'POST',
        body: JSON.stringify({ tenant: dashboardState.currentTenant, label: 'auto' })
      });
      await refreshKeys();
      const connectLink = absoluteUrl(data.link) || buildConnectLink(data.key);
      const settingsLink = buildSettingsLink(data.key);
      setMessage(
        `Новый ключ создан · <a href="${connectLink}" target="_blank" rel="noopener">подключение</a> · <a href="${settingsLink}" target="_blank" rel="noopener">настройки</a>`,
        'muted',
        true
      );
    } catch (error) {
      setMessage(error.message || 'Не удалось создать ключ', 'alert');
    }
  }

  async function updatePrimary(key) {
    if (!key) return;
    try {
      await fetchJSON('/admin/keys/set_primary', {
        method: 'POST',
        body: JSON.stringify({ tenant: dashboardState.currentTenant, key })
      });
      dashboardState.primary = { key };
      renderKeys();
      setMessage('Основной ключ обновлён');
    } catch (error) {
      setMessage(error.message || 'Не удалось обновить ключ', 'alert');
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
        body: JSON.stringify({ tenant: dashboardState.currentTenant, key })
      });
      dashboardState.keys = (dashboardState.keys || []).filter((item) => item.key !== key);
      if (dashboardState.primary && dashboardState.primary.key === key) {
        dashboardState.primary = null;
      }
      renderKeys();
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
      primary: form.primary.checked
    };
    if (!payload.key) {
      setMessage('Укажите ключ перед сохранением', 'alert');
      return;
    }
    try {
      const data = await fetchJSON('/admin/keys/save', {
        method: 'POST',
        body: JSON.stringify(payload)
      });
      form.reset();
      await refreshKeys();
      const connectLink = absoluteUrl(data.link) || buildConnectLink(payload.key);
      const settingsLink = buildSettingsLink(payload.key);
      setMessage(
        `Ключ сохранён · <a href="${connectLink}" target="_blank" rel="noopener">подключение</a> · <a href="${settingsLink}" target="_blank" rel="noopener">настройки</a>`,
        'muted',
        true
      );
    } catch (error) {
      setMessage(error.message || 'Не удалось сохранить ключ', 'alert');
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
