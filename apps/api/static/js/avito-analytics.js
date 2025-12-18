(function () {
  const stateScript = document.getElementById('avito-analytics-state');
  let pageState = {};
  try {
    pageState = JSON.parse(stateScript?.textContent || '{}');
  } catch (err) {
    console.error('Failed to parse avito analytics state', err);
    pageState = {};
  }

  const accountSelect = document.getElementById('account-select');
  const periodSelect = document.getElementById('period-select');
  const messageBox = document.getElementById('analytics-message');
  const cardsContainer = document.getElementById('analytics-cards');
  const itemsTableBody = document.querySelector('#items-table tbody');
  const operationsList = document.getElementById('operations-list');
  const rawPre = document.getElementById('raw-json');
  const connectBtn = document.getElementById('connect-avito');
  const refreshBtn = document.getElementById('refresh-report');
  const exportJson = document.getElementById('export-json');
  const exportCsv = document.getElementById('export-csv');
  const copyRaw = document.getElementById('copy-raw');

  let accounts = Array.isArray(pageState.accounts) ? pageState.accounts : [];
  let currentAccount = pageState.default_account || (accounts[0] && accounts[0].account_id);

  function setMessage(text, variant = 'muted') {
    if (!messageBox) return;
    messageBox.className = `status-text ${variant}`;
    messageBox.textContent = text || '';
  }

  async function fetchJSON(url, options = {}) {
    const resp = await fetch(url, options);
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || resp.statusText);
    }
    return resp.json();
  }

  function renderAccounts() {
    if (!accountSelect) return;
    accountSelect.innerHTML = '';
    if (!accounts.length) {
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = 'Нет подключённых аккаунтов';
      accountSelect.appendChild(opt);
      return;
    }
    accounts.forEach((acc) => {
      const opt = document.createElement('option');
      opt.value = acc.account_id;
      opt.textContent = acc.display_name ? `${acc.display_name} (${acc.account_id})` : `Account ${acc.account_id}`;
      if (Number(acc.account_id) === Number(currentAccount)) {
        opt.selected = true;
      }
      accountSelect.appendChild(opt);
    });
  }

  function renderCards(report) {
    if (!cardsContainer) return;
    cardsContainer.innerHTML = '';
    if (!report || !report.summary_cards) return;
    const cards = [
      { label: 'Активные', value: report.summary_cards.active_items },
      { label: 'Неактивные', value: report.summary_cards.inactive_items },
      { label: 'Просмотры', value: report.summary_cards.views },
      { label: 'Контакты', value: report.summary_cards.contacts },
      { label: 'Звонки', value: report.summary_cards.calls },
      { label: 'Списания', value: report.summary_cards.spend },
      { label: 'Чаты', value: report.summary_cards.chats },
    ];
    cards.forEach((card) => {
      const el = document.createElement('div');
      el.className = 'surface stack';
      el.style.gap = '6px';
      el.innerHTML = `<div class="section-subtitle">${card.label}</div><div class="section-title" style="font-size:1.6rem;">${card.value ?? '—'}</div>`;
      cardsContainer.appendChild(el);
    });
  }

  function renderItems(report) {
    if (!itemsTableBody) return;
    itemsTableBody.innerHTML = '';
    const rows = Array.isArray(report?.items_table) ? report.items_table : [];
    if (!rows.length) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 8;
      td.textContent = 'Нет данных по объявлениям';
      tr.appendChild(td);
      itemsTableBody.appendChild(tr);
      return;
    }
    rows.forEach((row) => {
      const tr = document.createElement('tr');
      const fields = ['id', 'title', 'status', 'price', 'views', 'contacts', 'calls'];
      fields.forEach((key) => {
        const td = document.createElement('td');
        td.textContent = row[key] != null ? row[key] : '';
        tr.appendChild(td);
      });
      const linkTd = document.createElement('td');
      if (row.url) {
        const a = document.createElement('a');
        a.href = row.url;
        a.target = '_blank';
        a.rel = 'noopener';
        a.textContent = 'Открыть';
        linkTd.appendChild(a);
      } else {
        linkTd.textContent = '—';
      }
      tr.appendChild(linkTd);
      itemsTableBody.appendChild(tr);
    });
  }

  function renderOperations(report) {
    if (!operationsList) return;
    operationsList.innerHTML = '';
    const ops = Array.isArray(report?.operations) ? report.operations.slice(0, 10) : [];
    if (!ops.length) {
      const div = document.createElement('div');
      div.className = 'status-text';
      div.textContent = 'Операций не найдено';
      operationsList.appendChild(div);
      return;
    }
    ops.forEach((op) => {
      const card = document.createElement('div');
      card.className = 'surface surface--muted';
      card.style.padding = '10px 12px';
      card.innerHTML = `
        <div class="section-title" style="font-size:1rem;">${op.description || op.title || op.type || 'Операция'}</div>
        <div class="section-subtitle">${op.date || op.created_at || ''}</div>
        <div class="section-title" style="font-size:1.1rem;">${op.amount ?? op.sum ?? ''}</div>
      `;
      operationsList.appendChild(card);
    });
  }

  function renderRaw(raw) {
    if (!rawPre) return;
    rawPre.textContent = raw ? JSON.stringify(raw, null, 2) : '';
  }

  function updateExportLinks() {
    const params = new URLSearchParams();
    if (currentAccount) params.set('account_id', currentAccount);
    params.set('period', periodSelect?.value || '30');
    exportJson.href = `/admin/avito-analytics/api/export.json?${params.toString()}`;
    exportCsv.href = `/admin/avito-analytics/api/export.csv?${params.toString()}`;
  }

  async function loadAccounts() {
    try {
      const data = await fetchJSON('/admin/avito-analytics/api/accounts');
      accounts = Array.isArray(data.accounts) ? data.accounts : [];
      if (!currentAccount && accounts.length) {
        currentAccount = accounts[0].account_id;
      }
      renderAccounts();
    } catch (err) {
      setMessage('Не удалось загрузить аккаунты', 'alert');
    }
  }

  async function loadReport() {
    if (!currentAccount) {
      setMessage('Подключите Avito аккаунт', 'alert');
      renderItems({});
      renderCards({});
      renderOperations({});
      renderRaw({});
      return;
    }
    setMessage('Загружаем...', 'muted');
    const period = periodSelect?.value || '30';
    try {
      const data = await fetchJSON(`/admin/avito-analytics/api/report?account_id=${encodeURIComponent(currentAccount)}&period=${encodeURIComponent(period)}`);
      if (!data || !data.ok) {
        setMessage('Отчёт недоступен', 'alert');
        return;
      }
      const report = data.report || {};
      renderCards(report);
      renderItems(report);
      renderOperations(report);
      renderRaw(report.raw || {});
      updateExportLinks();
      const warnings = Array.isArray(report.meta?.warnings) ? report.meta.warnings.filter(Boolean) : [];
      if (warnings.length) {
        setMessage(`Предупреждения: ${warnings.join(' · ')}`, 'warn');
      } else {
        setMessage('Обновлено', 'muted');
      }
    } catch (err) {
      console.error(err);
      setMessage('Ошибка загрузки отчёта', 'alert');
    }
  }

  function bindEvents() {
    accountSelect?.addEventListener('change', () => {
      currentAccount = accountSelect.value;
      updateExportLinks();
      loadReport().catch(() => {});
    });
    periodSelect?.addEventListener('change', () => {
      updateExportLinks();
      loadReport().catch(() => {});
    });
    connectBtn?.addEventListener('click', () => {
      window.location.href = '/admin/avito-analytics/oauth/start';
    });
    refreshBtn?.addEventListener('click', async () => {
      if (!currentAccount) return;
      const period = periodSelect?.value || '30';
      try {
        await fetchJSON(`/admin/avito-analytics/api/refresh?account_id=${encodeURIComponent(currentAccount)}&period=${encodeURIComponent(period)}`, { method: 'POST' });
        loadReport().catch(() => {});
      } catch {
        setMessage('Не удалось обновить', 'alert');
      }
    });
    copyRaw?.addEventListener('click', async () => {
      try {
        await navigator.clipboard.writeText(rawPre?.textContent || '');
        setMessage('Raw JSON скопирован', 'muted');
      } catch {
        setMessage('Не удалось скопировать', 'alert');
      }
    });
  }

  (async function init() {
    renderAccounts();
    bindEvents();
    updateExportLinks();
    await loadAccounts();
    await loadReport();
  })().catch((err) => {
    console.error(err);
    setMessage('Инициализация не удалась', 'alert');
  });
})();
