(function () {
  const stateScript = document.getElementById('avito-analytics-state');
  const state = JSON.parse(stateScript?.textContent || '{}');
  const tenant = state.tenant;
  const key = state.k;

  const periodSelect = document.getElementById('period-select');
  const slaSelect = document.getElementById('sla-select');
  const modeSelect = document.getElementById('mode-select');
  const statusText = document.getElementById('status-text');
  const refreshBtn = document.getElementById('refresh-btn');
  const lastUpdated = document.getElementById('last-updated');
  const connectBtn = document.getElementById('connect-btn');
  const oauthStatus = document.getElementById('oauth-status');
  const kpiCards = document.getElementById('kpi-cards');
  const funnelBlock = document.getElementById('funnel-block');
  const lossesBlock = document.getElementById('losses-block');
  const itemsTableBody = document.querySelector('#items-table tbody');
  const itemsSearch = document.getElementById('items-search');
  const heatmapTable = document.getElementById('heatmap-table');
  const rawPre = document.getElementById('raw-json');

  const avgCheckInput = document.getElementById('avg-check');
  const closeRateInput = document.getElementById('close-rate');
  const marginInput = document.getElementById('margin');
  const lossFactorInput = document.getElementById('loss-factor');
  const workdayStartInput = document.getElementById('workday-start');
  const workdayEndInput = document.getElementById('workday-end');
  const weekendInputs = Array.from(document.querySelectorAll('input[data-weekend-day]'));
  const messagingMetrics = document.getElementById('messaging-metrics');
  const hourlyChartEl = document.getElementById('hourly-chart');

  let trendChart = null;
  let hourlyChart = null;
  let cachedItems = [];

  function setStatus(text, variant = 'muted') {
    if (!statusText) return;
    statusText.className = `status-text ${variant}`;
    statusText.textContent = text || '';
  }

  function persistParams() {
    const weekendDays = weekendInputs.filter((input) => input.checked).map((input) => input.value).join(',');
    const payload = {
      avg_check: avgCheckInput?.value || '',
      close_rate_chat: closeRateInput?.value || '',
      gross_margin: marginInput?.value || '',
      loss_factor_slow_response: lossFactorInput?.value || '',
      workday_start: workdayStartInput?.value || '',
      workday_end: workdayEndInput?.value || '',
      weekend_days: weekendDays,
    };
    try {
      localStorage.setItem('avito-analytics-params', JSON.stringify(payload));
    } catch (_) {}
  }

  function restoreParams() {
    let raw = null;
    try {
      raw = localStorage.getItem('avito-analytics-params');
    } catch (_) {}
    if (!raw) {
      if (workdayStartInput) workdayStartInput.value = '09:00';
      if (workdayEndInput) workdayEndInput.value = '21:00';
      if (weekendInputs.length) {
        weekendInputs.forEach((input) => {
          input.checked = input.value === '5' || input.value === '6';
        });
      }
      return;
    }
    try {
      const data = JSON.parse(raw) || {};
      if (avgCheckInput && data.avg_check) avgCheckInput.value = data.avg_check;
      if (closeRateInput && data.close_rate_chat) closeRateInput.value = data.close_rate_chat;
      if (marginInput && data.gross_margin) marginInput.value = data.gross_margin;
      if (lossFactorInput && data.loss_factor_slow_response) lossFactorInput.value = data.loss_factor_slow_response;
      if (workdayStartInput && Object.prototype.hasOwnProperty.call(data, 'workday_start')) {
        workdayStartInput.value = data.workday_start || '';
      }
      if (workdayEndInput && Object.prototype.hasOwnProperty.call(data, 'workday_end')) {
        workdayEndInput.value = data.workday_end || '';
      }
      if (weekendInputs.length && Object.prototype.hasOwnProperty.call(data, 'weekend_days')) {
        const selected = String(data.weekend_days || '')
          .split(',')
          .map((val) => val.trim())
          .filter(Boolean);
        weekendInputs.forEach((input) => {
          input.checked = selected.includes(input.value);
        });
      }
    } catch (_) {}
  }
  restoreParams();

  function buildParams(force = false) {
    const params = new URLSearchParams();
    params.set('tenant', tenant);
    if (key) params.set('k', key);
    params.set('period', periodSelect?.value || '7');
    params.set('sla', slaSelect?.value || '15');
    if (modeSelect?.value !== 'full') params.set('fast', '1');
    if (force) params.set('force', '1');
    if (avgCheckInput?.value) params.set('avg_check', avgCheckInput.value);
    if (closeRateInput?.value) params.set('close_rate_chat', closeRateInput.value);
    if (marginInput?.value) params.set('gross_margin', marginInput.value);
    if (lossFactorInput?.value) params.set('loss_factor_slow_response', lossFactorInput.value);
    if (workdayStartInput) params.set('workday_start', workdayStartInput.value || '');
    if (workdayEndInput) params.set('workday_end', workdayEndInput.value || '');
    const weekendDays = weekendInputs.filter((input) => input.checked).map((input) => input.value).join(',');
    if (weekendInputs.length) params.set('weekend_days', weekendDays);
    return params;
  }

  async function loadOauthStatus() {
    if (!tenant) return;
    try {
      const params = new URLSearchParams();
      params.set('tenant', tenant);
      if (key) params.set('k', key);
      const data = await fetchJSON(`/v1/oauth/avito-analytics/status?${params.toString()}`);
      if (oauthStatus) {
        const label = data.connected ? 'Подключено' : 'Не подключено';
        oauthStatus.textContent = `Статус: ${label}`;
        oauthStatus.className = `status-text ${data.connected ? 'ok' : 'warn'}`;
      }
    } catch (err) {
      if (oauthStatus) {
        oauthStatus.textContent = 'Статус: ошибка';
        oauthStatus.className = 'status-text alert';
      }
    }
  }

  async function fetchJSON(url) {
    const resp = await fetch(url);
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || resp.statusText);
    }
    return resp.json();
  }

  function formatValue(value) {
    if (value === null || value === undefined || value === '') return '—';
    if (typeof value === 'number') {
      return value.toLocaleString('ru-RU', { maximumFractionDigits: 2 });
    }
    return value;
  }

  function renderCards(summary) {
    if (!kpiCards) return;
    kpiCards.innerHTML = '';
    const cards = [
      { label: 'Просмотры', value: summary.views },
      { label: 'Диалоги', value: summary.chats },
      { label: 'Звонки', value: summary.calls },
      { label: 'Неотвеченные', value: summary.unanswered },
      { label: 'SLA breach', value: summary.sla_breach },
      { label: 'Расходы', value: summary.spend },
      { label: 'Стоимость клиента', value: summary.client_cost },
      {
        label: 'Ночная/выходная нагрузка',
        value: summary.night_weekend_share !== undefined && summary.night_weekend_share !== null
          ? `${summary.night_weekend_share}%`
          : null,
      },
    ];
    cards.forEach((card) => {
      const el = document.createElement('div');
      el.className = 'surface stack';
      el.style.gap = '6px';
      el.innerHTML = `<div class="section-subtitle">${card.label}</div><div class="section-title" style="font-size:1.6rem;">${formatValue(card.value)}</div>`;
      kpiCards.appendChild(el);
    });
  }

  function formatDuration(seconds) {
    if (seconds === null || seconds === undefined || seconds === '') return '—';
    const value = Number(seconds);
    if (!Number.isFinite(value)) return '—';
    if (value < 60) return `${Math.round(value)} сек`;
    if (value < 3600) return `${Math.round(value / 60)} мин`;
    return `${(value / 3600).toFixed(1)} ч`;
  }

  function renderMessagingMetrics(messaging, summary) {
    if (!messagingMetrics) return;
    const stats = messaging.stats || {};
    const chatShare = summary.chat_share_percent;
    const callShare = summary.call_share_percent;
    const cards = [
      { label: 'Среднее время ответа', value: formatDuration(stats.avg_response_sec) },
      { label: 'Среднее время первого ответа', value: formatDuration(messaging.sla?.avg_first_response_sec) },
      {
        label: 'Диалоги / звонки',
        value:
          chatShare !== undefined && callShare !== undefined
            ? `${chatShare}% / ${callShare}%`
            : '—',
      },
      {
        label: 'Среднее сообщений в диалоге',
        value: stats.avg_messages_per_chat !== undefined ? stats.avg_messages_per_chat : '—',
      },
      {
        label: 'Сообщений в рабочее время',
        value:
          stats.incoming_in_work_hours !== undefined
            ? `${stats.incoming_in_work_hours} (${stats.incoming_in_work_hours_share ?? 0}%)`
            : '—',
      },
      { label: 'Время в диалогах (оценка)', value: formatDuration(stats.active_time_sec) },
    ];
    messagingMetrics.innerHTML = '';
    cards.forEach((card) => {
      const el = document.createElement('div');
      el.className = 'surface stack';
      el.style.gap = '6px';
      el.innerHTML = `<div class="section-subtitle">${card.label}</div><div class="section-title" style="font-size:1.3rem;">${formatValue(card.value)}</div>`;
      messagingMetrics.appendChild(el);
    });
  }

  function renderHourly(series) {
    if (!hourlyChartEl) return;
    const labels = Array.from({ length: 24 }, (_, idx) => String(idx).padStart(2, '0'));
    const data = Array.isArray(series) && series.length === 24 ? series : new Array(24).fill(0);
    if (hourlyChart) hourlyChart.destroy();
    hourlyChart = new Chart(hourlyChartEl.getContext('2d'), {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {
            label: 'Сообщения',
            data,
            backgroundColor: 'rgba(59, 130, 246, 0.6)',
          },
        ],
      },
      options: {
        responsive: true,
        scales: {
          y: {
            beginAtZero: true,
            ticks: { precision: 0 },
          },
        },
        plugins: {
          legend: { display: false },
        },
      },
    });
  }

  function renderFunnel(funnel) {
    if (!funnelBlock) return;
    funnelBlock.innerHTML = '';
    const views = funnel.views ?? 0;
    const chats = funnel.chats ?? 0;
    const calls = funnel.calls ?? 0;
    const rows = [
      `👁 ${views} просмотров`,
      `💬 ${chats} диалогов (${views ? Math.round((chats / views) * 100) : 0}%)`,
      `📞 ${calls} звонков (${chats ? Math.round((calls / chats) * 100) : 0}%)`,
    ];
    rows.forEach((text) => {
      const div = document.createElement('div');
      div.className = 'status-text';
      div.textContent = text;
      funnelBlock.appendChild(div);
    });
  }

  function renderLosses(losses) {
    if (!lossesBlock) return;
    lossesBlock.innerHTML = '';
    const rows = [
      { label: 'Неотвеченные диалоги', count: losses.unanswered_leads, money: losses.revenue_at_risk_unanswered },
      { label: 'Долгий ответ (SLA)', count: losses.slow_response_leads, money: losses.revenue_at_risk_slow },
    ];
    rows.forEach((row) => {
      const el = document.createElement('div');
      el.className = 'surface surface--muted';
      el.style.padding = '10px';
      el.innerHTML = `<div class="section-title" style="font-size:1rem;">${row.label}</div>
        <div class="section-subtitle">Кол-во: ${row.count ?? '—'}</div>
        <div class="section-subtitle">Оценка: ${row.money ?? '—'}</div>`;
      lossesBlock.appendChild(el);
    });
  }

  function renderItems(items) {
    if (!itemsTableBody) return;
    itemsTableBody.innerHTML = '';
    const query = (itemsSearch?.value || '').toLowerCase();
    const filtered = items.filter((it) => String(it.title || '').toLowerCase().includes(query));
    filtered.forEach((row) => {
      const tr = document.createElement('tr');
      const cells = [
        row.title || row.id || '',
        row.status || '',
        row.views ?? '',
        row.contacts ?? '',
        row.calls ?? '',
        row.created_at || '',
      ];
      cells.forEach((cell) => {
        const td = document.createElement('td');
        td.textContent = cell;
        tr.appendChild(td);
      });
      itemsTableBody.appendChild(tr);
    });
  }

  function renderHeatmap(grid) {
    if (!heatmapTable) return;
    heatmapTable.innerHTML = '';
    const header = document.createElement('tr');
    header.innerHTML = '<th>День/час</th>' + Array.from({ length: 24 }).map((_, i) => `<th>${i}</th>`).join('');
    heatmapTable.appendChild(header);
    const max = Math.max(1, ...grid.flat());
    const days = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];
    grid.forEach((row, idx) => {
      const tr = document.createElement('tr');
      const label = document.createElement('td');
      label.textContent = days[idx] || '';
      tr.appendChild(label);
      row.forEach((val) => {
        const td = document.createElement('td');
        const intensity = Math.round((val / max) * 100);
        td.style.background = `rgba(255, 153, 0, ${intensity / 100})`;
        td.style.color = intensity > 60 ? '#000' : '#333';
        td.textContent = val ? String(val) : '';
        tr.appendChild(td);
      });
      heatmapTable.appendChild(tr);
    });
  }

  function renderTrend(series) {
    const labels = series.map((row) => row.date);
    const views = series.map((row) => row.views || 0);
    const chats = series.map((row) => row.contacts || 0);
    const ctx = document.getElementById('trend-chart');
    if (!ctx) return;
    if (trendChart) trendChart.destroy();
    trendChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'Просмотры', data: views, borderColor: '#1e293b', tension: 0.3 },
          { label: 'Диалоги', data: chats, borderColor: '#f97316', tension: 0.3 },
        ],
      },
      options: {
        responsive: true,
        plugins: { legend: { display: true } },
        scales: { y: { beginAtZero: true } },
      },
    });
  }

  async function loadReport(force = false) {
    if (!tenant) {
      setStatus('tenant не указан', 'alert');
      return;
    }
    setStatus('Загружаем…', 'muted');
    persistParams();
    const params = buildParams(force);
    try {
      const data = await fetchJSON(`/v1/analytics/avito/report?${params.toString()}`);
      const report = data.report || {};
      renderCards(report.summary || {});
      renderMessagingMetrics(report.messaging || {}, report.summary || {});
      renderFunnel(report.funnel || {});
      renderLosses(report.losses || {});
      cachedItems = (report.listings || {}).items || [];
      renderItems(cachedItems);
      renderHeatmap(((report.messaging || {}).heatmap) || []);
      renderHourly(((report.messaging || {}).stats || {}).incoming_by_hour || []);
      renderTrend(((report.stats || {}).series) || []);
      rawPre.textContent = JSON.stringify(report.raw || {}, null, 2);
      lastUpdated.textContent = `Обновлено: ${new Date().toLocaleTimeString()}`;
      const warnings = report.meta?.warnings || [];
      if (warnings.length) {
        setStatus(`Предупреждения: ${warnings.join(' · ')}`, 'warn');
      } else {
        setStatus('Готово', 'ok');
      }
    } catch (err) {
      console.error(err);
      setStatus('Не удалось загрузить данные', 'alert');
    }
  }

  function bindEvents() {
    refreshBtn?.addEventListener('click', () => loadReport(true));
    [periodSelect, slaSelect, modeSelect].forEach((el) =>
      el?.addEventListener('change', () => loadReport())
    );
    [avgCheckInput, closeRateInput, marginInput, lossFactorInput, workdayStartInput, workdayEndInput].forEach(
      (el) => el?.addEventListener('change', () => loadReport())
    );
    weekendInputs.forEach((input) => input.addEventListener('change', () => loadReport()));
    itemsSearch?.addEventListener('input', () => renderItems(cachedItems));
    connectBtn?.addEventListener('click', async () => {
      if (!tenant) return;
      try {
        const params = new URLSearchParams();
        params.set('tenant', tenant);
        if (key) params.set('k', key);
        const data = await fetchJSON(`/v1/oauth/avito-analytics/authorize?${params.toString()}`);
        const url = data.authorize_url || data.url;
        if (!url) throw new Error('authorize_url missing');
        window.open(url, 'avito-analytics-oauth', 'width=640,height=760,noopener=yes,noreferrer=yes');
      } catch (err) {
        setStatus('Не удалось начать авторизацию', 'alert');
      }
    });
  }

  bindEvents();
  loadOauthStatus();
  loadReport();
  setInterval(() => loadReport(), 5 * 60 * 1000);
})();
