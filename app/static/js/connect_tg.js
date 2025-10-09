(function () {
  const stateEl = document.getElementById('tg-connect-state');
  if (!stateEl) return;

  let config = {};
  try {
    config = JSON.parse(stateEl.textContent || '{}');
  } catch (err) {
    console.error('[tg-connect] failed to parse state', err);
    return;
  }

  const tenantId = config.tenant;
  const accessKey = config.key;
  const query = config.query || {};
  const tenantQuery = query.tenant !== undefined && query.tenant !== null
    ? String(query.tenant)
    : (tenantId !== undefined && tenantId !== null ? String(tenantId) : '');
  const keyQuery = query.k !== undefined && query.k !== null
    ? String(query.k)
    : (accessKey !== undefined && accessKey !== null ? String(accessKey) : '');
  const urls = config.urls || {};
  if (!tenantQuery || !keyQuery || !urls.start || !urls.status || !urls.qr) {
    console.warn('[tg-connect] missing config');
    return;
  }

  const statusChip = document.getElementById('tg-status');
  const statusMessage = document.getElementById('tg-status-message');
  const qrImg = document.getElementById('tg-qr');
  const qrFallback = document.getElementById('tg-qr-fallback');
  const refreshBtn = document.getElementById('tg-refresh');

  let pollTimer = null;
  let currentQrId = '';
  let loadingQr = false;
  let authorized = false;

  function withQuery(base, params) {
    const connector = base.includes('?') ? '&' : '?';
    const query = Object.entries(params)
      .filter(([, value]) => value !== undefined && value !== null)
      .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`)
      .join('&');
    return `${base}${connector}${query}`;
  }

  function setStatus(status, message) {
    if (!statusChip) return;
    const normalized = (status || '').toLowerCase();
    const label = normalized ? `Статус: ${normalized}` : 'Статус: неизвестен';
    statusChip.textContent = label;
    statusChip.classList.remove('waiting', 'authorized', 'offline', 'needs-2fa');
    if (normalized === 'authorized') statusChip.classList.add('authorized');
    else if (normalized === 'waiting_qr') statusChip.classList.add('waiting');
    else if (normalized === 'needs_2fa') statusChip.classList.add('needs-2fa');
    else statusChip.classList.add('offline');

    if (statusMessage) {
      if (message) {
        statusMessage.textContent = message;
        statusMessage.style.display = '';
      } else {
        statusMessage.style.display = 'none';
      }
    }
  }

  function hideQr(message) {
    if (qrImg) {
      qrImg.style.display = 'none';
      qrImg.removeAttribute('src');
    }
    if (qrFallback) {
      qrFallback.textContent = message || 'QR генерируется…';
      qrFallback.style.display = '';
    }
  }

  function showQr(qrId) {
    if (!qrImg) return;
    currentQrId = qrId;
    const src = withQuery(urls.qr, {
      tenant: tenantQuery,
      k: keyQuery,
      qr_id: qrId,
    });
    qrImg.src = src;
    qrImg.style.display = '';
    if (qrFallback) qrFallback.style.display = 'none';
  }

  function schedulePoll(delay) {
    if (pollTimer) window.clearTimeout(pollTimer);
    const timeout = typeof delay === 'number' ? delay : 3500;
    pollTimer = window.setTimeout(pollStatus, timeout);
  }

  async function requestNewQr(force) {
    if (loadingQr) return;
    loadingQr = true;
    if (force) currentQrId = '';
    hideQr('QR генерируется…');
    setStatus('waiting_qr', 'Готовим новый QR-код…');

    const url = withQuery(urls.start, { tenant: tenantQuery, k: keyQuery });
    try {
      const resp = await fetch(url, {
        method: 'POST',
        cache: 'no-store',
      });
      if (!resp.ok) throw new Error(`start failed: ${resp.status}`);
      const data = await resp.json();
      const upstreamStatus = data && data.status ? String(data.status) : '';
      if (upstreamStatus) {
        applyStatus(data);
        if (authorized) return;
      }
      const qrId = data && data.qr_id ? String(data.qr_id) : '';
      if (qrId) {
        showQr(qrId);
        const normalizedStatus = upstreamStatus.toLowerCase();
        if (!upstreamStatus || normalizedStatus === 'waiting_qr') {
          setStatus(upstreamStatus || 'waiting_qr', 'Откройте Telegram → Settings → Devices → Link Desktop Device.');
        }
      } else if (!upstreamStatus || normalizedStatus === 'waiting_qr') {
        hideQr('QR недоступен. Попробуйте обновить позже.');
        setStatus('offline', 'Не удалось получить QR. Попробуйте обновить.');
      }
    } catch (err) {
      console.error('[tg-connect] start error', err);
      hideQr('Сервис Telegram недоступен.');
      setStatus('offline', 'Не удалось запросить QR. Попробуйте позже.');
    } finally {
      loadingQr = false;
      schedulePoll(2000);
    }
  }

  function applyStatus(data) {
    if (refreshBtn) refreshBtn.disabled = false;
    const rawStatus = data && data.status ? String(data.status) : '';
    let status = rawStatus.toLowerCase();
    const needs2fa = Boolean(data && data.needs_2fa);

    if (status === 'authorized' && !needs2fa) {
      authorized = true;
      hideQr('Аккаунт подключён. Можно закрыть страницу.');
      setStatus('authorized', 'Подключено. Можно закрыть страницу.');
      if (refreshBtn) refreshBtn.disabled = true;
      return;
    }

    if (needs2fa) {
      status = 'needs_2fa';
    }

    let message = 'Проверяем статус…';
    if (status === 'waiting_qr') {
      message = 'Отсканируйте QR в Telegram → Settings → Devices.';
    } else if (status === 'needs_2fa') {
      message = 'Введите пароль двухфакторной аутентификации в Telegram.';
    } else if (!status) {
      message = 'Проверяем статус…';
    } else {
      message = 'Ожидаем ответ от Telegram…';
    }
    setStatus(status || 'waiting_qr', message);

    const upstreamQr = data && data.qr_id ? String(data.qr_id) : '';
    if (status === 'waiting_qr') {
      if (upstreamQr && upstreamQr !== currentQrId) {
        showQr(upstreamQr);
      } else if (!upstreamQr && !currentQrId && !loadingQr) {
        requestNewQr(true);
      }
    } else if (status === 'needs_2fa') {
      hideQr('Откройте Telegram и введите пароль двухфакторной аутентификации.');
    }
  }

  async function pollStatus() {
    const url = withQuery(urls.status, {
      tenant: tenantQuery,
      k: keyQuery,
    });
    try {
      const resp = await fetch(url, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`status failed: ${resp.status}`);
      const data = await resp.json();
      applyStatus(data);
    } catch (err) {
      console.error('[tg-connect] status error', err);
      if (!authorized) {
        setStatus('offline', 'Сервис временно недоступен.');
      }
    } finally {
      if (!authorized) schedulePoll(3000 + Math.random() * 2000);
    }
  }

  function handleQrError() {
    if (authorized) return;
    console.warn('[tg-connect] qr load failed, requesting new code');
    requestNewQr(true);
  }

  if (qrImg) {
    qrImg.addEventListener('error', handleQrError);
  }

  if (refreshBtn) {
    refreshBtn.addEventListener('click', function () {
      requestNewQr(true);
    });
  }

  requestNewQr(false);
})();
