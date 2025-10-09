;(function () {
  const config = (window.__tgConnectConfig || {});
  const tenant = String(config.tenant || '').trim();
  const key = String(config.key || '').trim();
  if (!tenant || !key) {
    console.warn('[tg-connect] missing tenant or key');
    return;
  }

  const statusChip = document.getElementById('tg-status');
  const statusMessage = document.getElementById('tg-status-message');
  const qrImg = document.getElementById('tg-qr');
  const qrFallback = document.getElementById('tg-qr-fallback');
  const refreshBtn = document.getElementById('tg-refresh');

  let pollTimer = null;
  let currentQrId = '';
  let authorized = false;
  let loading = false;

  function buildUrl(path, params) {
    const search = new URLSearchParams({ tenant, k: key, ...params });
    return `${path}?${search.toString()}`;
  }

  function schedulePoll(delayMs) {
    if (pollTimer) window.clearTimeout(pollTimer);
    if (authorized) return;
    const base = 3000 + Math.random() * 2000;
    const timeout = typeof delayMs === 'number' ? delayMs : base;
    pollTimer = window.setTimeout(pollStatus, timeout);
  }

  function setStatus(status, message) {
    if (!statusChip) return;
    const normalized = (status || '').toLowerCase();
    if (normalized === 'authorized') {
      statusChip.textContent = 'Подключено';
    } else if (normalized) {
      statusChip.textContent = `Статус: ${normalized}`;
    } else {
      statusChip.textContent = 'Статус: неизвестен';
    }
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
      qrImg.removeAttribute('src');
      qrImg.style.display = 'none';
    }
    if (qrFallback) {
      qrFallback.textContent = message || 'QR генерируется…';
      qrFallback.style.display = '';
    }
  }

  function showQr(qrId) {
    if (!qrImg) return;
    currentQrId = qrId;
    qrImg.src = buildUrl('/pub/tg/qr.png', { qr_id: qrId, t: Date.now() });
    qrImg.style.display = 'block';
    if (qrFallback) qrFallback.style.display = 'none';
  }

  function applyStatus(data) {
    const rawStatus = data && data.status ? String(data.status) : '';
    const normalized = rawStatus.toLowerCase();
    const needs2fa = normalized === 'needs_2fa' || Boolean(data && data.twofa_pending);

    if (normalized === 'authorized' && !needs2fa) {
      authorized = true;
      setStatus('authorized', 'Подключено. Можно закрыть страницу.');
      hideQr('Подключено');
      if (refreshBtn) refreshBtn.disabled = true;
      if (pollTimer) window.clearTimeout(pollTimer);
      return;
    }

    if (needs2fa) {
      setStatus('needs_2fa', 'Введите пароль двухфакторной аутентификации в Telegram.');
      hideQr('Откройте Telegram и введите пароль двухфакторной аутентификации.');
      return;
    }

    if (normalized === 'waiting_qr' || !normalized) {
      const qrId = data && data.qr_id ? String(data.qr_id) : '';
      if (qrId && qrId !== currentQrId) {
        showQr(qrId);
      } else if (!qrId && !currentQrId && !loading) {
        requestStart(true);
      }
      setStatus('waiting_qr', 'Отсканируйте QR в Telegram → Settings → Devices.');
    } else {
      if (normalized === 'disconnected' && data && data.last_error === 'qr_login_timeout') {
        setStatus('disconnected', 'QR-код истёк. Получите новый код.');
      } else {
        setStatus(normalized, 'Ожидаем ответ от Telegram…');
      }
    }
  }

  async function requestStart(force) {
    if (loading) return;
    loading = true;
    if (force) currentQrId = '';
    hideQr('QR генерируется…');
    setStatus('waiting_qr', 'Готовим новый QR-код…');

    try {
      const resp = await fetch(buildUrl('/pub/tg/start'), {
        method: 'POST',
        cache: 'no-store',
      });
      if (!resp.ok) throw new Error(`start failed: ${resp.status}`);
      const data = await resp.json();
      const qrId = data && data.qr_id ? String(data.qr_id) : '';
      if (qrId) {
        showQr(qrId);
      }
      applyStatus(data);
    } catch (err) {
      console.error('[tg-connect] start error', err);
      setStatus('offline', 'Не удалось запросить QR. Попробуйте позже.');
      hideQr('Сервис Telegram недоступен.');
    } finally {
      loading = false;
      if (!authorized) schedulePoll();
    }
  }

  async function pollStatus() {
    try {
      const resp = await fetch(buildUrl('/pub/tg/status'), { cache: 'no-store' });
      if (!resp.ok) throw new Error(`status failed: ${resp.status}`);
      const data = await resp.json();
      applyStatus(data);
    } catch (err) {
      console.error('[tg-connect] status error', err);
      if (!authorized) {
        setStatus('offline', 'Сервис временно недоступен.');
      }
    } finally {
      if (!authorized) schedulePoll();
    }
  }

  async function handleQrError() {
    if (authorized) return;
    const currentSrc = qrImg ? qrImg.currentSrc || qrImg.src || '' : '';
    if (!currentSrc) {
      requestStart(true);
      return;
    }
    try {
      const resp = await fetch(currentSrc, { cache: 'no-store' });
      if (resp.status === 404 || resp.status === 410) {
        let data = null;
        try {
          data = await resp.clone().json();
        } catch (jsonErr) {
          data = null;
        }
        if (data && data.error === 'qr_expired') {
          requestStart(true);
          return;
        }
      }
    } catch (err) {
      console.warn('[tg-connect] qr fetch error', err);
    }
    if (!loading) {
      setStatus('offline', 'Не удалось загрузить QR. Попробуйте обновить.');
      hideQr('QR недоступен. Обновите страницу.');
      schedulePoll(3000);
    }
  }

  if (qrImg) {
    qrImg.addEventListener('error', handleQrError);
  }

  if (refreshBtn) {
    refreshBtn.addEventListener('click', function () {
      requestStart(true);
    });
  }

  requestStart(false);
})();
