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

  const tenant = config.tenant;
  const key = config.key;
  const urls = config.urls || {};
  if (!tenant || !key || !urls.status || !urls.qr || !urls.logout) {
    console.warn('[tg-connect] missing config');
    return;
  }

  const statusChip = document.getElementById('tg-status');
  const qrImg = document.getElementById('tg-qr');
  const qrMessage = document.getElementById('tg-qr-message');
  const startBtn = document.getElementById('tg-start');
  const logoutBtn = document.getElementById('tg-logout');

  let pollTimer = null;
  let currentQr = '';

  function setStatus(status, extra) {
    if (!statusChip) return;
    statusChip.textContent = status ? `Статус: ${status}` : 'Статус неизвестен';
    statusChip.classList.remove('offline', 'waiting', 'authorized');
    const normalized = (status || '').toLowerCase();
    if (normalized === 'authorized') statusChip.classList.add('authorized');
    else if (normalized === 'waiting_qr') statusChip.classList.add('waiting');
    else statusChip.classList.add('offline');
    if (extra && qrMessage) {
      qrMessage.style.display = '';
      qrMessage.textContent = extra;
    }
  }

  function showQr(src) {
    if (!qrImg) return;
    qrImg.src = src;
    qrImg.style.display = '';
  }

  function hideQr(message) {
    if (qrImg) qrImg.style.display = 'none';
    if (qrMessage) {
      if (message) {
        qrMessage.textContent = message;
        qrMessage.style.display = '';
      } else {
        qrMessage.style.display = 'none';
      }
    }
  }

  function schedulePoll(delay) {
    if (pollTimer) window.clearTimeout(pollTimer);
    pollTimer = window.setTimeout(pollStatus, typeof delay === 'number' ? delay : 3000);
  }

  async function loadQr(qrId) {
    if (!qrId) return;
    if (currentQr === qrId && qrImg && qrImg.src) return;
    currentQr = qrId;
    const url = `${urls.qr}?tenant=${encodeURIComponent(tenant)}&k=${encodeURIComponent(key)}&qr_id=${encodeURIComponent(qrId)}&t=${Date.now()}`;
    try {
      const resp = await fetch(url, { cache: 'no-store' });
      if (resp.status === 200) {
        const blob = await resp.blob();
        const objectUrl = URL.createObjectURL(blob);
        showQr(objectUrl);
        hideQr('');
      } else if (resp.status === 204) {
        hideQr('QR генерируется…');
      } else {
        hideQr('QR недоступен. Попробуйте позже.');
      }
    } catch (err) {
      console.error('[tg-connect] qr fetch failed', err);
      hideQr('Не удалось получить QR.');
    }
  }

  async function pollStatus() {
    const url = `${urls.status}?tenant=${encodeURIComponent(tenant)}&k=${encodeURIComponent(key)}&t=${Date.now()}`;
    try {
      const resp = await fetch(url, { cache: 'no-store' });
      const data = await resp.json();
      const status = data.status || data.State || '';
      setStatus(status, data.needs_2fa ? 'Требуется ввод пароля 2FA' : '');
      if (status === 'waiting_qr' && data.qr_id) {
        await loadQr(String(data.qr_id));
      } else if (status === 'authorized') {
        hideQr('Аккаунт подключён');
      } else if (!data.qr_id) {
        hideQr('QR не готов');
      }
    } catch (err) {
      console.error('[tg-connect] status error', err);
      setStatus('offline', 'Сервис временно недоступен');
    } finally {
      schedulePoll(3000);
    }
  }

  async function logout() {
    const url = `${urls.logout}?tenant=${encodeURIComponent(tenant)}&k=${encodeURIComponent(key)}`;
    try {
      await fetch(url, { method: 'POST', cache: 'no-store' });
    } catch (err) {
      console.error('[tg-connect] logout failed', err);
    } finally {
      currentQr = '';
      hideQr('QR не готов');
      schedulePoll(500);
    }
  }

  if (startBtn) startBtn.addEventListener('click', () => schedulePoll(0));
  if (logoutBtn) logoutBtn.addEventListener('click', logout);

  schedulePoll(0);
})();
