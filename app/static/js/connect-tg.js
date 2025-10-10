;(function () {
  const config = window.__tgConnectConfig || {};
  const tenantValue = config.tenant;
  const keyValue = config.key;
  const urls = config.urls || {};
  const tenant = tenantValue === undefined || tenantValue === null ? '' : String(tenantValue).trim();
  const key = typeof keyValue === 'string' ? keyValue.trim() : keyValue === undefined || keyValue === null ? '' : String(keyValue).trim();

  const statusEl = document.getElementById('tg-status');
  const qrImage = document.getElementById('tg-qr-image');
  const qrPlaceholder = document.getElementById('tg-qr-placeholder');
  const refreshButton = document.getElementById('tg-qr-refresh');
  const twofaBlock = document.getElementById('tg-2fa-block');
  const twofaForm = document.getElementById('tg-2fa-form');
  const twofaPassword = document.getElementById('tg-2fa-password');
  const twofaSubmit = document.getElementById('tg-2fa-submit');
  const twofaError = document.getElementById('tg-2fa-error');

  if (!tenant || !key) {
    if (statusEl) {
      statusEl.textContent = 'Нет доступа: отсутствует ключ подключения.';
      statusEl.className = 'status-text alert';
    }
    if (refreshButton) {
      refreshButton.disabled = true;
    }
    if (twofaPassword) {
      twofaPassword.disabled = true;
    }
    if (twofaSubmit) {
      twofaSubmit.disabled = true;
    }
    return;
  }

  const baseParams = new URLSearchParams();
  baseParams.set('tenant', tenant);
  baseParams.set('k', key);

  let pollTimer = null;
  let pollInFlight = false;
  let startInFlight = false;
  let awaitingExpiryRecovery = false;
  let currentQrId = '';
  let qrValidUntilMs = 0;
  let authorized = false;

  function buildUrl(base, extra) {
    let target;
    try {
      target = new URL(base || '/', window.location.origin);
    } catch (err) {
      target = new URL('/', window.location.origin);
    }
    baseParams.forEach((value, name) => {
      if (value !== undefined && value !== null) {
        target.searchParams.set(name, value);
      }
    });
    if (extra) {
      Object.entries(extra).forEach(([name, value]) => {
        if (value === undefined || value === null || value === '') {
          return;
        }
        target.searchParams.set(name, String(value));
      });
    }
    return target.toString();
  }

  function buildQrSrc(qrId) {
    const base = urls.tg_qr_png || '/pub/tg/qr.png';
    let target;
    try {
      target = new URL(base, window.location.origin);
    } catch (err) {
      target = new URL('/pub/tg/qr.png', window.location.origin);
    }
    target.searchParams.set('qr_id', String(qrId));
    target.searchParams.set('t', String(Date.now()));
    return target.toString();
  }

  function setStatus(text, variant) {
    if (!statusEl) {
      return;
    }
    const classes = ['status-text'];
    if (variant === 'success') {
      classes.push('success');
    } else if (variant === 'alert') {
      classes.push('alert');
    } else {
      classes.push('muted');
    }
    statusEl.className = classes.join(' ');
    statusEl.textContent = text || '';
  }

  function showPlaceholder(text) {
    if (qrPlaceholder) {
      qrPlaceholder.textContent = text || '';
      qrPlaceholder.style.display = '';
    }
    if (qrImage) {
      qrImage.removeAttribute('src');
      qrImage.style.display = 'none';
    }
  }

  function showQr(qrId) {
    const clean = String(qrId || '').trim();
    if (!clean) {
      showPlaceholder('QR генерируется…');
      return;
    }
    currentQrId = clean;
    awaitingExpiryRecovery = false;
    if (qrImage) {
      qrImage.src = buildQrSrc(clean);
      qrImage.style.display = '';
    }
    if (qrPlaceholder) {
      qrPlaceholder.style.display = 'none';
    }
  }

  function hideQr(message) {
    if (qrImage) {
      qrImage.removeAttribute('src');
      qrImage.style.display = 'none';
    }
    if (qrPlaceholder) {
      if (message) {
        qrPlaceholder.textContent = message;
        qrPlaceholder.style.display = '';
      } else {
        qrPlaceholder.style.display = 'none';
      }
    }
  }

  function hideTwofa() {
    if (twofaBlock) {
      twofaBlock.style.display = 'none';
    }
    if (twofaError) {
      twofaError.textContent = '';
      twofaError.style.display = 'none';
    }
  }

  function showTwofa(message) {
    if (twofaBlock) {
      twofaBlock.style.display = '';
    }
    if (twofaError) {
      if (message) {
        twofaError.textContent = message;
        twofaError.style.display = '';
      } else {
        twofaError.textContent = '';
        twofaError.style.display = 'none';
      }
    }
    if (twofaPassword) {
      twofaPassword.focus();
    }
  }

  function stopPoll() {
    if (pollTimer) {
      window.clearTimeout(pollTimer);
      pollTimer = null;
    }
  }

  function schedulePoll(delay) {
    if (authorized) {
      stopPoll();
      return;
    }
    stopPoll();
    const fallback = 3000;
    const ms = typeof delay === 'number' && Number.isFinite(delay) ? Math.max(500, delay) : fallback;
    pollTimer = window.setTimeout(() => {
      pollStatus();
    }, ms);
  }

  function parseValidUntil(value) {
    const num = Number(value);
    if (!Number.isFinite(num) || num <= 0) {
      return 0;
    }
    return num > 1e12 ? num : Math.round(num * 1000);
  }

  function maybeTriggerExpiryStart(status, lastError) {
    const normalizedStatus = (status || '').toLowerCase();
    const normalizedError = (lastError || '').toLowerCase();
    const now = Date.now();
    const expiredByTime = qrValidUntilMs > 0 && now > qrValidUntilMs;
    const expiredByStatus = normalizedStatus === 'qr_expired' || normalizedStatus === 'qr_login_timeout';
    const expiredByError = normalizedError === 'qr_expired' || normalizedError === 'qr_login_timeout';
    const expired = expiredByTime || expiredByStatus || expiredByError;
    if (!expired) {
      awaitingExpiryRecovery = false;
      return;
    }
    if (awaitingExpiryRecovery || startInFlight) {
      return;
    }
    awaitingExpiryRecovery = true;
    startSession(true, 'expiry');
  }

  function handleStatus(data) {
    const result = { continue: true, delay: 3000 };
    const payload = data && typeof data === 'object' ? data : {};
    const rawStatus = typeof payload.status === 'string' ? payload.status : '';
    const status = rawStatus.toLowerCase();
    const lastError = typeof payload.last_error === 'string' ? payload.last_error : '';
    const qrId = payload.qr_id ? String(payload.qr_id) : '';
    const needsTwofa = Boolean(payload.needs_2fa) || Boolean(payload.twofa_pending) || status === 'needs_2fa' || status === 'twofa_pending';

    if ('qr_valid_until' in payload) {
      const parsed = parseValidUntil(payload.qr_valid_until);
      if (parsed > 0) {
        if (qrValidUntilMs !== parsed) {
          qrValidUntilMs = parsed;
          awaitingExpiryRecovery = false;
        }
      }
    }

    if (qrId && qrId !== currentQrId) {
      showQr(qrId);
    } else if (!qrId && !needsTwofa && !currentQrId) {
      showPlaceholder('QR генерируется…');
    }

    if (needsTwofa) {
      hideQr('QR скрыт до ввода пароля 2FA.');
      showTwofa('');
      setStatus('Требуется пароль двухфакторной аутентификации.', 'alert');
      result.delay = 2500;
      maybeTriggerExpiryStart(status, lastError);
      return result;
    }

    hideTwofa();

    if (status === 'authorized') {
      authorized = true;
      stopPoll();
      setStatus('Подключено', 'success');
      hideQr('Подключено');
      return { continue: false, delay: 0 };
    }

    authorized = false;

    if (status === 'waiting_qr') {
      if (qrId) {
        showQr(qrId);
      } else {
        showPlaceholder('QR генерируется…');
      }
      setStatus('Отсканируйте QR в Telegram → Настройки → Устройства.', 'muted');
      result.delay = 2500;
    } else if (
      status === 'qr_expired' ||
      status === 'qr_login_timeout' ||
      lastError.toLowerCase() === 'qr_expired' ||
      lastError.toLowerCase() === 'qr_login_timeout'
    ) {
      currentQrId = '';
      hideQr('QR истёк. Нажмите «Обновить QR».');
      setStatus('QR истёк. Получите новый код.', 'alert');
      result.delay = 3000;
    } else if (status === 'disconnected') {
      currentQrId = '';
      hideQr('Сессия отключена. Нажмите «Обновить QR».');
      setStatus('Сессия отключена.', 'alert');
      result.delay = 4000;
    } else if (status) {
      setStatus(`Статус: ${status}`, 'muted');
      result.delay = 3500;
    } else {
      setStatus('Ожидаем QR…', 'muted');
      result.delay = 3000;
    }

    maybeTriggerExpiryStart(status, lastError);
    return result;
  }

  function applyStatusAndSchedule(data) {
    const outcome = handleStatus(data);
    if (outcome.continue && !authorized) {
      schedulePoll(outcome.delay);
    } else {
      stopPoll();
    }
  }

  async function startSession(force, origin) {
    if (startInFlight) {
      return;
    }
    const autoExpiry = origin === 'expiry';
    if (!autoExpiry) {
      awaitingExpiryRecovery = false;
    }
    startInFlight = true;
    if (refreshButton) {
      refreshButton.disabled = true;
    }
    if (force) {
      currentQrId = '';
    }
    if (!authorized) {
      showPlaceholder(force ? 'Готовим новый QR…' : 'Запрашиваем QR…');
      setStatus('Запрашиваем QR…', 'muted');
    }

    const extra = { t: Date.now() };
    if (force) {
      extra.force = '1';
    }

    try {
      const response = await fetch(buildUrl(urls.tg_start || '/pub/tg/start', extra), {
        method: 'GET',
        cache: 'no-store',
      });
      let payload = null;
      if (response.status !== 204) {
        try {
          payload = await response.json();
        } catch (err) {
          payload = null;
        }
      }
      if (!response.ok) {
        throw new Error(`tg_start_${response.status}`);
      }
      applyStatusAndSchedule(payload || {});
    } catch (error) {
      console.error('[tg-connect] start error', error);
      if (!autoExpiry) {
        awaitingExpiryRecovery = false;
      }
      showPlaceholder('Не удалось запросить QR. Попробуйте позже.');
      setStatus('Не удалось запросить QR. Попробуйте позже.', 'alert');
      if (!authorized) {
        schedulePoll(5000);
      }
    } finally {
      startInFlight = false;
      if (refreshButton) {
        refreshButton.disabled = false;
      }
    }
  }

  async function pollStatus() {
    if (pollInFlight) {
      return;
    }
    pollInFlight = true;
    try {
      const response = await fetch(buildUrl(urls.tg_status || '/pub/tg/status', { t: Date.now() }), {
        method: 'GET',
        cache: 'no-store',
      });
      if (!response.ok) {
        throw new Error(`tg_status_${response.status}`);
      }
      let payload = null;
      if (response.status !== 204) {
        try {
          payload = await response.json();
        } catch (err) {
          payload = null;
        }
      }
      applyStatusAndSchedule(payload || {});
    } catch (error) {
      console.error('[tg-connect] status error', error);
      if (!authorized) {
        setStatus('Сервис Telegram временно недоступен.', 'alert');
        showPlaceholder('Сервис Telegram временно недоступен.');
        schedulePoll(5000);
      }
    } finally {
      pollInFlight = false;
    }
  }

  if (refreshButton) {
    refreshButton.addEventListener('click', () => {
      startSession(true, 'manual');
    });
  }

  if (qrImage) {
    qrImage.addEventListener('error', () => {
      if (!authorized) {
        currentQrId = '';
        showPlaceholder('Не удалось загрузить QR. Нажмите «Обновить QR».');
      }
    });
  }

  if (twofaForm) {
    twofaForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      if (!twofaPassword) {
        return;
      }
      const password = twofaPassword.value.trim();
      if (!password) {
        showTwofa('Введите пароль.');
        return;
      }
      if (twofaSubmit) {
        twofaSubmit.disabled = true;
      }
      try {
        const response = await fetch(buildUrl(urls.tg_password || '/pub/tg/password', { t: Date.now() }), {
          method: 'POST',
          cache: 'no-store',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ password }),
        });
        if (response.status === 400) {
          let message = 'Неверный пароль. Попробуйте ещё раз.';
          try {
            const errorBody = await response.json();
            if (errorBody && typeof errorBody === 'object') {
              if (errorBody.error === 'invalid_password') {
                message = 'Неверный пароль. Попробуйте ещё раз.';
              } else if (errorBody.detail) {
                message = String(errorBody.detail);
              }
            }
          } catch (err) {
            // ignore
          }
          showTwofa(message);
          return;
        }
        if (!response.ok) {
          throw new Error(`tg_password_${response.status}`);
        }
        showTwofa('');
        twofaPassword.value = '';
        setStatus('Пароль отправлен. Проверяем статус…', 'muted');
        pollStatus();
      } catch (error) {
        console.error('[tg-connect] password error', error);
        showTwofa('Не удалось отправить пароль. Попробуйте ещё раз.');
      } finally {
        if (twofaSubmit) {
          twofaSubmit.disabled = false;
        }
      }
    });
  }

  startSession(false, 'initial');
})();
