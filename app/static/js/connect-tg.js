;(function () {
  const POLL_INTERVAL = 1800;

  let autoBootstrapped = false;

  function init(rawConfig) {
    const config = rawConfig || window.__tgConnectConfig || {};
    const tenantValue = config.tenant;
    const keyValue = config.key;
    const urls = config.urls || {};
    const startUrlBase = urls.tg_start || urls.start || '/pub/tg/start';
    const statusUrlBase = urls.tg_status || urls.status || '/pub/tg/status';
    const qrUrlBase = urls.tg_qr_png || urls.qr || '/pub/tg/qr.png';
    const passwordUrlBase = urls.tg_password || urls.password || '/pub/tg/password';

    const tenant = tenantValue === undefined || tenantValue === null ? '' : String(tenantValue).trim();
    const key = keyValue === undefined || keyValue === null ? '' : String(keyValue).trim();

    const statusEl = document.getElementById('tg-status');
    const qrBlock = document.getElementById('tg-qr-block');
    const qrImage = document.getElementById('tg-qr-image');
    const qrPlaceholder = document.getElementById('tg-qr-placeholder');
    const refreshButton = document.getElementById('tg-qr-refresh');
    const twofaBlock = document.getElementById('tg-2fa-block');
    const twofaForm = document.getElementById('tg-2fa-form');
    const twofaPassword = document.getElementById('tg-2fa-password');
    const twofaError = document.getElementById('tg-2fa-error');
    const twofaSubmit = document.getElementById('tg-2fa-submit');
    const newQrBlock = document.getElementById('tg-new-qr-block');
    const newQrMessage = document.getElementById('tg-new-qr-message');
    const newQrButton = document.getElementById('tg-new-qr-button');

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
    let qrValidUntilMs = 0;
    let authorized = false;
    let lastQrId = '';
    let inTwoFA = false;
    let manualRestartAllowed = true;

    function buildUrl(basePath, extraParams) {
      let target;
      try {
        target = new URL(basePath || '/', window.location.origin);
      } catch (err) {
        target = new URL('/', window.location.origin);
        if (typeof basePath === 'string' && basePath) {
          target.pathname = basePath;
        }
      }
      baseParams.forEach((value, name) => {
        target.searchParams.set(name, value);
      });
      if (extraParams && typeof extraParams === 'object') {
        Object.entries(extraParams).forEach(([name, value]) => {
          if (value === undefined || value === null || value === '') {
            return;
          }
          target.searchParams.set(name, String(value));
        });
      }
      return target.toString();
    }

    function buildQrSrc(qrId) {
      let target;
      try {
        target = new URL(qrUrlBase, window.location.origin);
      } catch (err) {
        target = new URL('/pub/tg/qr.png', window.location.origin);
      }
      target.searchParams.set('qr_id', String(qrId));
      target.searchParams.set('t', String(Date.now()));
      return target.toString();
    }

    function stopPolling() {
      if (pollTimer) {
        window.clearTimeout(pollTimer);
        pollTimer = null;
      }
    }

    function scheduleNext(delay) {
      if (authorized) {
        stopPolling();
        return;
      }
      stopPolling();
      const ms = typeof delay === 'number' && Number.isFinite(delay) ? Math.max(500, delay) : POLL_INTERVAL;
      pollTimer = window.setTimeout(() => {
        pollStatus();
      }, ms);
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

    function updateControls() {
      if (refreshButton) {
        refreshButton.disabled = startInFlight || inTwoFA;
      }
      if (newQrButton) {
        newQrButton.disabled = startInFlight || !manualRestartAllowed;
      }
    }

    function clearQrImage() {
      if (qrImage) {
        qrImage.removeAttribute('src');
        qrImage.style.display = 'none';
      }
    }

    function showQrBlock() {
      if (qrBlock) {
        qrBlock.style.display = '';
      }
    }

    function hideQrBlock() {
      if (qrBlock) {
        qrBlock.style.display = 'none';
      }
    }

    function showPlaceholder(text, resetId) {
      if (resetId) {
        lastQrId = '';
      }
      clearQrImage();
      if (qrPlaceholder) {
        qrPlaceholder.textContent = text || '';
        qrPlaceholder.style.display = text ? '' : 'none';
      }
      hideNewQr();
      showQrBlock();
    }

    function updateQrImage(qrId) {
      const normalized = typeof qrId === 'string' ? qrId.trim() : String(qrId || '').trim();
      if (!normalized) {
        showPlaceholder('QR генерируется…', true);
        return;
      }
      if (normalized !== lastQrId) {
        lastQrId = normalized;
        if (qrImage) {
          qrImage.src = buildQrSrc(normalized);
        }
      }
      if (qrImage) {
        qrImage.style.display = '';
      }
      if (qrPlaceholder) {
        qrPlaceholder.style.display = 'none';
      }
      hideNewQr();
      showQrBlock();
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
      hideNewQr();
      if (twofaPassword) {
        window.setTimeout(() => {
          try {
            twofaPassword.focus();
          } catch (err) {
            /* noop */
          }
        }, 0);
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

    function showNewQr(message) {
      if (newQrMessage) {
        if (message) {
          newQrMessage.textContent = message;
          newQrMessage.style.display = '';
        } else {
          newQrMessage.textContent = '';
          newQrMessage.style.display = 'none';
        }
      }
      if (newQrBlock) {
        newQrBlock.style.display = '';
      }
    }

    function hideNewQr() {
      if (newQrBlock) {
        newQrBlock.style.display = 'none';
      }
      if (newQrMessage) {
        newQrMessage.textContent = '';
        newQrMessage.style.display = 'none';
      }
    }

    function parseValidUntil(value) {
      const num = Number(value);
      if (!Number.isFinite(num) || num <= 0) {
        return 0;
      }
      return num > 1e12 ? Math.round(num) : Math.round(num * 1000);
    }

    function handleStatus(payload) {
      const outcome = { continuePolling: true, forceRefresh: false, forceReason: '', pollDelay: POLL_INTERVAL };
      const data = payload && typeof payload === 'object' ? payload : {};
      const status = typeof data.status === 'string' ? data.status : '';
      const lastError = typeof data.last_error === 'string' ? data.last_error : '';
      const qrId = data.qr_id !== undefined && data.qr_id !== null ? String(data.qr_id) : '';
      const needsTwofa = status === 'needs_2fa' || data.needs_2fa === true || data.twofa_pending === true;
      const waitingQr = status === 'waiting_qr';
      const canRestart = data.can_restart === true;
      const twofaTimeout = status === 'twofa_timeout' || lastError === 'twofa_timeout';

      if ('qr_valid_until' in data) {
        const parsed = parseValidUntil(data.qr_valid_until);
        if (parsed > 0) {
          qrValidUntilMs = parsed;
        }
      }

      if (status === 'authorized') {
        authorized = true;
        inTwoFA = false;
        manualRestartAllowed = true;
        updateControls();
        hideNewQr();
        setStatus('Подключено', 'success');
        hideQrBlock();
        hideTwofa();
        return { continuePolling: false, forceRefresh: false, forceReason: '', pollDelay: POLL_INTERVAL };
      }

      authorized = false;

      if (needsTwofa) {
        manualRestartAllowed = false;
        inTwoFA = true;
        updateControls();
        lastQrId = '';
        clearQrImage();
        if (qrPlaceholder) {
          qrPlaceholder.textContent = '';
          qrPlaceholder.style.display = 'none';
        }
        hideQrBlock();
        const twofaMessage = lastError === 'invalid_password' ? 'Неверный пароль. Попробуйте ещё раз.' : '';
        showTwofa(twofaMessage);
        setStatus('Требуется пароль 2FA', 'alert');
        outcome.pollDelay = Math.max(4000, POLL_INTERVAL);
        return outcome;
      }

      const twofaWindowExpired = twofaTimeout || (waitingQr && inTwoFA);
      if (twofaWindowExpired) {
        manualRestartAllowed = !!canRestart;
        inTwoFA = true;
        updateControls();
        lastQrId = '';
        clearQrImage();
        if (qrPlaceholder) {
          qrPlaceholder.textContent = '';
          qrPlaceholder.style.display = 'none';
        }
        hideQrBlock();
        hideTwofa();
        if (twofaPassword) {
          twofaPassword.value = '';
        }
        const message = canRestart
          ? 'Срок ожидания 2FA истёк. Получите новый QR-код.'
          : 'Срок ожидания 2FA истёк. Обратитесь к администратору для перезапуска.';
        showNewQr(message);
        setStatus('Срок ожидания пароля 2FA истёк. Нажмите «Новый QR».', 'alert');
        outcome.pollDelay = Math.max(6000, POLL_INTERVAL);
        return outcome;
      }

      manualRestartAllowed = true;
      inTwoFA = false;
      updateControls();
      hideTwofa();
      hideNewQr();

      if (qrId) {
        updateQrImage(qrId);
      } else {
        showPlaceholder('QR генерируется…', true);
      }

      if (status === 'waiting_qr') {
        setStatus('ждём сканирования', 'muted');
      } else if (status === 'disconnected') {
        showPlaceholder('Сессия отключена. Нажмите «Обновить QR».', true);
        setStatus(lastError || 'Сессия отключена. Нажмите «Обновить QR».', 'alert');
      } else if (status === 'qr_expired' || status === 'qr_login_timeout') {
        showPlaceholder('QR истёк. Обновляем…', true);
        setStatus('QR истёк. Получаем новый…', 'alert');
      } else if (status) {
        setStatus(`Статус: ${status}`, 'muted');
      } else {
        setStatus('Ожидаем статус…', 'muted');
      }

      const now = Date.now();
      const expiredByTime = qrValidUntilMs > 0 && now >= qrValidUntilMs;
      const expiredByStatus = status === 'qr_expired' || status === 'qr_login_timeout';
      const expiredByError = lastError === 'qr_expired' || lastError === 'qr_login_timeout';

      if (expiredByTime || expiredByStatus || expiredByError) {
        outcome.forceRefresh = true;
        outcome.forceReason = expiredByTime ? 'qr_valid_until' : 'status';
      }

      return outcome;
    }

    function applyOutcome(outcome) {
      if (!outcome) {
        if (!authorized) {
          scheduleNext(POLL_INTERVAL);
        } else {
          stopPolling();
        }
        return;
      }
      if (outcome.forceRefresh && !startInFlight && !inTwoFA) {
        startSession(true, outcome.forceReason || 'status');
        return;
      }
      if (outcome.continuePolling && !authorized) {
        scheduleNext(outcome.pollDelay);
      } else {
        stopPolling();
      }
    }

    async function startSession(force, origin) {
      if (startInFlight) {
        return;
      }
      if (inTwoFA && !force) {
        return;
      }
      startInFlight = true;
      updateControls();
      stopPolling();
      if (!authorized) {
        showPlaceholder(force ? 'Готовим новый QR…' : 'Готовим QR-код…', true);
        setStatus('Запрашиваем QR…', 'muted');
      }

      const extra = { t: Date.now() };
      if (force) {
        extra.force = '1';
      }

      let outcome = null;
      let errorOccurred = false;

      try {
        const response = await fetch(buildUrl(startUrlBase, extra), {
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
        outcome = handleStatus(payload || {});
      } catch (error) {
        errorOccurred = true;
        console.error('[tg-connect] start error', error);
        setStatus('Не удалось запросить QR. Попробуйте позже.', 'alert');
        showPlaceholder('Не удалось запросить QR. Попробуйте позже.', true);
      } finally {
        startInFlight = false;
        updateControls();
      }

      if (errorOccurred) {
        if (!authorized) {
          scheduleNext(4000);
        }
        return;
      }

      applyOutcome(outcome);
    }

    async function pollStatus() {
      if (pollInFlight || startInFlight || authorized) {
        return;
      }
      pollInFlight = true;
      stopPolling();

      let outcome = null;
      let errorOccurred = false;

      try {
        const response = await fetch(buildUrl(statusUrlBase, { t: Date.now() }), {
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
          throw new Error(`tg_status_${response.status}`);
        }
        outcome = handleStatus(payload || {});
      } catch (error) {
        errorOccurred = true;
        console.error('[tg-connect] status error', error);
        if (!authorized) {
          setStatus('Сервис Telegram временно недоступен.', 'alert');
          showPlaceholder('Сервис Telegram временно недоступен.', false);
        }
      } finally {
        pollInFlight = false;
      }

      if (errorOccurred) {
        if (!authorized) {
          scheduleNext(4000);
        }
        return;
      }

      applyOutcome(outcome);
    }

    if (refreshButton) {
      refreshButton.addEventListener('click', () => {
        startSession(true, 'manual');
      });
    }

    if (newQrButton) {
      newQrButton.addEventListener('click', () => {
        if (startInFlight) {
          return;
        }
        inTwoFA = false;
        if (twofaPassword) {
          twofaPassword.value = '';
        }
        hideTwofa();
        hideNewQr();
        updateControls();
        showPlaceholder('Готовим новый QR…', true);
        startSession(true, 'twofa-reset');
      });
    }

    if (qrImage) {
      qrImage.addEventListener('error', () => {
        if (!authorized) {
          showPlaceholder('Не удалось загрузить QR. Нажмите «Обновить QR».', true);
        }
      });
    }

    if (twofaForm) {
      twofaForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        if (!twofaPassword) {
          return;
        }
        const rawPassword = twofaPassword.value || '';
        if (!rawPassword.trim()) {
          manualRestartAllowed = false;
          inTwoFA = true;
          updateControls();
          showTwofa('Введите пароль.');
          return;
        }
        if (twofaSubmit) {
          twofaSubmit.disabled = true;
        }
        try {
          const response = await fetch(buildUrl(passwordUrlBase, { t: Date.now() }), {
            method: 'POST',
            cache: 'no-store',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ password: rawPassword }),
          });
          let payload = null;
          try {
            payload = await response.json();
          } catch (err) {
            payload = null;
          }
          if (response.ok) {
            manualRestartAllowed = true;
            inTwoFA = false;
            updateControls();
            hideTwofa();
            hideNewQr();
            clearQrImage();
            hideQrBlock();
            twofaPassword.value = '';
            setStatus('Подключено', 'success');
            pollStatus();
            return;
          }
          const errorData = payload && typeof payload === 'object' ? payload : null;
          const errorCode = errorData && typeof errorData.error === 'string' ? errorData.error : '';
          if (errorCode === 'invalid_password') {
            manualRestartAllowed = false;
            inTwoFA = true;
            updateControls();
            showTwofa('Неверный пароль. Попробуйте ещё раз.');
            setStatus('Неверный пароль. Попробуйте ещё раз.', 'alert');
            return;
          }
          if (errorCode === 'twofa_timeout') {
            manualRestartAllowed = true;
            inTwoFA = true;
            updateControls();
            hideTwofa();
            twofaPassword.value = '';
            showNewQr('Срок ожидания 2FA истёк. Получите новый QR-код.');
            setStatus('Срок ожидания пароля 2FA истёк. Нажмите «Новый QR».', 'alert');
            return;
          }
          if (errorCode === 'password_required') {
            manualRestartAllowed = false;
            inTwoFA = true;
            updateControls();
            showTwofa('Введите пароль.');
            return;
          }
          if (errorData && typeof errorData.detail === 'string') {
            manualRestartAllowed = false;
            inTwoFA = true;
            updateControls();
            showTwofa(errorData.detail);
            return;
          }
          manualRestartAllowed = false;
          inTwoFA = true;
          updateControls();
          showTwofa('Не удалось отправить пароль. Попробуйте ещё раз.');
        } catch (error) {
          console.error('[tg-connect] password error', error);
          manualRestartAllowed = false;
          inTwoFA = true;
          updateControls();
          showTwofa('Не удалось отправить пароль. Попробуйте ещё раз.');
        } finally {
          if (twofaSubmit) {
            twofaSubmit.disabled = false;
          }
        }
      });
    }

    updateControls();
    showPlaceholder('Готовим QR-код…', true);
    startSession(false, 'initial');
  }

  function bootstrapOnce() {
    if (autoBootstrapped) {
      return;
    }
    autoBootstrapped = true;
    init(window.__tgConnectConfig || {});
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bootstrapOnce, { once: true });
  } else {
    bootstrapOnce();
  }

  window.tgConnect = window.tgConnect || {};
  window.tgConnect.init = init;
})();
