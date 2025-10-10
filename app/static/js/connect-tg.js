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
    let authorized = false;
    let inTwoFA = false;
    let lastQrId = '';

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
      const ms = typeof delay === 'number' && Number.isFinite(delay) ? Math.max(600, delay) : POLL_INTERVAL;
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
        refreshButton.disabled = startInFlight;
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

    function clearQrImage() {
      if (qrImage) {
        qrImage.removeAttribute('src');
        qrImage.style.display = 'none';
      }
    }

    function setPlaceholder(text) {
      if (!qrPlaceholder) {
        return;
      }
      if (text) {
        qrPlaceholder.textContent = text;
        qrPlaceholder.style.display = '';
      } else {
        qrPlaceholder.textContent = '';
        qrPlaceholder.style.display = 'none';
      }
    }

    function showQrImage(qrId) {
      const normalized = typeof qrId === 'string' ? qrId.trim() : String(qrId || '').trim();
      if (!normalized) {
        lastQrId = '';
        clearQrImage();
        setPlaceholder('QR генерируется…');
        return;
      }
      if (normalized !== lastQrId && qrImage) {
        lastQrId = normalized;
        qrImage.src = buildQrSrc(normalized);
      }
      if (qrImage) {
        qrImage.style.display = '';
      }
      setPlaceholder('');
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

    function handleTwofaTimeout() {
      inTwoFA = false;
      hideTwofa();
      lastQrId = '';
      clearQrImage();
      showQrBlock();
      setPlaceholder('Срок ожидания 2FA истёк. Нажмите «Обновить QR».');
      setStatus('Срок ожидания 2FA истёк. Нажмите «Обновить QR».', 'alert');
    }

    function processStatus(payload) {
      const data = payload && typeof payload === 'object' ? payload : {};
      const statusValue = typeof data.status === 'string' ? data.status : '';
      const lastError = typeof data.last_error === 'string' ? data.last_error : '';
      const qrIdValue = data.qr_id !== undefined && data.qr_id !== null ? String(data.qr_id) : '';
      const normalizedQrId = qrIdValue.trim();
      const twofaPending = data.twofa_pending === true;
      const needsTwofa =
        statusValue === 'needs_2fa' || data.needs_2fa === true || twofaPending === true;
      const twofaTimeout = statusValue === 'twofa_timeout' || lastError === 'twofa_timeout';
      const errorCode = typeof data.error === 'string' ? data.error : '';
      let nextDelay = POLL_INTERVAL;

      if (statusValue === 'authorized') {
        authorized = true;
        inTwoFA = false;
        hideTwofa();
        hideQrBlock();
        setStatus('Подключено', 'success');
        stopPolling();
        return null;
      }

      authorized = false;

      if (twofaTimeout) {
        handleTwofaTimeout();
        return Math.max(POLL_INTERVAL, 5000);
      }

      if (needsTwofa) {
        inTwoFA = true;
        hideQrBlock();
        lastQrId = '';
        clearQrImage();
        const message =
          lastError === 'invalid_2fa_password' ? 'Неверный пароль. Попробуйте ещё раз.' : '';
        showTwofa(message);
        setStatus('Нужен пароль 2FA', 'alert');
        return Math.max(POLL_INTERVAL, 4000);
      }

      inTwoFA = false;
      hideTwofa();
      showQrBlock();

      if (statusValue === 'waiting_qr') {
        if (normalizedQrId) {
          showQrImage(normalizedQrId);
          setPlaceholder('');
        } else {
          lastQrId = '';
          clearQrImage();
          setPlaceholder('QR генерируется…');
        }
        setStatus('Ждём сканирования', 'muted');
        return nextDelay;
      }

      if (normalizedQrId) {
        showQrImage(normalizedQrId);
      } else {
        lastQrId = '';
        clearQrImage();
        setPlaceholder('QR генерируется…');
      }

      if (statusValue === 'qr_expired' || statusValue === 'qr_login_timeout') {
        lastQrId = '';
        clearQrImage();
        setPlaceholder('QR истёк. Нажмите «Обновить QR».');
        setStatus('QR истёк. Нажмите «Обновить QR».', 'alert');
      } else if (statusValue === 'disconnected') {
        setPlaceholder('Сессия отключена. Нажмите «Обновить QR».');
        setStatus('Сессия отключена. Нажмите «Обновить QR».', 'alert');
      } else if (statusValue) {
        setStatus(`Статус: ${statusValue}`, 'muted');
      } else if (lastError) {
        setStatus(`Ошибка: ${lastError}`, 'alert');
      } else if (errorCode) {
        setStatus(`Ошибка: ${errorCode}`, 'alert');
      } else {
        setStatus('Ожидаем статус…', 'muted');
      }

      return nextDelay;
    }

    async function pollStatus() {
      if (pollInFlight || startInFlight || authorized) {
        return;
      }
      pollInFlight = true;
      stopPolling();
      let scheduled = false;
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
        let nextDelay = POLL_INTERVAL;
        if (payload && typeof payload === 'object') {
          const processedDelay = processStatus(payload);
          if (processedDelay === null) {
            nextDelay = null;
          } else if (typeof processedDelay === 'number' && Number.isFinite(processedDelay)) {
            nextDelay = processedDelay;
          }
        }
        if (!authorized && nextDelay !== null) {
          scheduleNext(nextDelay);
          scheduled = true;
        }
      } catch (error) {
        console.error('[tg-connect] status error', error);
        if (!authorized) {
          setPlaceholder('Сервис Telegram временно недоступен.');
          setStatus('Сервис Telegram временно недоступен.', 'alert');
          scheduleNext(Math.max(POLL_INTERVAL, 5000));
          scheduled = true;
        }
      } finally {
        pollInFlight = false;
        if (!authorized && !scheduled) {
          scheduleNext(POLL_INTERVAL);
        }
      }
    }

    async function startSession(force) {
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
        if (force) {
          lastQrId = '';
        }
        clearQrImage();
        showQrBlock();
        setPlaceholder(force ? 'Запрашиваем новый QR…' : 'Готовим QR-код…');
        setStatus('Запрашиваем QR…', 'muted');
      }

      const extra = { t: Date.now() };
      if (force) {
        extra.force = '1';
      }

      let scheduled = false;
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
        const isConflict = response.status === 409;
        if (!response.ok && !isConflict) {
          throw new Error(`tg_start_${response.status}`);
        }
        let nextDelay = POLL_INTERVAL;
        if (payload && typeof payload === 'object') {
          const processedDelay = processStatus(payload);
          if (processedDelay === null) {
            nextDelay = null;
          } else if (typeof processedDelay === 'number' && Number.isFinite(processedDelay)) {
            nextDelay = processedDelay;
          }
        }
        if (!authorized && nextDelay !== null) {
          scheduleNext(nextDelay);
          scheduled = true;
        }
      } catch (error) {
        console.error('[tg-connect] start error', error);
        if (!authorized) {
          setPlaceholder('Не удалось запросить QR. Попробуйте позже.');
          setStatus('Не удалось запросить QR. Попробуйте позже.', 'alert');
          scheduleNext(Math.max(POLL_INTERVAL, 5000));
          scheduled = true;
        }
      } finally {
        startInFlight = false;
        updateControls();
        if (!authorized && !scheduled) {
          scheduleNext(POLL_INTERVAL);
        }
      }
    }

    if (refreshButton) {
      refreshButton.addEventListener('click', () => {
        inTwoFA = false;
        if (twofaPassword) {
          twofaPassword.value = '';
        }
        hideTwofa();
        lastQrId = '';
        clearQrImage();
        showQrBlock();
        setPlaceholder('Запрашиваем новый QR…');
        startSession(true);
      });
    }

    if (qrImage) {
      qrImage.addEventListener('error', () => {
        if (!authorized) {
          lastQrId = '';
          clearQrImage();
          setPlaceholder('Не удалось загрузить QR. Нажмите «Обновить QR».');
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
          showTwofa('Введите пароль.');
          setStatus('Введите пароль 2FA.', 'alert');
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
          if (response.status !== 204) {
            try {
              payload = await response.json();
            } catch (err) {
              payload = null;
            }
          }
          if (response.ok) {
            twofaPassword.value = '';
            showTwofa('');
            setStatus('Проверяем пароль…', 'muted');
            if (!authorized) {
              scheduleNext(1200);
            }
            return;
          }
          const errorData = payload && typeof payload === 'object' ? payload : null;
          const errorCode = errorData && typeof errorData.error === 'string' ? errorData.error : '';
          if (errorCode === 'invalid_2fa_password') {
            showTwofa('Неверный пароль. Попробуйте ещё раз.');
            setStatus('Неверный пароль. Попробуйте ещё раз.', 'alert');
            if (!authorized) {
              scheduleNext(Math.max(4000, POLL_INTERVAL));
            }
            return;
          }
          if (errorCode === 'twofa_timeout') {
            handleTwofaTimeout();
            return;
          }
          if (errorCode === 'password_required') {
            showTwofa('Введите пароль.');
            setStatus('Введите пароль 2FA.', 'alert');
            return;
          }
          if (errorData && typeof errorData.detail === 'string') {
            showTwofa(errorData.detail);
            setStatus(errorData.detail, 'alert');
            return;
          }
          showTwofa('Не удалось отправить пароль. Попробуйте ещё раз.');
          setStatus('Не удалось отправить пароль. Попробуйте ещё раз.', 'alert');
        } catch (error) {
          console.error('[tg-connect] password error', error);
          showTwofa('Не удалось отправить пароль. Попробуйте ещё раз.');
          setStatus('Не удалось отправить пароль. Попробуйте ещё раз.', 'alert');
        } finally {
          if (twofaSubmit) {
            twofaSubmit.disabled = false;
          }
        }
      });
    }

    updateControls();
    showQrBlock();
    setPlaceholder('Готовим QR-код…');
    startSession(false);
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
