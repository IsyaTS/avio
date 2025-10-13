;(function () {
  const POLL_INTERVAL = 1800;

  let autoBootstrapped = false;

  function init(rawConfig) {
    const providedConfig = rawConfig && typeof rawConfig === 'object' ? rawConfig : {};
    const globalConfig =
      window.__tgConnectConfig && typeof window.__tgConnectConfig === 'object'
        ? window.__tgConnectConfig
        : {};
    const config = { ...providedConfig, ...globalConfig };
    const tenantValue = globalConfig.tenant !== undefined ? globalConfig.tenant : config.tenant;
    const keyValue = globalConfig.key !== undefined ? globalConfig.key : config.key;
    const urls = config.urls && typeof config.urls === 'object' ? config.urls : {};
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

    const tenantNumber = Number(tenant);
    const tenantIdPayload = Number.isFinite(tenantNumber) && !Number.isNaN(tenantNumber) ? tenantNumber : tenant;

    let pollTimer = null;
    let pollInFlight = false;
    let startInFlight = false;
    let authorized = false;
    let inTwoFA = false;
    let lastQrId = '';
    let lastStatusValue = '';

    function truthyFlag(value) {
      if (value === true) {
        return true;
      }
      if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase();
        if (!normalized) {
          return false;
        }
        return normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on';
      }
      if (typeof value === 'number') {
        return Number.isFinite(value) && value !== 0;
      }
      return false;
    }

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
      return buildUrl(qrUrlBase, { qr_id: qrId, t: Date.now() });
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
        refreshButton.disabled = startInFlight || inTwoFA;
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
      updateControls();
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
      lastStatusValue = statusValue;
      const lastError = typeof data.last_error === 'string' ? data.last_error : '';
      const qrIdValue = data.qr_id !== undefined && data.qr_id !== null ? String(data.qr_id) : '';
      const normalizedQrId = qrIdValue.trim();
      const twofaPending = truthyFlag(data.twofa_pending);
      const needsTwofa =
        statusValue === 'needs_2fa' || truthyFlag(data.needs_2fa) || twofaPending;
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

      if (needsTwofa) {
        inTwoFA = true;
        updateControls();
        hideQrBlock();
        lastQrId = '';
        clearQrImage();
        const message =
          lastError === 'invalid_2fa_password' ? 'Неверный пароль. Попробуйте ещё раз.' : '';
        showTwofa(message);
        setPlaceholder('Введите пароль 2FA.');
        setStatus('Нужен пароль 2FA', 'alert');
        return Math.max(POLL_INTERVAL, 4000);
      }

      if (twofaTimeout) {
        handleTwofaTimeout();
        return Math.max(POLL_INTERVAL, 5000);
      }

      inTwoFA = false;
      updateControls();
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
      if (inTwoFA) {
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
        if (inTwoFA) {
          showTwofa('Введите пароль 2FA.');
          setStatus('Введите пароль 2FA.', 'alert');
          return;
        }
        if (twofaPassword) {
          twofaPassword.value = '';
        }
        hideTwofa();
        updateControls();
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
            body: JSON.stringify({ tenant_id: tenantIdPayload, password: rawPassword }),
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
            inTwoFA = true;
            updateControls();
            showTwofa('');
            setStatus('Пароль принят. Ждём подтверждения…', 'muted');
            setPlaceholder('Ждём подтверждение 2FA…');
            if (!authorized) {
              scheduleNext(1200);
            }
            return;
          }
          const errorData = payload && typeof payload === 'object' ? payload : null;
          const errorCode = errorData && typeof errorData.error === 'string' ? errorData.error : '';
          const normalizedCode = typeof errorCode === 'string' ? errorCode.toUpperCase() : '';
          if (normalizedCode === 'PASSWORD_HASH_INVALID' || errorCode === 'invalid_2fa_password') {
            inTwoFA = true;
            updateControls();
            showTwofa('Неверный пароль. Попробуйте ещё раз.');
            setStatus('Неверный пароль. Попробуйте ещё раз.', 'alert');
            if (!authorized) {
              scheduleNext(Math.max(4000, POLL_INTERVAL));
            }
            return;
          }
          if (normalizedCode === 'TWOFA_TIMEOUT' || errorCode === 'twofa_timeout') {
            handleTwofaTimeout();
            return;
          }
          if (normalizedCode === 'TWO_FACTOR_PENDING' || errorCode === 'two_factor_pending') {
            inTwoFA = true;
            updateControls();
            showTwofa('Введите пароль 2FA.');
            setStatus('Нужен пароль 2FA', 'alert');
            if (!authorized) {
              scheduleNext(Math.max(4000, POLL_INTERVAL));
            }
            return;
          }
          if (normalizedCode === 'TWO_FACTOR_NOT_PENDING' || errorCode === 'two_factor_not_pending') {
            if (errorData && typeof errorData === 'object') {
              const processedDelay = processStatus(errorData);
              if (!authorized && typeof processedDelay === 'number' && Number.isFinite(processedDelay)) {
                scheduleNext(processedDelay);
              }
            }
            if (!authorized) {
              handleTwofaTimeout();
            }
            return;
          }
          if (normalizedCode === 'PASSWORD_REQUIRED' || errorCode === 'password_required') {
            showTwofa('Введите пароль.');
            setStatus('Введите пароль 2FA.', 'alert');
            return;
          }
          if (normalizedCode === 'PASSWORD_FLOOD' || errorCode === 'password_flood') {
            const retryAfter = errorData && typeof errorData.retry_after === 'number' && Number.isFinite(errorData.retry_after)
              ? Math.max(1, Math.floor(errorData.retry_after))
              : null;
            const waitMessage = retryAfter
              ? `Слишком много попыток. Попробуйте ещё раз через ${retryAfter} с.`
              : 'Слишком много попыток. Попробуйте позже.';
            inTwoFA = true;
            updateControls();
            showTwofa(waitMessage);
            setStatus(waitMessage, 'alert');
            if (!authorized) {
              const delayMs = retryAfter ? Math.max(600, retryAfter * 1000) : Math.max(5000, POLL_INTERVAL);
              scheduleNext(delayMs);
            }
            return;
          }
          if (normalizedCode === 'SRP_ID_INVALID') {
            inTwoFA = true;
            updateControls();
            showTwofa('Сеанс устарел. Обновите QR-код и попробуйте ещё раз.');
            setStatus('Сеанс устарел. Обновите QR-код и попробуйте ещё раз.', 'alert');
            if (!authorized) {
              scheduleNext(Math.max(4000, POLL_INTERVAL));
            }
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
    setPlaceholder('Проверяем статус…');

    (async () => {
      try {
        await pollStatus();
      } catch (err) {
        console.error('[tg-connect] initial status error', err);
      } finally {
        if (!authorized && !inTwoFA && !lastQrId && lastStatusValue !== 'waiting_qr') {
          startSession(false);
        }
      }
    })();
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
