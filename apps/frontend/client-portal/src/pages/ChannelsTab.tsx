import React, { useEffect, useMemo, useState } from 'react';
import toast from 'react-hot-toast';
import { useClient } from '../context/ClientContext';
import { buildUrl, postJson, requestJson } from '../lib/api';
import Hint from '../components/Hint';

const StatusBadge: React.FC<{ state: 'ok' | 'warn' | 'err' | 'idle'; label: string }> = ({
  state,
  label,
}) => {
  const className =
    state === 'ok'
      ? 'badge badge-success'
      : state === 'warn'
      ? 'badge badge-warning'
      : state === 'err'
      ? 'badge badge-danger'
      : 'badge badge-neutral';
  return <span className={className}>{label}</span>;
};

const ChannelsTab: React.FC = () => {
  return (
    <div className="space-y-6">
      <div className="grid gap-6 lg:grid-cols-3">
        <WhatsAppCard />
        <TelegramCard />
        <AvitoCard />
        <MaxCard />
        <MaxPersonalCard />
        <AmoCRMCard />
      </div>
    </div>
  );
};

const WhatsAppCard: React.FC = () => {
  const { api } = useClient();
  const [status, setStatus] = useState('Проверяем статус…');
  const [badge, setBadge] = useState<'ok' | 'warn' | 'err' | 'idle'>('idle');
  const [qrUrl, setQrUrl] = useState<string | null>(null);
  const [polling, setPolling] = useState(false);

  const query = useMemo(() => buildUrl('/pub/wa/status', api), [api]);

  const fetchStatus = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<Record<string, any>>(query);
      const ready = Boolean(data.ready || data.connected || data.state === 'ready');
      const needQr = Boolean(data.need_qr || data.state === 'qr');
      if (ready) {
        setStatus('Подключено');
        setBadge('ok');
        setQrUrl(null);
        return;
      }
      if (needQr) {
        setStatus('Ожидает сканирования');
        setBadge('warn');
        const url = (data.qr_url as string) || buildUrl('/pub/wa/qr.svg', api, { t: Date.now() });
        setQrUrl(url);
        return;
      }
      setStatus(`Статус: ${data.state || 'нет данных'}`);
      setBadge('warn');
      setQrUrl(null);
    } catch (error) {
      setStatus('Статус недоступен');
      setBadge('err');
      setQrUrl(null);
    }
  };

  useEffect(() => {
    fetchStatus().catch(() => undefined);
    if (!api.tenantId || !api.key) return;
    setPolling(true);
    const timer = window.setInterval(() => {
      fetchStatus().catch(() => undefined);
    }, 2000);
    return () => {
      window.clearInterval(timer);
      setPolling(false);
    };
  }, [api.tenantId, api.key]);

  const refreshQr = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      await requestJson(buildUrl('/pub/wa/start', api));
      setQrUrl(buildUrl('/pub/wa/qr.svg', api, { force: 1, t: Date.now() }));
      setStatus('QR обновлён');
      setBadge('warn');
    } catch (error) {
      toast.error('Не удалось обновить QR');
    }
  };

  const copyLink = async () => {
    try {
      await navigator.clipboard.writeText(window.location.href);
      toast.success('Ссылка скопирована');
    } catch {
      toast.error('Не удалось скопировать ссылку');
    }
  };

  return (
    <div className="card space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <div className="card-title">WhatsApp</div>
          <div className="card-subtitle flex items-center gap-2">QR и статус подключения <Hint text="Сканируйте QR в мобильном WhatsApp, чтобы подключить аккаунт к боту." /></div>
        </div>
        <StatusBadge state={badge} label={status} />
      </div>
      <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
        {qrUrl ? (
          <img src={qrUrl} alt="QR WhatsApp" className="mx-auto h-52 w-52 rounded-xl bg-white p-3" />
        ) : (
          <div className="text-center text-sm text-slate-400">QR появится после запроса</div>
        )}
      </div>
      <div className="flex flex-wrap gap-3">
        <button className="btn-secondary" onClick={refreshQr}>
          Обновить QR
        </button>
        <button className="btn-ghost" onClick={copyLink}>
          Скопировать ссылку
        </button>
      </div>
      {polling && <div className="text-xs text-slate-400">Автообновление каждые 2 сек</div>}
    </div>
  );
};

const TelegramCard: React.FC = () => {
  const { api } = useClient();
  const [status, setStatus] = useState('Проверяем статус…');
  const [badge, setBadge] = useState<'ok' | 'warn' | 'err' | 'idle'>('idle');
  const [qrId, setQrId] = useState<string | null>(null);
  const [needs2fa, setNeeds2fa] = useState(false);
  const [password, setPassword] = useState('');
  const [qrSlotsCount, setQrSlotsCount] = useState(1);
  const [selectedSlot, setSelectedSlot] = useState(1);
  const [multiMode, setMultiMode] = useState(true);
  const [slotEnabled, setSlotEnabled] = useState<Record<number, boolean>>({ 1: true, 2: true, 3: true, 4: true, 5: true });
  const [slotsLoaded, setSlotsLoaded] = useState(false);
  const [suppressAutosave, setSuppressAutosave] = useState(false);

  const slotQuery = useMemo(() => ({ slot: selectedSlot }), [selectedSlot]);
  const selectedSlotEnabled = slotEnabled[selectedSlot] ?? true;

  const fetchSlotSettings = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<Record<string, any>>(buildUrl('/pub/tg/slots', api));
      const count = Math.max(1, Math.min(5, Number(data.slot_count) || 1));
      const enabledMap: Record<number, boolean> = { 1: true, 2: true, 3: true, 4: true, 5: true };
      const rawEnabled = (data.slot_enabled as Record<string, any>) || {};
      for (let i = 1; i <= 5; i += 1) {
        enabledMap[i] = rawEnabled[String(i)] !== false;
      }
      setMultiMode(Boolean(data.multi_mode ?? true));
      setQrSlotsCount(count);
      setSlotEnabled(enabledMap);
      const storageKey = api.tenantId ? `tg:selected_slot:${api.tenantId}` : '';
      let preferredSlot = 1;
      if (storageKey) {
        const raw = window.localStorage.getItem(storageKey);
        const parsed = Number(raw || '1');
        preferredSlot = Math.max(1, Math.min(count, Number.isFinite(parsed) ? parsed : 1));
      }
      setSelectedSlot(preferredSlot);
      setSlotsLoaded(true);
    } catch {
      // silent fallback to defaults
      setSlotsLoaded(true);
    }
  };

  const saveSlotSettings = async (quiet = false) => {
    if (!api.tenantId || !api.key) return;
    const payload = {
      multi_mode: multiMode,
      slot_count: qrSlotsCount,
      slot_enabled: {
        1: slotEnabled[1] ?? true,
        2: slotEnabled[2] ?? true,
        3: slotEnabled[3] ?? true,
        4: slotEnabled[4] ?? true,
        5: slotEnabled[5] ?? true,
      },
    };
    try {
      await postJson(buildUrl('/pub/tg/slots', api), payload);
      if (!quiet) toast.success('Настройки слотов сохранены');
      await fetchStatus();
    } catch {
      if (!quiet) toast.error('Не удалось сохранить настройки слотов');
    }
  };

  const fetchStatus = async () => {
    if (!api.tenantId || !api.key) return;
    if (!selectedSlotEnabled) {
      setStatus('Слот выключен');
      setBadge('idle');
      setNeeds2fa(false);
      setQrId(null);
      return;
    }
    try {
      const data = await requestJson<Record<string, any>>(
        buildUrl('/pub/tg/status', api, { _: Date.now(), ...slotQuery }),
      );
      const currentStatus = String(data.state || data.raw_state || data.status || 'unknown');
      if (currentStatus === 'authorized') {
        setStatus('Подключено');
        setBadge('ok');
      } else if (currentStatus === 'waiting_qr') {
        setStatus('Ожидание QR');
        setBadge('warn');
      } else if (currentStatus === 'needs_2fa' || currentStatus === 'need_2fa') {
        setStatus('Нужен пароль 2FA');
        setBadge('warn');
      } else if (currentStatus === 'disconnected') {
        setStatus('Не подключено');
        setBadge('warn');
      } else {
        setStatus('Неизвестно');
        setBadge('warn');
      }
      setNeeds2fa(Boolean(data.needs_2fa || data.twofa_pending));
      setQrId(data.qr_id ? String(data.qr_id) : null);
    } catch (error) {
      setStatus('tg_unavailable');
      setBadge('err');
      setNeeds2fa(false);
      setQrId(null);
    }
  };

  useEffect(() => {
    fetchSlotSettings().catch(() => undefined);
  }, [api.tenantId, api.key]);

  useEffect(() => {
    if (!api.tenantId) return;
    const key = `tg:selected_slot:${api.tenantId}`;
    window.localStorage.setItem(key, String(selectedSlot));
  }, [api.tenantId, selectedSlot]);

  useEffect(() => {
    if (!slotsLoaded || suppressAutosave) return;
    const timer = window.setTimeout(() => {
      saveSlotSettings(true).catch(() => undefined);
    }, 350);
    return () => window.clearTimeout(timer);
  }, [multiMode, qrSlotsCount, slotEnabled, slotsLoaded, suppressAutosave]);

  useEffect(() => {
    fetchStatus().catch(() => undefined);
    if (!api.tenantId || !api.key) return;
    const timer = window.setInterval(() => {
      fetchStatus().catch(() => undefined);
    }, 2500);
    return () => window.clearInterval(timer);
  }, [api.tenantId, api.key, selectedSlot, selectedSlotEnabled]);

  const refreshQr = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      if (!selectedSlotEnabled) {
        toast.error('Слот выключен');
        return;
      }
      await requestJson(buildUrl('/pub/tg/start', api, { force: 1, ...slotQuery }));
      await fetchStatus();
      toast.success('QR обновлён');
    } catch {
      toast.error('Не удалось обновить QR');
    }
  };

  const handleDisconnect = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      await requestJson(buildUrl('/pub/tg/logout', api, slotQuery), { method: 'POST' });
      await fetchStatus();
      toast.success('Telegram отключён');
    } catch {
      toast.error('Не удалось отключить Telegram');
    }
  };

  const submit2fa = async () => {
    if (!password.trim()) {
      toast.error('Введите пароль');
      return;
    }
    try {
      if (!selectedSlotEnabled) {
        toast.error('Слот выключен');
        return;
      }
      await postJson(buildUrl('/pub/tg/2fa', api, slotQuery), { password: password.trim() });
      toast.success('Пароль отправлен');
      setPassword('');
      fetchStatus().catch(() => undefined);
    } catch (error) {
      toast.error('Пароль не принят');
    }
  };

  const qrUrl = qrId
    ? buildUrl('/pub/tg/qr.png', api, { qr_id: qrId, t: Date.now(), ...slotQuery })
    : buildUrl('/pub/tg/qr.png', api, { t: Date.now(), ...slotQuery });

  return (
    <div className="card space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <div className="card-title">Telegram</div>
          <div className="card-subtitle flex items-center gap-2">Подключение через QR или 2FA <Hint text="Подключение Telegram через QR, при необходимости вводится пароль двухфакторной защиты." /></div>
        </div>
        <StatusBadge state={badge} label={status} />
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        <label className="space-y-1">
          <span className="text-xs text-slate-500">Мульти-режим</span>
          <select
            className="input"
            value={multiMode ? 'on' : 'off'}
            onChange={(e) => setMultiMode(e.target.value === 'on')}
          >
            <option value="on">Включён (все активные слоты)</option>
            <option value="off">Выключен (слот 1)</option>
          </select>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-slate-500">Количество QR-слотов</span>
          <select
            className="input"
            value={qrSlotsCount}
            onChange={(e) => {
              setSuppressAutosave(true);
              const count = Math.max(1, Math.min(5, Number(e.target.value) || 1));
              setQrSlotsCount(count);
              if (selectedSlot > count) setSelectedSlot(count);
              window.setTimeout(() => setSuppressAutosave(false), 0);
            }}
          >
            {[1, 2, 3, 4, 5].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-slate-500">Активный QR-слот</span>
          <select
            className="input"
            value={selectedSlot}
            onChange={(e) => setSelectedSlot(Math.max(1, Math.min(qrSlotsCount, Number(e.target.value) || 1)))}
          >
            {Array.from({ length: qrSlotsCount }, (_, i) => i + 1).map((n) => (
              <option key={n} value={n}>
                Слот {n}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
        <div className="mb-2 text-xs font-medium text-slate-600">Статус слотов</div>
        <div className="grid gap-2 sm:grid-cols-2">
          {Array.from({ length: qrSlotsCount }, (_, i) => i + 1).map((slot) => (
            <label key={slot} className="flex items-center justify-between rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm">
              <span>Слот {slot}</span>
              <input
                type="checkbox"
                checked={slotEnabled[slot] ?? true}
                onChange={(e) =>
                  setSlotEnabled((prev) => ({
                    ...prev,
                    [slot]: e.target.checked,
                  }))
                }
              />
            </label>
          ))}
        </div>
      </div>
      <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
        {needs2fa ? (
          <div className="space-y-3">
            <div className="text-sm text-slate-600">Требуется пароль 2FA</div>
            <input
              className="input"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Пароль Telegram"
            />
            <button className="btn" onClick={submit2fa}>
              Отправить
            </button>
          </div>
        ) : (
          <img src={qrUrl} alt="QR Telegram" className="mx-auto h-52 w-52 rounded-xl bg-white p-3" />
        )}
      </div>
      <div className="flex flex-wrap gap-3">
        <button className="btn-secondary" onClick={refreshQr}>
          Обновить QR
        </button>
        <button className="btn-ghost" onClick={handleDisconnect}>
          Отключить
        </button>
      </div>
    </div>
  );
};

const AvitoCard: React.FC = () => {
  const { api } = useClient();
  const [status, setStatus] = useState('Проверяем статус…');
  const [badge, setBadge] = useState<'ok' | 'warn' | 'err' | 'idle'>('idle');
  const [connected, setConnected] = useState(false);
  const [accounts, setAccounts] = useState<Array<Record<string, any>>>([]);
  const [editingAccountId, setEditingAccountId] = useState<string | null>(null);
  const [displayNameDraft, setDisplayNameDraft] = useState('');

  const statusUrl = useMemo(() => buildUrl('/v1/oauth/avito/status', api), [api]);
  const authorizeUrl = useMemo(() => buildUrl('/v1/oauth/avito/authorize', api), [api]);
  const webhookUrl = useMemo(() => buildUrl('/v1/oauth/avito/webhook', api), [api]);
  const disconnectUrl = useMemo(() => buildUrl('/v1/oauth/avito/disconnect', api), [api]);

  const fetchStatus = async (quiet = false) => {
    try {
      const data = await requestJson<Record<string, any>>(statusUrl);
      const isConnected = Boolean(data.connected);
      const accountList = Array.isArray(data.accounts) ? data.accounts : [];
      setAccounts(accountList);
      setConnected(isConnected);
      if (isConnected) {
        setStatus('Подключено');
        setBadge('ok');
      } else {
        setStatus('Не подключено');
        setBadge('warn');
      }
    } catch (error) {
      setStatus('Статус недоступен');
      setBadge('err');
      if (!quiet) toast.error('Не удалось получить статус Avito');
    }
  };

  useEffect(() => {
    if (!api.tenantId || !api.key) return;
    fetchStatus(true).catch(() => undefined);
  }, [api.tenantId, api.key]);

  const ensureWebhook = async () => {
    await requestJson(webhookUrl, { method: 'POST' });
  };

  const connect = async () => {
    const popup = window.open('', 'avito-oauth', 'width=640,height=760');
    if (!popup) {
      toast.error('Разрешите всплывающие окна для подключения Avito');
      return;
    }
    try {
      const data = await requestJson<Record<string, any>>(authorizeUrl);
      const target = data.authorize_url || data.url;
      if (!target) {
        throw new Error('authorize_url missing');
      }
      popup.location.href = String(target);
      try {
        popup.opener = null;
      } catch {
        // Some browsers do not allow changing opener after navigation.
      }
      toast.success('Окно авторизации открыто');
    } catch (error) {
      popup.close();
      toast.error('Не удалось начать авторизацию');
    }
  };

  const disconnect = async () => {
    try {
      await requestJson(disconnectUrl, { method: 'POST' });
      toast.success('Avito отключён');
      fetchStatus(true).catch(() => undefined);
    } catch {
      toast.error('Не удалось отключить Avito');
    }
  };

  const accountUrl = (accountId: unknown, action: 'primary' | 'disconnect' | 'webhook') =>
    buildUrl(`/v1/oauth/avito/accounts/${accountId}/${action}`, api);

  const accountRenameUrl = (accountId: unknown) => buildUrl(`/v1/oauth/avito/accounts/${accountId}/rename`, api);

  const makePrimary = async (accountId: unknown) => {
    try {
      await requestJson(accountUrl(accountId, 'primary'), { method: 'POST' });
      toast.success('Основной аккаунт обновлён');
      fetchStatus(true).catch(() => undefined);
    } catch {
      toast.error('Не удалось сделать аккаунт основным');
    }
  };

  const verifyAccountWebhook = async (accountId: unknown) => {
    try {
      await requestJson(accountUrl(accountId, 'webhook'), { method: 'POST' });
      toast.success('Webhook проверен');
    } catch {
      toast.error('Не удалось проверить webhook');
    }
  };

  const disconnectAccount = async (accountId: unknown) => {
    try {
      await requestJson(accountUrl(accountId, 'disconnect'), { method: 'POST' });
      toast.success('Аккаунт отключён');
      fetchStatus(true).catch(() => undefined);
    } catch {
      toast.error('Не удалось отключить аккаунт');
    }
  };

  const startRename = (account: Record<string, any>) => {
    setEditingAccountId(String(account.account_id || ''));
    setDisplayNameDraft(String(account.display_name || account.account_login || '').trim());
  };

  const saveRename = async (accountId: unknown) => {
    try {
      await postJson(accountRenameUrl(accountId), { display_name: displayNameDraft.trim() });
      toast.success('Название сохранено');
      setEditingAccountId(null);
      setDisplayNameDraft('');
      fetchStatus(true).catch(() => undefined);
    } catch {
      toast.error('Не удалось сохранить название');
    }
  };

  return (
    <div className="card space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <div className="card-title">Avito</div>
          <div className="card-subtitle flex items-center gap-2">OAuth подключение аккаунта <Hint text="Авторизует Avito аккаунт и проверяет, что вебхук получает новые сообщения." /></div>
        </div>
        <StatusBadge state={badge} label={status} />
      </div>
      <div className="flex flex-wrap gap-3">
        <button className="btn" onClick={connect}>
          {connected ? 'Подключить ещё аккаунт' : 'Подключить Avito'}
        </button>
        <button className="btn-secondary" onClick={() => fetchStatus()}>
          Проверить статус
        </button>
        {connected && (
          <button className="btn-ghost" onClick={disconnect}>
            Отключить
          </button>
        )}
      </div>
      <div className="space-y-2">
        {accounts.length === 0 ? (
          <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-500">
            Не подключено
          </div>
        ) : (
          accounts.map((account) => {
            const accountId = account.account_id;
            const editActive = editingAccountId === String(accountId || '');
            const title = account.display_name || account.account_login || `ID ${String(accountId || '').slice(-6)}`;
            return (
              <div key={String(accountId)} className="rounded-lg border border-slate-200 bg-white p-3">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    {editActive ? (
                      <div className="flex max-w-xs items-center gap-2">
                        <input
                          className="input h-9"
                          value={displayNameDraft}
                          autoFocus
                          maxLength={120}
                          placeholder="Название аккаунта"
                          onChange={(event) => setDisplayNameDraft(event.target.value)}
                          onKeyDown={(event) => {
                            if (event.key === 'Enter') saveRename(accountId).catch(() => undefined);
                            if (event.key === 'Escape') setEditingAccountId(null);
                          }}
                        />
                        <button className="btn-secondary h-9 px-3" onClick={() => saveRename(accountId)}>
                          Сохранить
                        </button>
                      </div>
                    ) : (
                      <div className="text-sm font-semibold text-slate-900">{title}</div>
                    )}
                    <div className="text-xs text-slate-500">
                      ID {String(accountId || '').slice(-8)}
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    {account.is_primary && <span className="badge badge-success">Основной</span>}
                    <span className="badge badge-neutral">{account.status || 'active'}</span>
                  </div>
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  {!account.is_primary && account.status === 'active' && (
                    <button className="btn-ghost" onClick={() => makePrimary(accountId)}>
                      Сделать основным
                    </button>
                  )}
                  {account.status === 'active' && (
                    <button className="btn-ghost" onClick={() => verifyAccountWebhook(accountId)}>
                      Проверить webhook
                    </button>
                  )}
                  {!editActive && (
                    <button className="btn-ghost" onClick={() => startRename(account)}>
                      Переименовать
                    </button>
                  )}
                  <button className="btn-ghost text-red-600" onClick={() => disconnectAccount(accountId)}>
                    Отключить
                  </button>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
};

const MaxCard: React.FC = () => {
  const { api } = useClient();
  const [status, setStatus] = useState('Проверяем статус…');
  const [badge, setBadge] = useState<'ok' | 'warn' | 'err' | 'idle'>('idle');
  const [connected, setConnected] = useState(false);
  const [token, setToken] = useState('');
  const [saving, setSaving] = useState(false);

  const fetchStatus = async (quiet = false) => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<Record<string, any>>(buildUrl('/v1/max/status', api, { _: Date.now() }));
      const isConnected = Boolean(data.connected);
      setConnected(isConnected);
      if (isConnected) {
        setStatus(data.webhook_registered ? 'Подключено' : 'Токен сохранён');
        setBadge(data.webhook_registered ? 'ok' : 'warn');
      } else {
        setStatus('Не подключено');
        setBadge('warn');
      }
    } catch (error) {
      setStatus('Статус недоступен');
      setBadge('err');
      if (!quiet) toast.error('Не удалось получить статус MAX');
    }
  };

  useEffect(() => {
    fetchStatus(true).catch(() => undefined);
  }, [api.tenantId, api.key]);

  const handleConnect = async () => {
    if (!token.trim()) {
      toast.error('Введите токен MAX');
      return;
    }
    setSaving(true);
    try {
      await postJson(buildUrl('/v1/max/connect', api), { token: token.trim() });
      toast.success('MAX подключён');
      setToken('');
      fetchStatus(true).catch(() => undefined);
    } catch (error) {
      toast.error('Не удалось подключить MAX');
    } finally {
      setSaving(false);
    }
  };

  const handleDisconnect = async () => {
    setSaving(true);
    try {
      await postJson(buildUrl('/v1/max/disconnect', api), {});
      toast.success('MAX отключён');
      fetchStatus(true).catch(() => undefined);
    } catch (error) {
      toast.error('Не удалось отключить MAX');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="card space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <div className="card-title">MAX</div>
          <div className="card-subtitle flex items-center gap-2">Официальный бот по токену <Hint text="Канал MAX работает через официальный Bot API и токен бота." /></div>
        </div>
        <StatusBadge state={badge} label={status} />
      </div>
      <label className="space-y-2">
        <span className="flex items-center gap-2 text-sm font-medium text-slate-600">Токен бота MAX <Hint text="Вставьте токен, выданный в кабинете MAX для вашего бота." /></span>
        <input
          className="input"
          type="password"
          value={token}
          placeholder="Введите токен"
          onChange={(e) => setToken(e.target.value)}
        />
      </label>
      <div className="flex flex-wrap gap-3">
        <button className="btn" onClick={handleConnect} disabled={saving}>
          Подключить
        </button>
        <button className="btn-secondary" onClick={handleDisconnect} disabled={saving}>
          Отключить
        </button>
      </div>
      <div className="text-xs text-slate-400">
        Подключение работает через официальный Bot API MAX. После подключения будет зарегистрирован вебхук.
      </div>
    </div>
  );
};

const MaxPersonalCard: React.FC = () => {
  const { api } = useClient();
  const [status, setStatus] = useState('Проверяем статус…');
  const [badge, setBadge] = useState<'ok' | 'warn' | 'err' | 'idle'>('idle');
  const [connected, setConnected] = useState(false);
  const [qrDataUrl, setQrDataUrl] = useState<string | null>(null);
  const [accountTitle, setAccountTitle] = useState('');
  const [busy, setBusy] = useState(false);

  const fetchQr = async (quiet = false): Promise<boolean> => {
    if (!api.tenantId || !api.key) return false;
    try {
      const data = await requestJson<Record<string, any>>(buildUrl('/v1/max-personal/session/qr', api, { _: Date.now() }));
      const nextQr = String(data.qr_png_data_url || '').trim();
      if (nextQr) {
        setQrDataUrl(nextQr);
        return true;
      }
      setQrDataUrl(null);
      return false;
    } catch {
      setQrDataUrl(null);
      if (!quiet) toast.error('QR недоступен, попробуйте запустить сессию заново');
      return false;
    }
  };

  const fetchStatus = async (quiet = false) => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<Record<string, any>>(buildUrl('/v1/max-personal/status', api, { _: Date.now() }));
      const currentStatus = String(data.status || 'idle');
      const isConnected = Boolean(data.connected);
      const account = (data.account || {}) as Record<string, any>;
      const title = String(account.display_name || account.username || account.phone || '').trim();
      setAccountTitle(title);
      setConnected(isConnected);
      if (isConnected) {
        setStatus('Подключено');
        setBadge('ok');
        setQrDataUrl(null);
        return;
      }
      if (currentStatus === 'waiting_qr' || currentStatus === 'authorizing' || data.qr_required) {
        setStatus('Ожидает QR');
        setBadge('warn');
        void fetchQr(true);
        return;
      }
      if (currentStatus === 'reauth_required') {
        setStatus('Нужна повторная авторизация');
        setBadge('warn');
      } else if (currentStatus === 'error') {
        setStatus('Ошибка сессии');
        setBadge('err');
      } else {
        setStatus('Не подключено');
        setBadge('warn');
      }
      setQrDataUrl(null);
      if (!quiet && data.last_error) {
        toast.error(String(data.last_error));
      }
    } catch {
      setStatus('Статус недоступен');
      setBadge('err');
      setQrDataUrl(null);
      if (!quiet) toast.error('Не удалось получить статус MAX');
    }
  };

  useEffect(() => {
    if (!api.tenantId || !api.key) return;
    fetchStatus(true).catch(() => undefined);
    const timer = window.setInterval(() => {
      fetchStatus(true).catch(() => undefined);
    }, 3000);
    return () => window.clearInterval(timer);
  }, [api.tenantId, api.key]);

  const connect = async () => {
    setBusy(true);
    try {
      await postJson(buildUrl('/v1/max-personal/connect', api), { force: true });
      toast.success('MAX запущен, получаем QR…');
      await fetchStatus(true);
      const hasQr = await fetchQr(true);
      if (!hasQr) {
        toast('QR ещё подготавливается, подождите 3-10 секунд');
      }
    } catch {
      toast.error('Не удалось запустить MAX');
    } finally {
      setBusy(false);
    }
  };

  const refreshQr = async () => {
    await connect();
  };

  const logout = async () => {
    setBusy(true);
    try {
      await postJson(buildUrl('/v1/max-personal/session/logout', api), {});
      toast.success('MAX отключён');
      await fetchStatus(true);
    } catch {
      toast.error('Не удалось отключить MAX');
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="card space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <div className="card-title">MAX</div>
          <div className="card-subtitle flex items-center gap-2">
            Подключение личного аккаунта по QR <Hint text="Канал подключает личный MAX-аккаунт через QR. Исходящие и входящие сообщения идут в общую логику Avio." />
          </div>
        </div>
        <StatusBadge state={badge} label={status} />
      </div>
      <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
        {qrDataUrl ? (
          <img src={qrDataUrl} alt="QR MAX" className="mx-auto h-52 w-52 rounded-xl bg-white p-3" />
        ) : (
          <div className="text-center text-sm text-slate-400">QR появится после запуска сессии</div>
        )}
      </div>
      {accountTitle ? (
        <div className="text-xs text-slate-500">Аккаунт: {accountTitle}</div>
      ) : null}
      <div className="flex flex-wrap gap-3">
        <button className="btn-secondary" onClick={refreshQr} disabled={busy}>
          Обновить QR
        </button>
        <button className="btn-ghost" onClick={logout} disabled={busy}>
          Отключиться
        </button>
      </div>
    </div>
  );
};

type AmoPipelineOption = {
  id: number;
  name: string;
};

const AmoCRMCard: React.FC = () => {
  const { api, settings, refreshSettings, setSettings } = useClient();
  const [status, setStatus] = useState('Проверяем статус…');
  const [badge, setBadge] = useState<'ok' | 'warn' | 'err' | 'idle'>('idle');
  const [connected, setConnected] = useState(false);
  const [chatInfo, setChatInfo] = useState<Record<string, any> | null>(null);
  const [stages, setStages] = useState<Array<Record<string, any>>>([]);
  const [pipelineOptions, setPipelineOptions] = useState<AmoPipelineOption[]>([]);
  const [pipelineRouting, setPipelineRouting] = useState({
    default_pipeline_id: 0,
    avito_pipeline_id: 0,
    tgmax_pipeline_id: 0,
  });
  const [rulesOptions, setRulesOptions] = useState({
    stage_router_mode: 'auto',
    stage_router_confidence_auto: 0.72,
    stage_router_confidence_semi: 0.45,
    stage_router_cooldown_seconds: 300,
    stage_router_max_stage_jump: 1,
    stage_router_allow_terminal_auto: false,
  });
  const [loadingStages, setLoadingStages] = useState(false);
  const [savingRules, setSavingRules] = useState(false);

  const statusUrl = useMemo(() => buildUrl('/pub/integrations/amocrm/status', api), [api]);
  const oauthStartUrl = useMemo(
    () => buildUrl('/pub/integrations/amocrm/oauth/start', api),
    [api]
  );
  const settingsSaveUrl = useMemo(() => buildUrl('/pub/settings/save', api), [api]);
  const disconnectUrl = useMemo(
    () => buildUrl('/pub/integrations/amocrm/disconnect', api),
    [api]
  );

  const normalizeStages = (items: unknown[]) => {
    return (items || []).map((stage) => ({ ...(stage || {}) } as Record<string, any>));
  };

  const normalizePipelines = (items: unknown): AmoPipelineOption[] => {
    if (!Array.isArray(items)) return [];
    return items
      .map((item) => {
        const id = Number.parseInt(String((item as any)?.id ?? ''), 10);
        if (!Number.isFinite(id) || id <= 0) return null;
        const name = String((item as any)?.name || '').trim() || `Воронка ${id}`;
        return { id, name };
      })
      .filter((item): item is AmoPipelineOption => Boolean(item));
  };

  const asPipelineId = (value: any): number => {
    const id = Number.parseInt(String(value ?? ''), 10);
    return Number.isFinite(id) && id > 0 ? id : 0;
  };

  const normalizeOptions = (raw: any) => {
    const options = raw && typeof raw === 'object' ? raw : {};
    const modeRaw = String(options.stage_router_mode || 'auto').toLowerCase();
    const stage_router_mode = modeRaw === 'off' || modeRaw === 'semi_auto' || modeRaw === 'auto' ? modeRaw : 'auto';
    const stage_router_confidence_auto = Math.min(
      1,
      Math.max(0, Number.parseFloat(String(options.stage_router_confidence_auto ?? '0.72')) || 0.72),
    );
    const stage_router_confidence_semi = Math.min(
      1,
      Math.max(0, Number.parseFloat(String(options.stage_router_confidence_semi ?? '0.45')) || 0.45),
    );
    const stage_router_cooldown_seconds = Math.max(
      0,
      Number.parseInt(String(options.stage_router_cooldown_seconds ?? '300'), 10) || 300,
    );
    const stage_router_max_stage_jump = Math.min(
      3,
      Math.max(1, Number.parseInt(String(options.stage_router_max_stage_jump ?? '1'), 10) || 1),
    );
    const stage_router_allow_terminal_auto = Boolean(options.stage_router_allow_terminal_auto);
    return {
      stage_router_mode,
      stage_router_confidence_auto,
      stage_router_confidence_semi,
      stage_router_cooldown_seconds,
      stage_router_max_stage_jump,
      stage_router_allow_terminal_auto,
    };
  };

  const fetchStatus = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<Record<string, any>>(statusUrl);
      const isConnected = Boolean(data.connected);
      const statusPipelines = normalizePipelines((data as any).pipelines);
      if (statusPipelines.length > 0) {
        setPipelineOptions(statusPipelines);
        setPipelineRouting((prev) => {
          const fallback = statusPipelines[0].id;
          return {
            default_pipeline_id: prev.default_pipeline_id || fallback,
            avito_pipeline_id: prev.avito_pipeline_id || prev.default_pipeline_id || fallback,
            tgmax_pipeline_id: prev.tgmax_pipeline_id || prev.default_pipeline_id || fallback,
          };
        });
      }
      setChatInfo(data.chat && typeof data.chat === 'object' ? data.chat : null);
      setConnected(isConnected);
      if (isConnected) {
        setStatus('Подключено');
        setBadge('ok');
      } else {
        setStatus('Не подключено');
        setBadge('warn');
      }
    } catch (error) {
      setStatus('Статус недоступен');
      setBadge('err');
    }
  };

  useEffect(() => {
    if (!api.tenantId || !api.key) return;
    fetchStatus().catch(() => undefined);
  }, [api.tenantId, api.key]);

  useEffect(() => {
    const cfg = (settings?.cfg as Record<string, any>) || {};
    const amocrmCfg = (cfg.integrations || {}).amocrm || {};
    const list = normalizeStages(amocrmCfg.stages || []);
    setStages(list);
    const options = normalizePipelines(amocrmCfg.pipelines_cache || []);
    if (options.length > 0) {
      setPipelineOptions(options);
    }
    const defaultPipeline = asPipelineId(amocrmCfg.pipeline_id);
    const avitoPipeline = asPipelineId(amocrmCfg.pipeline_id_avito);
    const tgmaxPipeline = asPipelineId(amocrmCfg.pipeline_id_tgmax);
    setPipelineRouting({
      default_pipeline_id: defaultPipeline,
      avito_pipeline_id: avitoPipeline || defaultPipeline,
      tgmax_pipeline_id: tgmaxPipeline || defaultPipeline,
    });
    setRulesOptions(normalizeOptions(amocrmCfg.rules_options || {}));
  }, [settings]);

  const handleConnect = () => {
    if (!api.tenantId || !api.key) return;
    window.open(oauthStartUrl, 'amocrm-oauth', 'width=640,height=760,noopener=yes,noreferrer=yes');
  };

  const handleDisconnect = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      await requestJson(disconnectUrl, { method: 'POST' });
      toast.success('amoCRM отключён');
      setConnected(false);
      setStatus('Не подключено');
      setBadge('warn');
    } catch (error) {
      toast.error('Не удалось отключить amoCRM');
    }
  };

  const refreshPipeline = async (targetPipelineId?: number, opts?: { silent?: boolean }) => {
    if (!api.tenantId || !api.key) return;
    setLoadingStages(true);
    try {
      const data = await requestJson<Record<string, any>>(
        buildUrl('/pub/integrations/amocrm/pipeline', api, targetPipelineId ? { pipeline_id: targetPipelineId } : undefined),
      );
      const available = normalizePipelines(data.pipelines);
      if (available.length > 0) {
        setPipelineOptions(available);
      }
      if (Array.isArray(data.stages)) {
        setStages(normalizeStages(data.stages));
        if (!opts?.silent) {
          toast.success('Стадии загружены из amoCRM');
        }
      } else {
        if (!opts?.silent) {
          toast.error('Не удалось получить стадии');
        }
      }
    } catch (error) {
      if (!opts?.silent) {
        toast.error('Не удалось загрузить стадии');
      }
    } finally {
      setLoadingStages(false);
    }
  };

  useEffect(() => {
    if (!connected || !api.tenantId || !api.key) return;
    const target = pipelineRouting.default_pipeline_id > 0 ? pipelineRouting.default_pipeline_id : undefined;
    refreshPipeline(target, { silent: true }).catch(() => undefined);
  }, [connected, api.tenantId, api.key]);

  const handleSaveRules = async () => {
    if (!api.tenantId || !api.key) return;
    setSavingRules(true);
    try {
      const cfg = ((settings && settings.cfg) || {}) as Record<string, any>;
      const integrations = { ...(cfg.integrations || {}) };
      const amocrmCfg = { ...(integrations.amocrm || {}) };
      const selectedDefault =
        pipelineRouting.default_pipeline_id > 0
          ? pipelineRouting.default_pipeline_id
          : (pipelineOptions[0]?.id || 0);
      if (selectedDefault > 0) {
        amocrmCfg.pipeline_id = selectedDefault;
      }
      if (pipelineRouting.avito_pipeline_id > 0) {
        amocrmCfg.pipeline_id_avito = pipelineRouting.avito_pipeline_id;
      } else {
        delete amocrmCfg.pipeline_id_avito;
      }
      if (pipelineRouting.tgmax_pipeline_id > 0) {
        amocrmCfg.pipeline_id_tgmax = pipelineRouting.tgmax_pipeline_id;
      } else {
        delete amocrmCfg.pipeline_id_tgmax;
      }
      if (pipelineOptions.length > 0) {
        amocrmCfg.pipelines_cache = pipelineOptions.map((item) => ({ id: item.id, name: item.name }));
      }
      amocrmCfg.stages = stages;
      amocrmCfg.rules_options = rulesOptions;
      integrations.amocrm = amocrmCfg;
      const nextCfg = { ...cfg, integrations };
      await postJson(settingsSaveUrl, { cfg: nextCfg });
      setSettings((prev) => ({ ...(prev || {}), cfg: nextCfg }));
      toast.success('Правила сохранены');
      refreshSettings().catch(() => undefined);
    } catch (error) {
      toast.error('Не удалось сохранить правила');
    } finally {
      setSavingRules(false);
    }
  };

  return (
    <div className="card space-y-4 lg:col-span-3">
      <div className="flex items-center justify-between">
        <div>
          <div className="card-title">amoCRM</div>
          <div className="card-subtitle flex items-center gap-2">Подключение аккаунта <Hint text="Интеграция amoCRM через OAuth с AI-автопереходами по стадиям воронки." /></div>
        </div>
        <StatusBadge state={badge} label={status} />
      </div>
      <div className="flex flex-wrap gap-3">
        <button className="btn" onClick={connected ? handleDisconnect : handleConnect}>
          {connected ? 'Отключить' : 'Подключить'}
        </button>
        <button className="btn-secondary" onClick={refreshPipeline} disabled={!connected || loadingStages}>
          {loadingStages ? 'Загрузка…' : 'Обновить стадии'}
        </button>
        <button className="btn-ghost" onClick={handleSaveRules} disabled={!connected || savingRules}>
          {savingRules ? 'Сохраняем…' : 'Сохранить правила'}
        </button>
      </div>
      <div className="text-xs text-slate-400">
        Подключение выполняется через OAuth, настройки берутся из env.
      </div>
      {connected && chatInfo && (
        <div className="rounded-2xl border border-blue-100 bg-blue-50/80 p-4 text-sm text-slate-700">
          <div className="font-semibold text-slate-900">amoCRM Chats / Inbox</div>
          <div className="mt-1">
            {chatInfo.connected
              ? 'Telegram → amoCRM Inbox подключён. Новые сообщения смогут попадать в сделки и чат amoCRM.'
              : chatInfo.env_configured
                ? 'Коннектор amoCRM Chats готов, но аккаунт ещё не подключён к Inbox. После первого успешного connect появится scope канала.'
                : 'Для реального Inbox нужны реквизиты коннектора amoCRM Chats. Пока обычная CRM-связка работает, но Telegram ещё не зеркалится в Inbox.'}
          </div>
          {chatInfo.webhook_url && (
            <div className="mt-2 break-all text-xs text-slate-500">
              Webhook URL: {chatInfo.webhook_url}
            </div>
          )}
        </div>
      )}
      {connected && (
        <div className="space-y-4 rounded-2xl border border-slate-200 bg-slate-50 p-4">
          <div className="space-y-3 rounded-2xl border border-slate-200 bg-white p-4">
            <div className="text-sm font-semibold text-slate-700">Воронки amoCRM по каналам</div>
            <div className="text-xs text-slate-500">
              Доступно воронок: {pipelineOptions.length || 0}. Выберите, куда создавать сделки по источнику лида.
            </div>
            <div className="grid gap-3 md:grid-cols-3">
              <label className="text-sm text-slate-600">
                Базовая воронка (stage router)
                <select
                  className="input mt-1"
                  value={pipelineRouting.default_pipeline_id || ''}
                  onChange={(e) => {
                    const nextId = asPipelineId(e.target.value);
                    setPipelineRouting((prev) => ({
                      ...prev,
                      default_pipeline_id: nextId,
                      avito_pipeline_id: prev.avito_pipeline_id || nextId,
                      tgmax_pipeline_id: prev.tgmax_pipeline_id || nextId,
                    }));
                    if (nextId > 0) {
                      refreshPipeline(nextId, { silent: true }).catch(() => undefined);
                    }
                  }}
                >
                  <option value="">Не выбрано</option>
                  {pipelineOptions.map((item) => (
                    <option key={`default-${item.id}`} value={item.id}>
                      {item.name}
                    </option>
                  ))}
                </select>
              </label>
              <label className="text-sm text-slate-600">
                Avito лиды
                <select
                  className="input mt-1"
                  value={pipelineRouting.avito_pipeline_id || ''}
                  onChange={(e) =>
                    setPipelineRouting((prev) => ({
                      ...prev,
                      avito_pipeline_id: asPipelineId(e.target.value),
                    }))
                  }
                >
                  <option value="">Использовать базовую</option>
                  {pipelineOptions.map((item) => (
                    <option key={`avito-${item.id}`} value={item.id}>
                      {item.name}
                    </option>
                  ))}
                </select>
              </label>
              <label className="text-sm text-slate-600">
                Telegram / MAX лиды
                <select
                  className="input mt-1"
                  value={pipelineRouting.tgmax_pipeline_id || ''}
                  onChange={(e) =>
                    setPipelineRouting((prev) => ({
                      ...prev,
                      tgmax_pipeline_id: asPipelineId(e.target.value),
                    }))
                  }
                >
                  <option value="">Использовать базовую</option>
                  {pipelineOptions.map((item) => (
                    <option key={`tgmax-${item.id}`} value={item.id}>
                      {item.name}
                    </option>
                  ))}
                </select>
              </label>
            </div>
          </div>
          <div className="text-sm font-semibold text-slate-700">Автопереходы по воронке</div>
          <div className="grid gap-3 md:grid-cols-3">
            <label className="text-sm text-slate-600">
              Режим автоперехода
              <select
                className="input mt-1"
                value={rulesOptions.stage_router_mode}
                onChange={(e) =>
                  setRulesOptions((prev) => ({
                    ...prev,
                    stage_router_mode: e.target.value as 'off' | 'semi_auto' | 'auto',
                  }))
                }
              >
                <option value="off">Выключен</option>
                <option value="semi_auto">Semi-auto (рекомендация менеджеру)</option>
                <option value="auto">Auto (автоприменение)</option>
              </select>
            </label>
            <label className="text-sm text-slate-600">
              Порог confidence (auto)
              <input
                className="input mt-1"
                type="number"
                min={0}
                max={1}
                step={0.01}
                value={rulesOptions.stage_router_confidence_auto}
                onChange={(e) =>
                  setRulesOptions((prev) => ({
                    ...prev,
                    stage_router_confidence_auto: Math.min(1, Math.max(0, Number.parseFloat(e.target.value) || 0)),
                  }))
                }
              />
            </label>
            <label className="text-sm text-slate-600">
              Порог confidence (semi-auto)
              <input
                className="input mt-1"
                type="number"
                min={0}
                max={1}
                step={0.01}
                value={rulesOptions.stage_router_confidence_semi}
                onChange={(e) =>
                  setRulesOptions((prev) => ({
                    ...prev,
                    stage_router_confidence_semi: Math.min(1, Math.max(0, Number.parseFloat(e.target.value) || 0)),
                  }))
                }
              />
            </label>
            <label className="text-sm text-slate-600">
              Cooldown между переходами (сек)
              <input
                className="input mt-1"
                type="number"
                min={0}
                value={rulesOptions.stage_router_cooldown_seconds}
                onChange={(e) =>
                  setRulesOptions((prev) => ({
                    ...prev,
                    stage_router_cooldown_seconds: Math.max(0, Number.parseInt(e.target.value || '0', 10) || 0),
                  }))
                }
              />
            </label>
            <label className="text-sm text-slate-600">
              Макс. шагов за переход
              <input
                className="input mt-1"
                type="number"
                min={1}
                max={3}
                value={rulesOptions.stage_router_max_stage_jump}
                onChange={(e) =>
                  setRulesOptions((prev) => ({
                    ...prev,
                    stage_router_max_stage_jump: Math.min(3, Math.max(1, Number.parseInt(e.target.value || '1', 10) || 1)),
                  }))
                }
              />
            </label>
            <label className="flex items-center gap-2 text-sm text-slate-600">
              <input
                type="checkbox"
                checked={rulesOptions.stage_router_allow_terminal_auto}
                onChange={(e) =>
                  setRulesOptions((prev) => ({
                    ...prev,
                    stage_router_allow_terminal_auto: e.target.checked,
                  }))
                }
              />
              Разрешить автопереход в финальные стадии
            </label>
          </div>
          <div className="rounded-xl border border-slate-200 bg-white p-3 text-xs text-slate-500">
            Rule-based переходы удалены. Используется только AI-роутер стадий + guard-ограничения.
          </div>
          <div className="space-y-3">
            {stages.length === 0 && (
              <div className="text-sm text-slate-500">Стадии не загружены. Нажмите «Обновить стадии».</div>
            )}
            {stages.map((stage, index) => {
              const stageType = String(stage.type || '').trim().toLowerCase();
              const isTerminal = stageType === 'won' || stageType === 'lost';
              return (
                <div key={`${stage.amo_stage_id || index}`} className="rounded-xl bg-white p-3 shadow-sm">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <div className="text-sm font-semibold text-slate-800">
                        {stage.name || `Стадия ${index + 1}`}
                      </div>
                      <div className="text-xs text-slate-400">
                        ID: {stage.amo_stage_id || '-'} · Тип: {stageType || 'open'}
                      </div>
                    </div>
                    {isTerminal && (
                      <span className="rounded-full border border-rose-200 bg-rose-50 px-2.5 py-1 text-xs font-semibold text-rose-700">
                        Финальная стадия
                      </span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

export default ChannelsTab;
