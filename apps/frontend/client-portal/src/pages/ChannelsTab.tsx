import React, { useEffect, useMemo, useState } from 'react';
import toast from 'react-hot-toast';
import { useClient } from '../context/ClientContext';
import { buildUrl, postJson, requestJson } from '../lib/api';

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
          <div className="card-subtitle">QR и статус подключения</div>
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

  const fetchStatus = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<Record<string, any>>(buildUrl('/pub/tg/status', api, { _: Date.now() }));
      const currentStatus = String(data.status || 'unknown');
      setStatus(currentStatus);
      if (currentStatus === 'authorized') {
        setBadge('ok');
      } else if (currentStatus === 'waiting_qr') {
        setBadge('warn');
      } else {
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
    fetchStatus().catch(() => undefined);
    if (!api.tenantId || !api.key) return;
    const timer = window.setInterval(() => {
      fetchStatus().catch(() => undefined);
    }, 2500);
    return () => window.clearInterval(timer);
  }, [api.tenantId, api.key]);

  const refreshQr = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      await requestJson(buildUrl('/pub/tg/start', api, { force: 1 }));
      await fetchStatus();
      toast.success('QR обновлён');
    } catch {
      toast.error('Не удалось обновить QR');
    }
  };

  const submit2fa = async () => {
    if (!password.trim()) {
      toast.error('Введите пароль');
      return;
    }
    try {
      await postJson(buildUrl('/pub/tg/2fa', api), { password: password.trim() });
      toast.success('Пароль отправлен');
      setPassword('');
      fetchStatus().catch(() => undefined);
    } catch (error) {
      toast.error('Пароль не принят');
    }
  };

  const qrUrl = qrId
    ? buildUrl('/pub/tg/qr.png', api, { qr_id: qrId, t: Date.now() })
    : buildUrl('/pub/tg/qr.png', api, { t: Date.now() });

  return (
    <div className="card space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <div className="card-title">Telegram</div>
          <div className="card-subtitle">Подключение через QR или 2FA</div>
        </div>
        <StatusBadge state={badge} label={status} />
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
      </div>
    </div>
  );
};

const AvitoCard: React.FC = () => {
  const { api } = useClient();
  const [status, setStatus] = useState('Проверяем статус…');
  const [badge, setBadge] = useState<'ok' | 'warn' | 'err' | 'idle'>('idle');
  const [connected, setConnected] = useState(false);

  const statusUrl = useMemo(() => buildUrl('/v1/oauth/avito/status', api), [api]);
  const authorizeUrl = useMemo(() => buildUrl('/v1/oauth/avito/authorize', api), [api]);
  const webhookUrl = useMemo(() => buildUrl('/v1/oauth/avito/webhook', api), [api]);
  const disconnectUrl = useMemo(() => buildUrl('/v1/oauth/avito/disconnect', api), [api]);

  const fetchStatus = async (quiet = false) => {
    try {
      const data = await requestJson<Record<string, any>>(statusUrl);
      const isConnected = Boolean(data.connected);
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
    try {
      await ensureWebhook();
      const data = await requestJson<Record<string, any>>(authorizeUrl);
      const target = data.authorize_url || data.url;
      if (!target) {
        throw new Error('authorize_url missing');
      }
      window.open(target, 'avito-oauth', 'width=640,height=760,noopener=yes,noreferrer=yes');
      toast.success('Окно авторизации открыто');
    } catch (error) {
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

  return (
    <div className="card space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <div className="card-title">Avito</div>
          <div className="card-subtitle">OAuth подключение аккаунта</div>
        </div>
        <StatusBadge state={badge} label={status} />
      </div>
      <div className="flex flex-wrap gap-3">
        <button className="btn" onClick={connect}>
          {connected ? 'Обновить авторизацию' : 'Подключить Avito'}
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
    </div>
  );
};

export default ChannelsTab;
