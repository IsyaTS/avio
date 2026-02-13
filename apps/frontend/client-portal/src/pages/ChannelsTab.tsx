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

  const fetchStatus = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<Record<string, any>>(buildUrl('/pub/tg/status', api, { _: Date.now() }));
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

  const handleDisconnect = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      await requestJson(buildUrl('/pub/tg/logout', api), { method: 'POST' });
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
          <div className="card-subtitle flex items-center gap-2">Подключение через QR или 2FA <Hint text="Подключение Telegram через QR, при необходимости вводится пароль двухфакторной защиты." /></div>
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
      if (connected) {
        await ensureWebhook();
      }
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
          <div className="card-subtitle flex items-center gap-2">OAuth подключение аккаунта <Hint text="Авторизует Avito аккаунт и проверяет, что вебхук получает новые сообщения." /></div>
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

const AmoCRMCard: React.FC = () => {
  const { api, settings, refreshSettings, setSettings } = useClient();
  const [status, setStatus] = useState('Проверяем статус…');
  const [badge, setBadge] = useState<'ok' | 'warn' | 'err' | 'idle'>('idle');
  const [connected, setConnected] = useState(false);
  const [stages, setStages] = useState<Array<Record<string, any>>>([]);
  const [rulesOptions, setRulesOptions] = useState({ allow_multi_step: false, max_steps_per_event: 1 });
  const [loadingStages, setLoadingStages] = useState(false);
  const [savingRules, setSavingRules] = useState(false);

  const statusUrl = useMemo(() => buildUrl('/pub/integrations/amocrm/status', api), [api]);
  const oauthStartUrl = useMemo(
    () => buildUrl('/pub/integrations/amocrm/oauth/start', api),
    [api]
  );
  const pipelineUrl = useMemo(() => buildUrl('/pub/integrations/amocrm/pipeline', api), [api]);
  const settingsSaveUrl = useMemo(() => buildUrl('/pub/settings/save', api), [api]);
  const disconnectUrl = useMemo(
    () => buildUrl('/pub/integrations/amocrm/disconnect', api),
    [api]
  );

  const defaultRuleForIndex = (index: number) => {
    if (index === 0) return { type: 'on_first_inbound', params: {} };
    if (index === 1) return { type: 'on_inbound_count', params: { min_inbound_messages: 2 } };
    if (index === 2) return { type: 'on_inbound_count', params: { min_inbound_messages: 4 } };
    return { type: 'manual_only', params: {} };
  };

  const normalizeStages = (items: unknown[]) => {
    return (items || []).map((stage, idx) => {
      const normalized = { ...(stage || {}) } as Record<string, any>;
      if (!normalized.rule) {
        normalized.rule = defaultRuleForIndex(idx);
      }
      if (!normalized.rule.params) {
        normalized.rule.params = {};
      }
      return normalized;
    });
  };

  const normalizeOptions = (raw: any) => {
    const options = raw && typeof raw === 'object' ? raw : {};
    const allow_multi_step = Boolean(options.allow_multi_step);
    const max_steps_per_event = Number.parseInt(options.max_steps_per_event || '1', 10) || 1;
    return {
      allow_multi_step,
      max_steps_per_event: max_steps_per_event > 0 ? max_steps_per_event : 1,
    };
  };

  const fetchStatus = async () => {
    if (!api.tenantId || !api.key) return;
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

  const refreshPipeline = async () => {
    if (!api.tenantId || !api.key) return;
    setLoadingStages(true);
    try {
      const data = await requestJson<Record<string, any>>(pipelineUrl);
      if (Array.isArray(data.stages)) {
        setStages(normalizeStages(data.stages));
        toast.success('Стадии загружены из amoCRM');
      } else {
        toast.error('Не удалось получить стадии');
      }
    } catch (error) {
      toast.error('Не удалось загрузить стадии');
    } finally {
      setLoadingStages(false);
    }
  };

  const updateStageRule = (index: number, next: Record<string, any>) => {
    setStages((prev) =>
      prev.map((stage, idx) => (idx === index ? { ...stage, rule: { ...stage.rule, ...next } } : stage))
    );
  };

  const handleSaveRules = async () => {
    if (!api.tenantId || !api.key) return;
    setSavingRules(true);
    try {
      const cfg = ((settings && settings.cfg) || {}) as Record<string, any>;
      const integrations = { ...(cfg.integrations || {}) };
      const amocrmCfg = { ...(integrations.amocrm || {}) };
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

  const ruleTypeOptions = [
    { value: 'on_first_inbound', label: 'Первое входящее' },
    { value: 'on_inbound_count', label: 'По количеству входящих' },
    { value: 'on_keyword', label: 'По ключевому слову' },
    { value: 'on_field_present', label: 'По наличию поля' },
    { value: 'manual_only', label: 'Только вручную' },
  ];

  return (
    <div className="card space-y-4 lg:col-span-3">
      <div className="flex items-center justify-between">
        <div>
          <div className="card-title">amoCRM</div>
          <div className="card-subtitle flex items-center gap-2">Подключение аккаунта <Hint text="Интеграция amoCRM через OAuth, включая правила автопереходов по стадиям." /></div>
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
      {connected && (
        <div className="space-y-4 rounded-2xl border border-slate-200 bg-slate-50 p-4">
          <div className="text-sm font-semibold text-slate-700">Автопереходы по воронке</div>
          <div className="grid gap-3 md:grid-cols-2">
            <label className="flex items-center gap-2 text-sm text-slate-600">
              <input
                type="checkbox"
                checked={rulesOptions.allow_multi_step}
                onChange={(e) =>
                  setRulesOptions((prev) => ({ ...prev, allow_multi_step: e.target.checked }))
                }
              />
              Разрешить несколько шагов за сообщение
            </label>
            <label className="text-sm text-slate-600">
              Максимум шагов за событие
              <input
                className="input mt-1"
                type="number"
                min={1}
                value={rulesOptions.max_steps_per_event}
                onChange={(e) =>
                  setRulesOptions((prev) => ({
                    ...prev,
                    max_steps_per_event: Number.parseInt(e.target.value || '1', 10) || 1,
                  }))
                }
              />
            </label>
          </div>
          <div className="space-y-3">
            {stages.length === 0 && (
              <div className="text-sm text-slate-500">Стадии не загружены. Нажмите «Обновить стадии».</div>
            )}
            {stages.map((stage, index) => {
              const rule = stage.rule || {};
              const params = rule.params || {};
              return (
                <div key={`${stage.amo_stage_id || index}`} className="rounded-xl bg-white p-3 shadow-sm">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <div className="text-sm font-semibold text-slate-800">
                        {stage.name || `Стадия ${index + 1}`}
                      </div>
                      <div className="text-xs text-slate-400">ID: {stage.amo_stage_id || '-'}</div>
                    </div>
                    <select
                      className="input"
                      value={rule.type || ''}
                      onChange={(e) =>
                        updateStageRule(index, {
                          type: e.target.value,
                          params:
                            e.target.value === 'on_inbound_count'
                              ? { min_inbound_messages: params.min_inbound_messages || 2 }
                              : e.target.value === 'on_keyword'
                              ? { keywords: params.keywords || [] }
                              : e.target.value === 'on_field_present'
                              ? { field_key: params.field_key || '' }
                              : {},
                        })
                      }
                    >
                      {ruleTypeOptions.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  </div>
                  {rule.type === 'on_inbound_count' && (
                    <div className="mt-2 text-sm text-slate-600">
                      Минимум входящих
                      <input
                        className="input mt-1"
                        type="number"
                        min={1}
                        value={params.min_inbound_messages || 1}
                        onChange={(e) =>
                          updateStageRule(index, {
                            params: {
                              ...params,
                              min_inbound_messages:
                                Number.parseInt(e.target.value || '1', 10) || 1,
                            },
                          })
                        }
                      />
                    </div>
                  )}
                  {rule.type === 'on_keyword' && (
                    <div className="mt-2 text-sm text-slate-600">
                      Ключевые слова (через запятую)
                      <input
                        className="input mt-1"
                        type="text"
                        value={(params.keywords || []).join(', ')}
                        onChange={(e) =>
                          updateStageRule(index, {
                            params: {
                              ...params,
                              keywords: e.target.value
                                .split(',')
                                .map((item: string) => item.trim())
                                .filter(Boolean),
                            },
                          })
                        }
                      />
                    </div>
                  )}
                  {rule.type === 'on_field_present' && (
                    <div className="mt-2 text-sm text-slate-600">
                      Ключ поля (например phone)
                      <input
                        className="input mt-1"
                        type="text"
                        value={params.field_key || ''}
                        onChange={(e) =>
                          updateStageRule(index, {
                            params: {
                              ...params,
                              field_key: e.target.value,
                            },
                          })
                        }
                      />
                    </div>
                  )}
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
