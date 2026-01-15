import React, { useEffect, useMemo, useState } from 'react';
import toast from 'react-hot-toast';
import { useClient } from '../context/ClientContext';
import { buildUrl, postJson, requestJson } from '../lib/api';

const channelOptions = [
  { value: 'any', label: 'Все' },
  { value: 'telegram', label: 'Telegram' },
  { value: 'avito', label: 'Avito' },
  { value: 'whatsapp', label: 'WhatsApp' },
];

type TriggerRule = {
  phrases: string[];
  channels: string[];
  silence: boolean;
  notify: boolean;
};

type FollowUpCondition = {
  key: string;
  op: string;
  value?: string;
};

type FollowUpCapture = {
  key: string;
  label?: string;
  yes: string[] | string;
  no: string[] | string;
  value_yes?: string;
  value_no?: string;
};

type FollowUpRule = {
  channel: string;
  delay_minutes: number;
  max_attempts: number;
  active: boolean;
  text: string;
  trigger_on_answer?: boolean;
  condition?: FollowUpCondition | FollowUpCondition[] | null;
  capture?: FollowUpCapture | null;
};

const emptyTrigger = (): TriggerRule => ({
  phrases: [],
  channels: ['telegram', 'avito', 'whatsapp'],
  silence: true,
  notify: false,
});

const emptyFollowup = (): FollowUpRule => ({
  channel: 'any',
  delay_minutes: 10,
  max_attempts: 1,
  active: true,
  text: '',
  trigger_on_answer: false,
});

const normalizeFactKey = (raw: string, fallback: string) => {
  const cleaned = raw
    .toLowerCase()
    .trim()
    .replace(/\s+/g, '_')
    .replace(/[^\w\u0400-\u04ff_]+/g, '');
  return cleaned || fallback;
};

const SettingsTab: React.FC = () => {
  const { bootstrap, api, settings, refreshSettings, setSettings } = useClient();
  const draftKey = useMemo(
    () => `client-settings-draft:${api.tenantId || 'unknown'}`,
    [api.tenantId]
  );
  const [draftInitialized, setDraftInitialized] = useState(false);
  const [hasDraft, setHasDraft] = useState(false);

  const passportDefaults = bootstrap.form || {};
  const behaviorDefaults = bootstrap.behavior || {};

  const [brand, setBrand] = useState(passportDefaults.brand || '');
  const [agent, setAgent] = useState(passportDefaults.agent || '');
  const [city, setCity] = useState(passportDefaults.city || '');
  const [currency, setCurrency] = useState(passportDefaults.currency || '');
  const [tone, setTone] = useState(passportDefaults.tone || '');

  const [personaChannel, setPersonaChannel] = useState<'base' | 'telegram' | 'avito'>('base');
  const [personaBase, setPersonaBase] = useState('');
  const [personaTelegram, setPersonaTelegram] = useState('');
  const [personaAvito, setPersonaAvito] = useState('');

  const [autoReply, setAutoReply] = useState(Boolean(behaviorDefaults.auto_reply));
  const [autoReplyText, setAutoReplyText] = useState(behaviorDefaults.auto_reply_text || '');
  const [avitoPhoneTemplate, setAvitoPhoneTemplate] = useState(
    behaviorDefaults.avito_phone_tg_template || ''
  );
  const [avitoSmartReply, setAvitoSmartReply] = useState(
    Boolean(behaviorDefaults.avito_smart_reply_enabled)
  );
  const [telegramReplyEnabled, setTelegramReplyEnabled] = useState(
    behaviorDefaults.telegram_reply_enabled !== false
  );
  const [sendCatalogTg, setSendCatalogTg] = useState(
    Boolean(behaviorDefaults.send_catalog_on_first_message)
  );
  const [autoPhotoEnabled, setAutoPhotoEnabled] = useState(
    Boolean(behaviorDefaults.auto_photo_enabled)
  );
  const [autoPhotoMax, setAutoPhotoMax] = useState(
    behaviorDefaults.auto_photo_max ? String(behaviorDefaults.auto_photo_max) : ''
  );
  const [photoMarkers, setPhotoMarkers] = useState(
    (behaviorDefaults.photo_expected_markers || []).join('\n')
  );
  const [photoReply, setPhotoReply] = useState(behaviorDefaults.photo_expected_reply || '');
  const [photoTtl, setPhotoTtl] = useState(
    behaviorDefaults.photo_expected_ttl ? String(behaviorDefaults.photo_expected_ttl) : ''
  );
  const initialTriggers = (behaviorDefaults.triggers || []).map((rule) => ({
    phrases: rule.phrases || [],
    channels: rule.channels || ['telegram', 'avito', 'whatsapp'],
    silence: rule.silence !== false,
    notify: Boolean(rule.notify),
  }));
  const [triggers, setTriggers] = useState<TriggerRule[]>(
    initialTriggers.length ? initialTriggers : [emptyTrigger()]
  );

  const [followups, setFollowups] = useState<FollowUpRule[]>([]);
  const [followupsLoading, setFollowupsLoading] = useState(false);
  const factOptions = useMemo(() => {
    const seen = new Map<string, string>();
    followups.forEach((rule, idx) => {
      const capture = rule.capture;
      if (!capture || !capture.key) return;
      const label = (capture.label || rule.text || capture.key || `Факт ${idx + 1}`).trim();
      if (!seen.has(capture.key)) {
        seen.set(capture.key, label);
      }
    });
    return Array.from(seen.entries()).map(([key, label]) => ({ key, label }));
  }, [followups]);

  const settingsReady = Boolean(settings && settings.cfg);

  useEffect(() => {
    if (!settingsReady || hasDraft) return;
    const cfg = settings?.cfg || {};
    const passport = (cfg as Record<string, any>).passport || {};
    if (!brand && passport.brand) setBrand(passport.brand);
    if (!agent && passport.agent_name) setAgent(passport.agent_name);
    if (!city && passport.city) setCity(passport.city);
    if (!currency && passport.currency) setCurrency(passport.currency);
    if (!tone && passport.tone) setTone(passport.tone);
    if (!personaBase && settings?.persona) setPersonaBase(settings.persona || '');
    const personas = settings?.personas || {};
    if (!personaTelegram && typeof personas.telegram === 'string') {
      setPersonaTelegram(personas.telegram || '');
    }
    if (!personaAvito && typeof personas.avito === 'string') {
      setPersonaAvito(personas.avito || '');
    }
  }, [
    settingsReady,
    settings,
    brand,
    agent,
    city,
    currency,
    tone,
    personaBase,
    personaTelegram,
    personaAvito,
    hasDraft,
  ]);

  useEffect(() => {
    if (!settings?.persona) return;
    if (!personaBase) {
      setPersonaBase(settings.persona || '');
    }
  }, [settings, personaBase]);

  useEffect(() => {
    if (draftInitialized) return;
    const raw = sessionStorage.getItem(draftKey);
    if (!raw) {
      setDraftInitialized(true);
      return;
    }
    try {
      const draft = JSON.parse(raw) as Record<string, any>;
      if (typeof draft.brand === 'string') setBrand(draft.brand);
      if (typeof draft.agent === 'string') setAgent(draft.agent);
      if (typeof draft.city === 'string') setCity(draft.city);
      if (typeof draft.currency === 'string') setCurrency(draft.currency);
      if (typeof draft.tone === 'string') setTone(draft.tone);
      if (typeof draft.personaBase === 'string') setPersonaBase(draft.personaBase);
      if (typeof draft.personaTelegram === 'string') setPersonaTelegram(draft.personaTelegram);
      if (typeof draft.personaAvito === 'string') setPersonaAvito(draft.personaAvito);
      if (typeof draft.personaChannel === 'string') {
        setPersonaChannel(draft.personaChannel as 'base' | 'telegram' | 'avito');
      }
      if (typeof draft.autoReply === 'boolean') setAutoReply(draft.autoReply);
      if (typeof draft.autoReplyText === 'string') setAutoReplyText(draft.autoReplyText);
      if (typeof draft.avitoPhoneTemplate === 'string') setAvitoPhoneTemplate(draft.avitoPhoneTemplate);
      if (typeof draft.avitoSmartReply === 'boolean') setAvitoSmartReply(draft.avitoSmartReply);
      if (typeof draft.telegramReplyEnabled === 'boolean') {
        setTelegramReplyEnabled(draft.telegramReplyEnabled);
      }
      if (typeof draft.sendCatalogTg === 'boolean') setSendCatalogTg(draft.sendCatalogTg);
      if (typeof draft.autoPhotoEnabled === 'boolean') setAutoPhotoEnabled(draft.autoPhotoEnabled);
      if (typeof draft.autoPhotoMax === 'string') setAutoPhotoMax(draft.autoPhotoMax);
      if (typeof draft.photoMarkers === 'string') setPhotoMarkers(draft.photoMarkers);
      if (typeof draft.photoReply === 'string') setPhotoReply(draft.photoReply);
      if (typeof draft.photoTtl === 'string') setPhotoTtl(draft.photoTtl);
      if (Array.isArray(draft.triggers)) setTriggers(draft.triggers as TriggerRule[]);
      if (Array.isArray(draft.followups)) setFollowups(draft.followups as FollowUpRule[]);
      setHasDraft(true);
    } catch (error) {
      setHasDraft(false);
    } finally {
      setDraftInitialized(true);
    }
  }, [draftKey, draftInitialized]);

  useEffect(() => {
    if (!draftInitialized) return;
    const payload = {
      brand,
      agent,
      city,
      currency,
      tone,
      personaChannel,
      personaBase,
      personaTelegram,
      personaAvito,
      autoReply,
      autoReplyText,
      avitoPhoneTemplate,
      avitoSmartReply,
      telegramReplyEnabled,
      sendCatalogTg,
      autoPhotoEnabled,
      autoPhotoMax,
      photoMarkers,
      photoReply,
      photoTtl,
      triggers,
      followups,
    };
    sessionStorage.setItem(draftKey, JSON.stringify(payload));
  }, [
    draftInitialized,
    draftKey,
    brand,
    agent,
    city,
    currency,
    tone,
    personaChannel,
    personaBase,
    personaTelegram,
    personaAvito,
    autoReply,
    autoReplyText,
    avitoPhoneTemplate,
    avitoSmartReply,
    telegramReplyEnabled,
    sendCatalogTg,
    autoPhotoEnabled,
    autoPhotoMax,
    photoMarkers,
    photoReply,
    photoTtl,
    triggers,
    followups,
  ]);

  useEffect(() => {
    const loadFollowups = async () => {
      const endpoint = bootstrap.urls?.get_followups || `/client/${api.tenantId}/follow-ups`;
      if (!api.tenantId || !api.key) return;
      setFollowupsLoading(true);
      try {
        const data = await requestJson<{ ok: boolean; rules: FollowUpRule[] }>(
          buildUrl(endpoint, api)
        );
        if (data.rules) {
          const normalized = data.rules.map((rule) => {
            const condition = Array.isArray(rule.condition) ? rule.condition[0] : rule.condition;
            const capture = rule.capture && typeof rule.capture === 'object' ? rule.capture : undefined;
            return {
              ...rule,
              condition: condition || undefined,
              capture: capture || undefined,
            };
          });
          setFollowups(normalized);
        }
      } catch (error) {
        toast.error('Не удалось загрузить фоллоу-апы');
      } finally {
        setFollowupsLoading(false);
      }
    };
    loadFollowups().catch(() => undefined);
  }, [api.tenantId, api.key, bootstrap.urls]);

  const handleSavePassport = async () => {
    const endpoint = bootstrap.urls?.save_settings || `/client/${api.tenantId}/settings/save`;
    if (!api.tenantId || !api.key) return;
    try {
      await postJson(buildUrl(endpoint, api), {
        brand,
        agent,
        city,
        currency,
        tone,
      });
      toast.success('Паспорт сохранён');
      refreshSettings().catch(() => undefined);
    } catch (error) {
      toast.error('Не удалось сохранить паспорт');
    }
  };

  const handleSavePersona = async () => {
    const endpoint = bootstrap.urls?.save_persona || `/client/${api.tenantId}/persona`;
    if (!api.tenantId || !api.key) return;
    const text =
      personaChannel === 'telegram'
        ? personaTelegram
        : personaChannel === 'avito'
        ? personaAvito
        : personaBase;
    const channel = personaChannel === 'base' ? undefined : personaChannel;
    try {
      await postJson(buildUrl(endpoint, api), { text, channel });
      toast.success('Персона обновлена');
      refreshSettings().catch(() => undefined);
    } catch (error) {
      toast.error('Не удалось сохранить персону');
    }
  };

  const handleDownloadConfig = async () => {
    const endpoint = bootstrap.urls?.settings_get || '/pub/settings/get';
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<Record<string, unknown>>(buildUrl(endpoint, api, { _: Date.now() }));
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = `tenant-${api.tenantId}-config.json`;
      anchor.click();
      URL.revokeObjectURL(url);
      toast.success('Конфиг скачан');
    } catch (error) {
      toast.error('Не удалось скачать конфиг');
    }
  };

  const handleSaveBehavior = async () => {
    const endpoint = bootstrap.urls?.save_behavior || `/client/${api.tenantId}/behavior/save`;
    if (!api.tenantId || !api.key) return;
    const payload = {
      auto_reply: autoReply,
      auto_reply_text: autoReplyText,
      avito_phone_tg_template: avitoPhoneTemplate,
      avito_smart_reply_enabled: avitoSmartReply,
      telegram_reply_enabled: telegramReplyEnabled,
      send_catalog_on_first_message: sendCatalogTg,
      auto_photo_enabled: autoPhotoEnabled,
      auto_photo_max: autoPhotoMax ? Number(autoPhotoMax) : 0,
      triggers,
      photo_expected_markers: photoMarkers
        .split(/\n|,/)
        .map((item) => item.trim())
        .filter(Boolean),
      photo_expected_reply: photoReply,
      photo_expected_ttl: Number.parseInt(photoTtl || '0', 10) || 0,
    };
    try {
      await postJson(buildUrl(endpoint, api), payload);
      toast.success('Поведение сохранено');
      setSettings((prev) => ({
        ...(prev || {}),
        cfg: { ...((prev && prev.cfg) || {}), behavior: payload },
      }));
    } catch (error) {
      toast.error('Не удалось сохранить поведение');
    }
  };

  const updateTrigger = (index: number, next: Partial<TriggerRule>) => {
    setTriggers((prev) =>
      prev.map((item, idx) => (idx === index ? { ...item, ...next } : item))
    );
  };

  const addTrigger = () => {
    setTriggers((prev) => [...prev, emptyTrigger()]);
  };

  const removeTrigger = (index: number) => {
    setTriggers((prev) => prev.filter((_, idx) => idx !== index));
  };

  const addFollowup = () => {
    setFollowups((prev) => [...prev, emptyFollowup()]);
  };

  const updateFollowup = (index: number, patch: Partial<FollowUpRule>) => {
    setFollowups((prev) => prev.map((item, idx) => (idx === index ? { ...item, ...patch } : item)));
  };

  const removeFollowup = (index: number) => {
    setFollowups((prev) => prev.filter((_, idx) => idx !== index));
  };

  const handleSaveFollowups = async () => {
    const endpoint = bootstrap.urls?.save_followups || `/client/${api.tenantId}/follow-ups`;
    if (!api.tenantId || !api.key) return;
    try {
      const payload = { rules: followups };
      await postJson(buildUrl(endpoint, api), payload);
      toast.success('Фоллоу-апы сохранены');
    } catch (error) {
      toast.error('Не удалось сохранить фоллоу-апы');
    }
  };

  const personaValue =
    personaChannel === 'telegram'
      ? personaTelegram
      : personaChannel === 'avito'
      ? personaAvito
      : personaBase;

  const handlePersonaChange = (next: string) => {
    if (personaChannel === 'telegram') {
      setPersonaTelegram(next);
    } else if (personaChannel === 'avito') {
      setPersonaAvito(next);
    } else {
      setPersonaBase(next);
    }
  };

  return (
    <div className="space-y-8">
      <div className="grid gap-6 lg:grid-cols-[2fr,1fr]">
        <div className="card space-y-6">
          <div>
            <div className="card-title">Паспорт бренда</div>
            <div className="card-subtitle">Основные данные бренда и голоса ассистента.</div>
          </div>
          <div className="grid gap-4 md:grid-cols-2">
            <label className="space-y-2">
              <span className="text-sm font-medium text-slate-600">Бренд</span>
              <input className="input" value={brand} onChange={(e) => setBrand(e.target.value)} />
            </label>
            <label className="space-y-2">
              <span className="text-sm font-medium text-slate-600">Имя ассистента</span>
              <input className="input" value={agent} onChange={(e) => setAgent(e.target.value)} />
            </label>
            <label className="space-y-2">
              <span className="text-sm font-medium text-slate-600">Город</span>
              <input className="input" value={city} onChange={(e) => setCity(e.target.value)} />
            </label>
            <label className="space-y-2">
              <span className="text-sm font-medium text-slate-600">Валюта</span>
              <input className="input" value={currency} onChange={(e) => setCurrency(e.target.value)} />
            </label>
            <label className="space-y-2 md:col-span-2">
              <span className="text-sm font-medium text-slate-600">Тональность</span>
              <input className="input" value={tone} onChange={(e) => setTone(e.target.value)} />
            </label>
          </div>
          <div className="flex flex-wrap gap-3">
            <button className="btn" onClick={handleSavePassport}>Сохранить паспорт</button>
          </div>
        </div>
        <div className="card space-y-3">
          <div className="card-title">Tenant</div>
          <div className="text-3xl font-semibold text-slate-900">#{api.tenantId || '—'}</div>
          <div className="text-xs uppercase tracking-[0.2em] text-slate-400">Ключ доступа</div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-xs text-slate-600 break-all">
            {api.key || '—'}
          </div>
        </div>
      </div>

      <div className="card space-y-6">
        <div>
          <div className="card-title">Персона</div>
          <div className="card-subtitle">Описание голоса ассистента, цели и стиль общения.</div>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-400">
            Канал персоны
          </span>
          <select
            className="input w-full max-w-[220px]"
            value={personaChannel}
            onChange={(e) => setPersonaChannel(e.target.value as 'base' | 'telegram' | 'avito')}
          >
            <option value="base">Основная (общая)</option>
            <option value="telegram">Telegram</option>
            <option value="avito">Avito</option>
          </select>
        </div>
        <textarea className="textarea" rows={8} value={personaValue} onChange={(e) => handlePersonaChange(e.target.value)} />
        <div className="flex flex-wrap gap-3">
          <button className="btn" onClick={handleSavePersona}>Сохранить персону</button>
          <button className="btn-secondary" onClick={handleDownloadConfig}>Скачать JSON конфиг</button>
        </div>
      </div>

      <div className="card space-y-6">
        <div>
          <div className="card-title">Поведение и триггеры</div>
          <div className="card-subtitle">Автоответы Avito, смарт-реплай и правила тишины.</div>
        </div>
        <div className="grid gap-4 md:grid-cols-2">
          <label className="flex items-center gap-3 rounded-xl border border-slate-200 px-4 py-3">
            <input type="checkbox" checked={autoReply} onChange={(e) => setAutoReply(e.target.checked)} />
            <span className="text-sm font-medium text-slate-700">Автоответ Avito</span>
          </label>
          <label className="flex items-center gap-3 rounded-xl border border-slate-200 px-4 py-3">
            <input type="checkbox" checked={avitoSmartReply} onChange={(e) => setAvitoSmartReply(e.target.checked)} />
            <span className="text-sm font-medium text-slate-700">Смарт-реплай Avito</span>
          </label>
          <label className="flex items-center gap-3 rounded-xl border border-slate-200 px-4 py-3">
            <input
              type="checkbox"
              checked={telegramReplyEnabled}
              onChange={(e) => setTelegramReplyEnabled(e.target.checked)}
            />
            <span className="text-sm font-medium text-slate-700">Автоответ Telegram</span>
          </label>
          <label className="flex items-center gap-3 rounded-xl border border-slate-200 px-4 py-3 md:col-span-2">
            <input type="checkbox" checked={sendCatalogTg} onChange={(e) => setSendCatalogTg(e.target.checked)} />
            <span className="text-sm font-medium text-slate-700">Отправлять PDF-каталог первым сообщением (Telegram)</span>
          </label>
          <label className="flex items-center gap-3 rounded-xl border border-slate-200 px-4 py-3">
            <input type="checkbox" checked={autoPhotoEnabled} onChange={(e) => setAutoPhotoEnabled(e.target.checked)} />
            <span className="text-sm font-medium text-slate-700">Авто‑отправка фото</span>
          </label>
          <label className="space-y-2">
            <span className="text-sm font-medium text-slate-600">Максимум фото за ответ</span>
            <input
              className="input"
              type="number"
              min={0}
              value={autoPhotoMax}
              onChange={(e) => setAutoPhotoMax(e.target.value)}
            />
          </label>
          <label className="space-y-2 md:col-span-2">
            <span className="text-sm font-medium text-slate-600">Текст автоответа Avito</span>
            <textarea className="textarea" rows={3} value={autoReplyText} onChange={(e) => setAutoReplyText(e.target.value)} />
          </label>
          <label className="space-y-2 md:col-span-2">
            <span className="text-sm font-medium text-slate-600">Текст для Telegram, если нашли номер в Avito</span>
            <textarea className="textarea" rows={3} value={avitoPhoneTemplate} onChange={(e) => setAvitoPhoneTemplate(e.target.value)} />
          </label>
          <label className="space-y-2 md:col-span-2">
            <span className="text-sm font-medium text-slate-600">Фразы, после которых ждём фото/файл</span>
            <textarea className="textarea" rows={3} value={photoMarkers} onChange={(e) => setPhotoMarkers(e.target.value)} />
          </label>
          <label className="space-y-2 md:col-span-2">
            <span className="text-sm font-medium text-slate-600">Ответ на фото/файл, если ждали</span>
            <textarea className="textarea" rows={3} value={photoReply} onChange={(e) => setPhotoReply(e.target.value)} />
          </label>
          <label className="space-y-2">
            <span className="text-sm font-medium text-slate-600">TTL ожидания (сек)</span>
            <input className="input" type="number" value={photoTtl} onChange={(e) => setPhotoTtl(e.target.value)} />
          </label>
        </div>

        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-base font-semibold text-slate-900">Триггеры тишины</div>
              <div className="text-sm text-slate-500">Фразы, при которых бот замолкает и зовёт менеджера.</div>
            </div>
            <button className="btn-secondary" onClick={addTrigger}>Добавить правило</button>
          </div>

          <div className="space-y-4">
            {triggers.map((trigger, index) => (
              <div key={index} className="rounded-2xl border border-slate-200 bg-slate-50 p-4 space-y-3">
                <div className="grid gap-3 lg:grid-cols-[2fr,1fr]">
                  <label className="space-y-2">
                    <span className="text-sm font-medium text-slate-600">Фразы</span>
                    <textarea
                      className="textarea"
                      rows={3}
                      value={trigger.phrases.join('\n')}
                      onChange={(e) =>
                        updateTrigger(index, {
                          phrases: e.target.value
                            .split(/\n|,/)
                            .map((item) => item.trim())
                            .filter(Boolean),
                        })
                      }
                    />
                  </label>
                  <label className="space-y-2">
                    <span className="text-sm font-medium text-slate-600">Каналы</span>
                    <select
                      className="input"
                      multiple
                      value={trigger.channels}
                      onChange={(e) => {
                        const values = Array.from(e.target.selectedOptions).map((opt) => opt.value);
                        updateTrigger(index, { channels: values.length ? values : ['telegram', 'avito', 'whatsapp'] });
                      }}
                    >
                      {channelOptions.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <div className="flex flex-wrap gap-4">
                  <label className="flex items-center gap-2 text-sm text-slate-600">
                    <input
                      type="checkbox"
                      checked={trigger.silence}
                      onChange={(e) => updateTrigger(index, { silence: e.target.checked })}
                    />
                    Тишина
                  </label>
                  <label className="flex items-center gap-2 text-sm text-slate-600">
                    <input
                      type="checkbox"
                      checked={trigger.notify}
                      onChange={(e) => updateTrigger(index, { notify: e.target.checked })}
                    />
                    Уведомить менеджера
                  </label>
                  {triggers.length > 1 && (
                    <button className="btn-ghost ml-auto" onClick={() => removeTrigger(index)}>
                      Удалить
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="flex flex-wrap gap-3">
          <button className="btn" onClick={handleSaveBehavior}>Сохранить поведение</button>
        </div>
      </div>

      <div className="card space-y-6">
        <div>
          <div className="card-title">Фоллоу-апы</div>
          <div className="card-subtitle">Автоматические сообщения после последнего контакта.</div>
          <div className="text-xs text-slate-500">
            Добавьте вопрос с сохранением ответа, чтобы использовать его в условиях других фоллоу-апов.
          </div>
        </div>

        {followupsLoading ? (
          <div className="text-sm text-slate-500">Загрузка правил…</div>
        ) : (
          <div className="rounded-3xl border border-slate-200 bg-gradient-to-br from-amber-50 via-white to-sky-50 p-4 md:p-6">
            {followups.length === 0 && (
              <div className="rounded-2xl border border-dashed border-slate-200 bg-white/70 p-6 text-sm text-slate-500">
                Правил пока нет. Добавьте первое правило.
              </div>
            )}
            <div className="space-y-6">
              {followups.map((rule, index) => {
                const condition = Array.isArray(rule.condition) ? rule.condition[0] : rule.condition;
                const conditionMode = condition ? 'conditional' : 'always';
                const conditionKey = condition?.key || '';
                const conditionOp = condition?.op || 'eq';
                const conditionValue =
                  typeof condition?.value === 'string'
                    ? condition.value
                    : Array.isArray(condition?.value)
                    ? condition.value.join(', ')
                    : '';
                const hasFacts = factOptions.length > 0;
                const matchedFact = factOptions.find((option) => option.key === conditionKey);
                const factSelectValue = matchedFact
                  ? matchedFact.key
                  : conditionKey
                  ? '__custom__'
                  : hasFacts
                  ? factOptions[0].key
                  : '__custom__';
                const resolvedKey = factSelectValue === '__custom__' ? conditionKey : factSelectValue;
                const resolvedLabel = matchedFact?.label || conditionKey || 'Факт';
                let preset = 'custom';
                if (conditionOp === 'eq' && conditionValue === 'yes') preset = 'yes';
                else if (conditionOp === 'eq' && conditionValue === 'no') preset = 'no';
                else if (conditionOp === 'neq' && conditionValue === 'yes') preset = 'not_yes';
                else if (conditionOp === 'neq' && conditionValue === 'no') preset = 'not_no';
                else if (conditionOp === 'exists') preset = 'exists';
                else if (conditionOp === 'not_exists') preset = 'not_exists';
                const showCustomFields = preset === 'custom';
                const showConditionValue = showCustomFields && !['exists', 'not_exists'].includes(conditionOp);
                const ensureCondition = (nextKey: string, opValue: string, value?: string) => {
                  const payload: FollowUpCondition = { key: nextKey, op: opValue };
                  if (value && !['exists', 'not_exists'].includes(opValue)) {
                    payload.value = value;
                  }
                  return payload;
                };
                const capture = rule.capture && typeof rule.capture === 'object' ? rule.capture : undefined;
                const captureEnabled = Boolean(capture);
                const yesText = Array.isArray(capture?.yes)
                  ? capture?.yes.join('\n')
                  : typeof capture?.yes === 'string'
                  ? capture.yes
                  : '';
                const noText = Array.isArray(capture?.no)
                  ? capture?.no.join('\n')
                  : typeof capture?.no === 'string'
                  ? capture.no
                  : '';
                const labelValue = capture?.label || '';
                const keyValue = capture?.key || '';
                const fallbackLabel = rule.text || `Факт ${index + 1}`;
                const fallbackKey = normalizeFactKey(labelValue || fallbackLabel, `fact_${index + 1}`);
                const conditionSummary =
                  conditionMode === 'always'
                    ? 'Всегда'
                    : `Если ${resolvedLabel} · ${preset === 'custom' ? conditionOp : preset}`;
                const instantSend = Boolean(rule.trigger_on_answer);

                return (
                  <div key={index} className="grid gap-4 lg:grid-cols-[64px,1fr]">
                    <div className="relative hidden lg:flex flex-col items-center">
                      {index > 0 && <div className="absolute left-1/2 top-0 h-6 w-px -translate-x-1/2 bg-slate-200" />}
                      {index < followups.length - 1 && (
                        <div className="absolute left-1/2 top-6 bottom-0 w-px -translate-x-1/2 bg-slate-200" />
                      )}
                      <div
                        className={`z-10 flex h-11 w-11 items-center justify-center rounded-full border ${
                          rule.active ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : 'border-slate-200 bg-white text-slate-400'
                        }`}
                      >
                        {index + 1}
                      </div>
                      <div className="mt-2 text-xs text-slate-500">
                        {instantSend ? 'Сразу' : `+${rule.delay_minutes} мин`}
                      </div>
                    </div>

                    <div className="rounded-2xl border border-slate-200 bg-white/80 p-4 shadow-sm backdrop-blur">
                      <div className="flex flex-wrap items-center gap-3">
                        <div className="text-xs uppercase tracking-wide text-slate-400">Шаг {index + 1}</div>
                        <span className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs text-slate-600">
                          {instantSend ? 'Сразу после ответа' : `+${rule.delay_minutes} мин`}
                        </span>
                        <span className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs text-slate-600">
                          {rule.channel === 'any' ? 'Любой канал' : rule.channel}
                        </span>
                        <span className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs text-slate-600">
                          Попыток: {rule.max_attempts}
                        </span>
                        <label className="ml-auto flex items-center gap-2 text-xs text-slate-500">
                          <input
                            type="checkbox"
                            checked={rule.active}
                            onChange={(e) => updateFollowup(index, { active: e.target.checked })}
                          />
                          Активно
                        </label>
                      </div>

                      <div className="mt-4 grid gap-3 md:grid-cols-[1.4fr,1fr]">
                        <label className="space-y-2">
                          <span className="text-xs uppercase tracking-wide text-slate-400">Сообщение</span>
                          <textarea
                            className="textarea"
                            rows={3}
                            value={rule.text}
                            onChange={(e) => updateFollowup(index, { text: e.target.value })}
                          />
                        </label>
                        <div className="space-y-3 rounded-xl border border-slate-200 bg-white p-3">
                          <div className="text-xs uppercase tracking-wide text-slate-400">Параметры</div>
                          <label className="space-y-2">
                            <span className="text-xs uppercase tracking-wide text-slate-400">Канал</span>
                            <select
                              className="input"
                              value={rule.channel}
                              onChange={(e) => updateFollowup(index, { channel: e.target.value })}
                            >
                              {channelOptions.map((option) => (
                                <option key={option.value} value={option.value}>
                                  {option.label}
                                </option>
                              ))}
                            </select>
                          </label>
                          <div className="grid gap-2 md:grid-cols-2">
                            <label className="space-y-2">
                              <span className="text-xs uppercase tracking-wide text-slate-400">Задержка (мин)</span>
                              <input
                                className="input"
                                type="number"
                                value={rule.delay_minutes}
                                onChange={(e) => updateFollowup(index, { delay_minutes: Number(e.target.value) })}
                                disabled={instantSend}
                              />
                            </label>
                            <label className="space-y-2">
                              <span className="text-xs uppercase tracking-wide text-slate-400">Попыток</span>
                              <input
                                className="input"
                                type="number"
                                value={rule.max_attempts}
                                onChange={(e) => updateFollowup(index, { max_attempts: Number(e.target.value) })}
                              />
                            </label>
                          </div>
                        </div>
                      </div>

                      <div className="mt-4 grid gap-4 md:grid-cols-[1.2fr,1fr]">
                        <div className="rounded-xl border border-slate-200 bg-white p-4">
                          <div className="flex flex-wrap items-center gap-2">
                            <span className="text-xs uppercase tracking-wide text-slate-400">Условие</span>
                            <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs text-slate-600">
                              {conditionSummary}
                            </span>
                          </div>
                          <div className="mt-3 flex flex-wrap gap-2">
                            <button
                              className={`rounded-full border px-3 py-1 text-xs ${
                                conditionMode === 'always'
                                  ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                  : 'border-slate-200 bg-white text-slate-500'
                              }`}
                              onClick={() => updateFollowup(index, { condition: undefined })}
                            >
                              Всегда
                            </button>
                            <button
                              className={`rounded-full border px-3 py-1 text-xs ${
                                conditionMode === 'conditional'
                                  ? 'border-amber-200 bg-amber-50 text-amber-700'
                                  : 'border-slate-200 bg-white text-slate-500'
                              }`}
                              onClick={() => {
                                const nextKey = hasFacts ? factOptions[0].key : '';
                                updateFollowup(index, {
                                  condition: ensureCondition(nextKey, 'eq', 'yes'),
                                });
                              }}
                            >
                              По условию
                            </button>
                          </div>
                          {conditionMode === 'conditional' && (
                            <label className="mt-3 flex items-center gap-2 text-xs text-slate-600">
                              <input
                                type="checkbox"
                                checked={instantSend}
                                onChange={(e) => {
                                  const enabled = e.target.checked;
                                  updateFollowup(index, {
                                    trigger_on_answer: enabled,
                                    delay_minutes: enabled ? 0 : rule.delay_minutes || 10,
                                  });
                                }}
                              />
                              Отправить сразу после ответа
                            </label>
                          )}

                          {conditionMode === 'conditional' && (
                            <div className="mt-3 space-y-3">
                              <label className="space-y-2">
                                <span className="text-xs uppercase tracking-wide text-slate-400">Факт</span>
                                <select
                                  className="input"
                                  value={factSelectValue}
                                  onChange={(e) => {
                                    const value = e.target.value;
                                    if (value === '__custom__') {
                                      updateFollowup(index, {
                                        condition: ensureCondition(conditionKey || '', conditionOp, conditionValue),
                                      });
                                      return;
                                    }
                                    updateFollowup(index, {
                                      condition: ensureCondition(value, conditionOp || 'eq', conditionValue || 'yes'),
                                    });
                                  }}
                                >
                                  {factOptions.map((option) => (
                                    <option key={option.key} value={option.key}>
                                      {option.label}
                                    </option>
                                  ))}
                                  <option value="__custom__">Свой факт...</option>
                                </select>
                              </label>
                              {factSelectValue === '__custom__' && (
                                <label className="space-y-2">
                                  <span className="text-xs uppercase tracking-wide text-slate-400">Название факта</span>
                                  <input
                                    className="input"
                                    value={conditionKey}
                                    onChange={(e) =>
                                      updateFollowup(index, {
                                        condition: ensureCondition(e.target.value, conditionOp || 'eq', conditionValue),
                                      })
                                    }
                                  />
                                </label>
                              )}
                              <label className="space-y-2">
                                <span className="text-xs uppercase tracking-wide text-slate-400">Что должно быть</span>
                                <select
                                  className="input"
                                  value={preset}
                                  onChange={(e) => {
                                    const selected = e.target.value;
                                    if (selected === 'yes') {
                                      updateFollowup(index, { condition: ensureCondition(resolvedKey, 'eq', 'yes') });
                                    } else if (selected === 'no') {
                                      updateFollowup(index, { condition: ensureCondition(resolvedKey, 'eq', 'no') });
                                    } else if (selected === 'not_yes') {
                                      updateFollowup(index, { condition: ensureCondition(resolvedKey, 'neq', 'yes') });
                                    } else if (selected === 'not_no') {
                                      updateFollowup(index, { condition: ensureCondition(resolvedKey, 'neq', 'no') });
                                    } else if (selected === 'exists') {
                                      updateFollowup(index, { condition: ensureCondition(resolvedKey, 'exists') });
                                    } else if (selected === 'not_exists') {
                                      updateFollowup(index, { condition: ensureCondition(resolvedKey, 'not_exists') });
                                    } else {
                                      updateFollowup(index, {
                                        condition: ensureCondition(resolvedKey, conditionOp || 'eq', conditionValue || ''),
                                      });
                                    }
                                  }}
                                >
                                  <option value="yes">Ответ “Да”</option>
                                  <option value="no">Ответ “Нет”</option>
                                  <option value="not_yes">Не “Да” (или нет ответа)</option>
                                  <option value="not_no">Не “Нет” (или нет ответа)</option>
                                  <option value="exists">Есть ответ/значение</option>
                                  <option value="not_exists">Нет ответа/значения</option>
                                  <option value="custom">Другое условие</option>
                                </select>
                              </label>
                              {showCustomFields && (
                                <div className="grid gap-3 md:grid-cols-2">
                                  <label className="space-y-2">
                                    <span className="text-xs uppercase tracking-wide text-slate-400">Оператор</span>
                                    <select
                                      className="input"
                                      value={conditionOp}
                                      onChange={(e) => {
                                        const op = e.target.value;
                                        updateFollowup(index, { condition: ensureCondition(resolvedKey, op, conditionValue) });
                                      }}
                                    >
                                      <option value="eq">=</option>
                                      <option value="neq">≠</option>
                                      <option value="exists">есть</option>
                                      <option value="not_exists">нет</option>
                                      <option value="in">в списке</option>
                                      <option value="not_in">не в списке</option>
                                    </select>
                                  </label>
                                  {showConditionValue && (
                                    <label className="space-y-2">
                                      <span className="text-xs uppercase tracking-wide text-slate-400">Значение</span>
                                      <input
                                        className="input"
                                        placeholder={conditionOp === 'in' || conditionOp === 'not_in' ? 'значения через запятую' : ''}
                                        value={conditionValue}
                                        onChange={(e) =>
                                          updateFollowup(index, {
                                            condition: ensureCondition(resolvedKey, conditionOp, e.target.value),
                                          })
                                        }
                                      />
                                    </label>
                                  )}
                                </div>
                              )}
                            </div>
                          )}
                        </div>

                        <div className="rounded-xl border border-slate-200 bg-white p-4">
                          <div className="flex items-center justify-between">
                            <div className="text-xs uppercase tracking-wide text-slate-400">Факт из ответа</div>
                            <label className="flex items-center gap-2 text-xs text-slate-600">
                              <input
                                type="checkbox"
                                checked={captureEnabled}
                                onChange={(e) => {
                                  if (e.target.checked) {
                                    updateFollowup(index, {
                                      capture: {
                                        key: keyValue || fallbackKey,
                                        label: labelValue || fallbackLabel,
                                        yes: capture?.yes?.length ? capture.yes : ['да'],
                                        no: capture?.no?.length ? capture.no : ['нет'],
                                      },
                                    });
                                  } else {
                                    updateFollowup(index, { capture: undefined });
                                  }
                                }}
                              />
                              Сохранять ответ
                            </label>
                          </div>
                          {captureEnabled ? (
                            <div className="mt-3 space-y-3">
                              <label className="space-y-2">
                                <span className="text-xs uppercase tracking-wide text-slate-400">Название факта</span>
                                <input
                                  className="input"
                                  placeholder="Например: Заказ оформлен"
                                  value={labelValue}
                                  onChange={(e) => {
                                    const nextLabel = e.target.value;
                                    const nextKey = keyValue || normalizeFactKey(nextLabel || fallbackLabel, fallbackKey);
                                    updateFollowup(index, {
                                      capture: {
                                        ...(capture as FollowUpCapture),
                                        label: nextLabel,
                                        key: nextKey,
                                      },
                                    });
                                  }}
                                />
                              </label>
                              <details className="rounded-xl border border-slate-200 bg-white p-3">
                                <summary className="text-xs font-semibold uppercase tracking-wide text-slate-400 cursor-pointer">
                                  Синонимы ответов
                                </summary>
                                <div className="mt-3 grid gap-3 md:grid-cols-2">
                                  <label className="space-y-2">
                                    <span className="text-xs uppercase tracking-wide text-slate-400">Ответы "Да"</span>
                                    <textarea
                                      className="textarea"
                                      rows={2}
                                      value={yesText}
                                      onChange={(e) =>
                                    updateFollowup(index, {
                                      capture: {
                                        ...(capture as FollowUpCapture),
                                        yes: e.target.value,
                                      },
                                    })
                                  }
                                />
                              </label>
                              <label className="space-y-2">
                                <span className="text-xs uppercase tracking-wide text-slate-400">Ответы "Нет"</span>
                                <textarea
                                  className="textarea"
                                  rows={2}
                                  value={noText}
                                  onChange={(e) =>
                                    updateFollowup(index, {
                                      capture: {
                                        ...(capture as FollowUpCapture),
                                        no: e.target.value,
                                      },
                                    })
                                  }
                                />
                                  </label>
                                </div>
                              </details>
                              <details className="rounded-xl border border-slate-200 bg-white p-3">
                                <summary className="text-xs font-semibold uppercase tracking-wide text-slate-400 cursor-pointer">
                                  Технический ключ
                                </summary>
                                <div className="mt-3 space-y-2">
                                  <input
                                    className="input"
                                    value={keyValue || fallbackKey}
                                    onChange={(e) =>
                                      updateFollowup(index, {
                                        capture: { ...(capture as FollowUpCapture), key: e.target.value },
                                      })
                                    }
                                  />
                                  <div className="text-xs text-slate-500">
                                    Используется в условиях. Меняйте только если понимаете последствия.
                                  </div>
                                </div>
                              </details>
                            </div>
                          ) : (
                            <div className="mt-3 text-xs text-slate-500">
                              Нет сохранения ответа — условные шаги не будут зависеть от этого сообщения.
                            </div>
                          )}
                        </div>
                      </div>

                      <div className="mt-4 flex justify-end">
                        <button className="btn-ghost" onClick={() => removeFollowup(index)}>
                          Удалить
                        </button>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        <div className="flex flex-wrap gap-3">
          <button className="btn-secondary" onClick={addFollowup}>Добавить правило</button>
          <button className="btn" onClick={handleSaveFollowups}>Сохранить фоллоу-апы</button>
        </div>
      </div>
    </div>
  );
};

export default SettingsTab;
