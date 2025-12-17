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

type FollowUpRule = {
  channel: string;
  delay_minutes: number;
  max_attempts: number;
  active: boolean;
  text: string;
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
});

const SettingsTab: React.FC = () => {
  const { bootstrap, api, settings, refreshSettings, setSettings } = useClient();

  const passportDefaults = bootstrap.form || {};
  const behaviorDefaults = bootstrap.behavior || {};

  const [brand, setBrand] = useState(passportDefaults.brand || '');
  const [agent, setAgent] = useState(passportDefaults.agent || '');
  const [city, setCity] = useState(passportDefaults.city || '');
  const [currency, setCurrency] = useState(passportDefaults.currency || '');
  const [tone, setTone] = useState(passportDefaults.tone || '');

  const [persona, setPersona] = useState('');

  const [autoReply, setAutoReply] = useState(Boolean(behaviorDefaults.auto_reply));
  const [autoReplyText, setAutoReplyText] = useState(behaviorDefaults.auto_reply_text || '');
  const [avitoPhoneTemplate, setAvitoPhoneTemplate] = useState(
    behaviorDefaults.avito_phone_tg_template || ''
  );
  const [avitoSmartReply, setAvitoSmartReply] = useState(
    Boolean(behaviorDefaults.avito_smart_reply_enabled)
  );
  const [sendCatalogTg, setSendCatalogTg] = useState(
    Boolean(behaviorDefaults.send_catalog_on_first_message)
  );
  const [photoMarkers, setPhotoMarkers] = useState(
    (behaviorDefaults.photo_expected_markers || []).join('\n')
  );
  const [photoReply, setPhotoReply] = useState(behaviorDefaults.photo_expected_reply || '');
  const [photoTtl, setPhotoTtl] = useState(
    behaviorDefaults.photo_expected_ttl ? String(behaviorDefaults.photo_expected_ttl) : ''
  );
  const [triggers, setTriggers] = useState<TriggerRule[]>(
    (behaviorDefaults.triggers || []).map((rule) => ({
      phrases: rule.phrases || [],
      channels: rule.channels || ['telegram', 'avito', 'whatsapp'],
      silence: rule.silence !== false,
      notify: Boolean(rule.notify),
    }))
  );

  const [followups, setFollowups] = useState<FollowUpRule[]>([]);
  const [followupsLoading, setFollowupsLoading] = useState(false);

  const settingsReady = Boolean(settings && settings.cfg);

  useEffect(() => {
    if (!settingsReady) return;
    const cfg = settings?.cfg || {};
    const passport = (cfg as Record<string, any>).passport || {};
    if (!brand && passport.brand) setBrand(passport.brand);
    if (!agent && passport.agent_name) setAgent(passport.agent_name);
    if (!city && passport.city) setCity(passport.city);
    if (!currency && passport.currency) setCurrency(passport.currency);
    if (!tone && passport.tone) setTone(passport.tone);
    if (!persona && settings?.persona) setPersona(settings.persona || '');
  }, [settingsReady, settings, brand, agent, city, currency, tone, persona]);

  useEffect(() => {
    if (!settings?.persona) return;
    if (!persona) {
      setPersona(settings.persona || '');
    }
  }, [settings, persona]);

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
          setFollowups(data.rules);
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
    try {
      await postJson(buildUrl(endpoint, api), { text: persona });
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
      send_catalog_on_first_message: sendCatalogTg,
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

  const formattedTriggers = useMemo(() => (triggers.length ? triggers : [emptyTrigger()]), [triggers]);

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
        <textarea className="textarea" rows={8} value={persona} onChange={(e) => setPersona(e.target.value)} />
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
          <label className="flex items-center gap-3 rounded-xl border border-slate-200 px-4 py-3 md:col-span-2">
            <input type="checkbox" checked={sendCatalogTg} onChange={(e) => setSendCatalogTg(e.target.checked)} />
            <span className="text-sm font-medium text-slate-700">Отправлять PDF-каталог первым сообщением (Telegram)</span>
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
            {formattedTriggers.map((trigger, index) => (
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
        </div>

        {followupsLoading ? (
          <div className="text-sm text-slate-500">Загрузка правил…</div>
        ) : (
          <div className="space-y-4">
            {followups.length === 0 && (
              <div className="rounded-2xl border border-dashed border-slate-200 p-6 text-sm text-slate-500">
                Правил пока нет. Добавьте первое правило.
              </div>
            )}
            {followups.map((rule, index) => (
              <div key={index} className="rounded-2xl border border-slate-200 bg-slate-50 p-4 space-y-3">
                <div className="grid gap-3 md:grid-cols-4">
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
                  <label className="space-y-2">
                    <span className="text-xs uppercase tracking-wide text-slate-400">Задержка (мин)</span>
                    <input
                      className="input"
                      type="number"
                      value={rule.delay_minutes}
                      onChange={(e) => updateFollowup(index, { delay_minutes: Number(e.target.value) })}
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
                  <label className="flex items-center gap-2 text-sm text-slate-600">
                    <input
                      type="checkbox"
                      checked={rule.active}
                      onChange={(e) => updateFollowup(index, { active: e.target.checked })}
                    />
                    Активно
                  </label>
                </div>
                <label className="space-y-2">
                  <span className="text-xs uppercase tracking-wide text-slate-400">Текст</span>
                  <textarea
                    className="textarea"
                    rows={2}
                    value={rule.text}
                    onChange={(e) => updateFollowup(index, { text: e.target.value })}
                  />
                </label>
                <div className="flex justify-end">
                  <button className="btn-ghost" onClick={() => removeFollowup(index)}>
                    Удалить
                  </button>
                </div>
              </div>
            ))}
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
