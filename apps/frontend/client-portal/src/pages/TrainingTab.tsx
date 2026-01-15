import React, { useEffect, useMemo, useRef, useState } from 'react';
import toast from 'react-hot-toast';
import { useClient } from '../context/ClientContext';
import { buildUrl, postJson, requestJson } from '../lib/api';

type DialogItem = {
  id: string;
  channel: string;
  title: string;
  contact?: string | null;
  last_message?: string | null;
  last_ts?: string | null;
};

type DialogMessage = {
  id: number | string;
  direction: number;
  text: string;
  ts?: string;
  status?: string;
  from_bot?: boolean;
  feedbacked?: boolean;
  isTemp?: boolean;
};

type SilenceInfo = {
  active: boolean;
  reason?: string | null;
  since?: string | null;
  ttl_seconds?: number | null;
  auto_reply_enabled?: boolean;
};

type FeedbackCounts = { like: number; dislike: number };

type PhotoItem = {
  id: string;
  original?: string;
  filename?: string;
  url?: string;
  size?: number;
};

const TrainingTab: React.FC = () => {
  const { api, bootstrap } = useClient();
  const [trainingStatus, setTrainingStatus] = useState('');
  const [trainingFile, setTrainingFile] = useState<File | null>(null);

  const [dialogs, setDialogs] = useState<DialogItem[]>([]);
  const [activeDialog, setActiveDialog] = useState<DialogItem | null>(null);
  const [messages, setMessages] = useState<DialogMessage[]>([]);
  const [silenceInfo, setSilenceInfo] = useState<SilenceInfo | null>(null);
  const [loadingDialogs, setLoadingDialogs] = useState(false);
  const [loadingMessages, setLoadingMessages] = useState(false);
  const [sendText, setSendText] = useState('');
  const [photos, setPhotos] = useState<PhotoItem[]>([]);
  const [selectedPhoto, setSelectedPhoto] = useState('');
  const [feedbackCounts, setFeedbackCounts] = useState<FeedbackCounts | null>(null);

  const messagesRef = useRef<HTMLDivElement | null>(null);

  const trainingUploadUrl = useMemo(() => {
    if (bootstrap.urls?.training_upload) {
      return bootstrap.urls.training_upload;
    }
    if (api.tenantId) {
      return `/client/${api.tenantId}/training/upload`;
    }
    return '/client/0/training/upload';
  }, [bootstrap.urls, api.tenantId]);

  const trainingStatusUrl = useMemo(() => {
    if (bootstrap.urls?.training_status) {
      return bootstrap.urls.training_status;
    }
    if (api.tenantId) {
      return `/client/${api.tenantId}/training/status`;
    }
    return '/client/0/training/status';
  }, [bootstrap.urls, api.tenantId]);

  const exportUrl = useMemo(() => bootstrap.urls?.whatsapp_export || '/pub/wa/export', [bootstrap.urls]);

  const dialogsListUrl = useMemo(() => bootstrap.urls?.dialogs_list || '/api/dialogs', [bootstrap.urls]);
  const dialogsDetailUrl = useMemo(() => bootstrap.urls?.dialogs_detail || '/api/dialogs/{lead_id}', [bootstrap.urls]);
  const dialogsSendUrl = useMemo(() => bootstrap.urls?.dialogs_send || '/api/dialogs/{lead_id}/send', [bootstrap.urls]);
  const feedbackUrl = useMemo(() => bootstrap.urls?.feedback || '/api/feedback', [bootstrap.urls]);
  const feedbackStatsUrl = useMemo(() => bootstrap.urls?.feedback_stats || '/api/feedback/stats', [bootstrap.urls]);
  const photosListUrl = useMemo(() => bootstrap.urls?.photos_list || '/pub/files/photos/list', [bootstrap.urls]);

  const channelBadge = (channel?: string) => {
    const value = (channel || '').toLowerCase();
    if (value === 'avito') {
      return { label: 'Avito', className: 'bg-orange-100 text-orange-700' };
    }
    if (value === 'telegram') {
      return { label: 'telegram', className: 'bg-sky-100 text-sky-700' };
    }
    return { label: channel || 'channel', className: 'bg-slate-100 text-slate-500' };
  };

  const silenceReasonLabel = (reason?: string | null) => {
    switch ((reason || '').toLowerCase()) {
      case 'manager_outgoing':
        return 'Менеджер ответил';
      case 'photo_received':
        return 'Получено фото';
      case 'trigger_match':
        return 'Сработал триггер тишины';
      case 'followup_sent':
        return 'Отправлен фоллоу‑ап';
      case 'silence_active':
      default:
        return 'Тишина включена';
    }
  };

  const refreshTrainingStatus = async () => {
    try {
      const data = await requestJson<Record<string, any>>(buildUrl(trainingStatusUrl, api));
      const info = data.info || {};
      const manifest = data.manifest || {};
      const pairs = manifest.pairs || info.pairs || 0;
      const ts = manifest.created_at || info.indexed_at || 0;
      const when = ts ? new Date(ts * 1000).toLocaleString() : '';
      const exportStats = data.export_stats || {};
      const parts: string[] = [];
      if (pairs) parts.push(`Индекс: ${pairs} пар · ${when}`);
      if (exportStats.total_found != null) {
        parts.push(`Экспорт: ${exportStats.total_found} найдено / ${exportStats.after_anonymize} после анонимизации`);
      }
      setTrainingStatus(parts.length ? parts.join(' · ') : 'Данные об обучении пока не загружены');
    } catch (error) {
      setTrainingStatus('Не удалось получить статус обучения');
    }
  };

  useEffect(() => {
    if (!api.tenantId || !api.key) return;
    refreshTrainingStatus().catch(() => undefined);
  }, [api.tenantId, api.key]);

  const handleTrainingUpload = async () => {
    if (!trainingFile) {
      toast.error('Выберите файл');
      return;
    }
    try {
      const formData = new FormData();
      formData.append('file', trainingFile);
      const response = await fetch(buildUrl(trainingUploadUrl, api), {
        method: 'POST',
        body: formData,
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      if (data.ok === false) {
        throw new Error(data.error || 'Ошибка загрузки');
      }
      toast.success('Файл загружен');
      setTrainingFile(null);
      refreshTrainingStatus().catch(() => undefined);
    } catch (error) {
      toast.error('Не удалось загрузить диалоги');
    }
  };

  const fetchDialogs = async () => {
    if (!api.tenantId || !api.key) return;
    setLoadingDialogs(true);
    try {
      const data = await requestJson<any>(buildUrl(dialogsListUrl, api, { _: Date.now() }));
      const listRaw: any[] = Array.isArray(data) ? data : data.dialogs || [];
      const list: DialogItem[] = listRaw.map((entry) => {
        const idStr = entry?.id_str ?? entry?.id ?? '';
        return {
          ...entry,
          id: typeof idStr === 'string' ? idStr : String(idStr),
        };
      });
      setDialogs(list);
      if (!activeDialog && list.length > 0) {
        setActiveDialog(list[0]);
      }
    } catch (error) {
      toast.error('Не удалось загрузить диалоги');
    } finally {
      setLoadingDialogs(false);
    }
  };

  const fetchMessages = async (dialog: DialogItem) => {
    if (!dialog) return;
    setLoadingMessages(true);
    try {
      const url = buildUrl(dialogsDetailUrl.replace('{lead_id}', String(dialog.id)), api, {
        limit: 50,
        _: Date.now(),
      });
      const data = await requestJson<{ messages: DialogMessage[]; silence?: SilenceInfo }>(url);
      setMessages(data.messages || []);
      setSilenceInfo(data.silence || null);
    } catch (error) {
      toast.error('Не удалось загрузить сообщения');
    } finally {
      setLoadingMessages(false);
    }
  };

  const fetchPhotos = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<{ photos: PhotoItem[] }>(buildUrl(photosListUrl, api));
      setPhotos(data.photos || []);
    } catch (error) {
      setPhotos([]);
    }
  };

  useEffect(() => {
    fetchDialogs().catch(() => undefined);
  }, [api.tenantId, api.key]);

  useEffect(() => {
    fetchPhotos().catch(() => undefined);
  }, [api.tenantId, api.key]);

  useEffect(() => {
    if (activeDialog) {
      fetchMessages(activeDialog).catch(() => undefined);
    }
  }, [activeDialog]);

  useEffect(() => {
    setSelectedPhoto('');
    setSilenceInfo(null);
  }, [activeDialog?.id]);

  useEffect(() => {
    if (!api.tenantId || !api.key) return;
    const timer = window.setInterval(() => {
      fetchDialogs().catch(() => undefined);
      if (activeDialog) {
        fetchMessages(activeDialog).catch(() => undefined);
      }
    }, 8000);
    return () => window.clearInterval(timer);
  }, [api.tenantId, api.key, activeDialog]);

  useEffect(() => {
    if (!messagesRef.current) return;
    const container = messagesRef.current;
    container.scrollTop = container.scrollHeight;
  }, [messages]);

  const handleSend = async () => {
    if (!activeDialog) return;
    const trimmed = sendText.trim();
    if (!trimmed && !selectedPhoto) return;
    const tempId = `temp-${Date.now()}`;
    const tempMessage: DialogMessage = {
      id: tempId,
      direction: 1,
      text: trimmed || 'Фото',
      ts: new Date().toISOString(),
      status: 'sending',
      isTemp: true,
    };
    setMessages((prev) => [...prev, tempMessage]);
    setSendText('');
    setSelectedPhoto('');
    try {
      await postJson(buildUrl(dialogsSendUrl.replace('{lead_id}', String(activeDialog.id)), api), {
        text: trimmed,
        photo_id: selectedPhoto || undefined,
      });
      fetchMessages(activeDialog).catch(() => undefined);
    } catch (error) {
      toast.error('Не удалось отправить сообщение');
      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === tempId ? { ...msg, status: 'failed' } : msg
        )
      );
    }
  };

  const handleFeedback = async (message: DialogMessage, rating: 'like' | 'dislike', expected?: string) => {
    try {
      const payload: Record<string, unknown> = {
        message_id: message.id,
        rating,
      };
      if (rating === 'dislike') {
        payload.expected_answer = expected;
      }
      await postJson(buildUrl(feedbackUrl, api), payload);
      toast.success('Фидбэк сохранён');
      setMessages((prev) =>
        prev.map((msg) => (msg.id === message.id ? { ...msg, feedbacked: true } : msg))
      );
      refreshFeedbackCounts().catch(() => undefined);
    } catch (error) {
      toast.error('Не удалось сохранить фидбэк');
    }
  };

  const refreshFeedbackCounts = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<{ counts: FeedbackCounts }>(buildUrl(feedbackStatsUrl, api));
      if (data.counts) setFeedbackCounts(data.counts);
    } catch {
      // ignore
    }
  };

  useEffect(() => {
    refreshFeedbackCounts().catch(() => undefined);
  }, [api.tenantId, api.key]);

  const handleExport = async () => {
    const days = (document.getElementById('export-days') as HTMLInputElement | null)?.value || '30';
    const limit = (document.getElementById('export-limit') as HTMLInputElement | null)?.value || '200';
    const per = (document.getElementById('export-per') as HTMLInputElement | null)?.value || '0';
    const url = buildUrl(exportUrl, api, { days, limit, per });
    window.open(url, '_blank');
  };

  return (
    <div className="space-y-6">
      <div className="grid gap-6 lg:grid-cols-2">
        <div className="card space-y-4">
          <div>
            <div className="card-title">Загрузка диалогов</div>
            <div className="card-subtitle">Добавьте свои примеры в формате JSONL/JSON/CSV.</div>
          </div>
          <input
            className="input"
            type="file"
            accept=".jsonl,.json,.csv"
            onChange={(e) => setTrainingFile(e.target.files?.[0] || null)}
          />
          <div className="flex flex-wrap gap-3">
            <button className="btn" onClick={handleTrainingUpload}>Загрузить</button>
            <button className="btn-secondary" onClick={refreshTrainingStatus}>Проверить статус</button>
          </div>
          {trainingStatus && <div className="text-sm text-slate-500">{trainingStatus}</div>}
        </div>

        <div className="card space-y-4">
          <div>
            <div className="card-title">Экспорт переписок</div>
            <div className="card-subtitle">Скачайте архив диалогов WhatsApp.</div>
          </div>
          <div className="grid gap-3 md:grid-cols-3">
            <input id="export-days" className="input" type="number" defaultValue={30} min={0} />
            <input id="export-limit" className="input" type="number" defaultValue={200} min={1} />
            <input id="export-per" className="input" type="number" defaultValue={0} min={0} />
          </div>
          <button className="btn" onClick={handleExport}>Скачать архив</button>
        </div>
      </div>

      <div className="card space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <div className="card-title">Диалоги</div>
            <div className="card-subtitle">Единое окно Avito и Telegram.</div>
          </div>
          <div className="text-sm text-slate-500">
            {feedbackCounts ? `Лайков: ${feedbackCounts.like} · Дизлайков: ${feedbackCounts.dislike}` : '—'}
          </div>
        </div>

        <div className="grid gap-4 lg:grid-cols-[320px,1fr]">
          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3 h-[620px] overflow-y-auto space-y-2">
            {loadingDialogs && <div className="text-sm text-slate-400">Загрузка…</div>}
            {!loadingDialogs && dialogs.length === 0 && (
              <div className="text-sm text-slate-400">Диалогов пока нет.</div>
            )}
            {dialogs.map((dialog) => (
              <button
                key={dialog.id}
                className={`w-full text-left rounded-xl border p-3 transition ${
                  activeDialog?.id === dialog.id
                    ? 'border-brand-400 bg-white'
                    : 'border-transparent bg-white hover:border-slate-200'
                }`}
                onClick={() => setActiveDialog(dialog)}
              >
                  <div className="flex items-center justify-between">
                    <div className="font-semibold text-slate-900">{dialog.title || dialog.contact || dialog.id}</div>
                    {(() => {
                      const badge = channelBadge(dialog.channel);
                      return (
                        <span className={`rounded-full px-2.5 py-0.5 text-xs font-semibold ${badge.className}`}>
                          {badge.label}
                        </span>
                      );
                    })()}
                  </div>
                <div className="text-sm text-slate-500 line-clamp-2">{dialog.last_message || '—'}</div>
                <div className="text-xs text-slate-400 mt-1">{dialog.last_ts || ''}</div>
              </button>
            ))}
          </div>

          <div className="rounded-2xl border border-slate-200 bg-white p-4 flex flex-col h-[620px]">
            {!activeDialog ? (
              <div className="text-sm text-slate-400">Выберите диалог слева.</div>
            ) : (
              <>
                <div className="flex items-center justify-between pb-3 border-b border-slate-100">
                  <div>
                    <div className="text-lg font-semibold text-slate-900">{activeDialog.title || activeDialog.id}</div>
                    {(() => {
                      const badge = channelBadge(activeDialog.channel);
                      return (
                        <span className={`inline-flex rounded-full px-2.5 py-0.5 text-xs font-semibold ${badge.className}`}>
                          {badge.label}
                        </span>
                      );
                    })()}
                    {silenceInfo && (silenceInfo.active || silenceInfo.auto_reply_enabled === false) && (
                      <div className="mt-1 text-xs text-amber-600">
                        {silenceInfo.active
                          ? `Бот молчит: ${silenceReasonLabel(silenceInfo.reason)}`
                          : 'Автоответ выключен'}
                        {silenceInfo.active && silenceInfo.ttl_seconds && (
                          <span className="ml-2 text-amber-500">
                            · осталось ~{Math.ceil(silenceInfo.ttl_seconds / 60)} мин
                          </span>
                        )}
                      </div>
                    )}
                  </div>
                  <button className="btn-secondary" onClick={() => fetchMessages(activeDialog)}>Обновить диалог</button>
                </div>
                <div ref={messagesRef} className="flex-1 overflow-y-auto py-4 space-y-4">
                  {loadingMessages && <div className="text-sm text-slate-400">Загрузка…</div>}
                  {!loadingMessages && messages.length === 0 && (
                    <div className="text-sm text-slate-400">Сообщений нет.</div>
                  )}
                  {messages.map((msg) => (
                    <MessageBubble key={msg.id} message={msg} onFeedback={handleFeedback} />
                  ))}
                </div>
                <div className="border-t border-slate-100 pt-3">
                  <div className="flex flex-col gap-3">
                    <div className="flex gap-3">
                      <textarea
                        className="textarea"
                        rows={2}
                        placeholder="Введите сообщение…"
                        value={sendText}
                        onChange={(e) => setSendText(e.target.value)}
                      />
                      <button className="btn" onClick={handleSend}>
                        Отправить
                      </button>
                    </div>
                    <div className="flex flex-wrap items-center gap-3">
                      <select
                        className="input"
                        value={selectedPhoto}
                        onChange={(e) => setSelectedPhoto(e.target.value)}
                      >
                        <option value="">Без фото</option>
                        {photos.map((photo) => (
                          <option key={photo.id} value={photo.id}>
                            {photo.original || photo.filename || photo.id}
                          </option>
                        ))}
                      </select>
                      <button className="btn-secondary" onClick={() => fetchPhotos().catch(() => undefined)}>
                        Обновить фото
                      </button>
                      {selectedPhoto && (
                        <span className="text-xs text-slate-500">Фото прикреплено</span>
                      )}
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

const MessageBubble: React.FC<{
  message: DialogMessage;
  onFeedback: (msg: DialogMessage, rating: 'like' | 'dislike', expected?: string) => void;
}> = ({ message, onFeedback }) => {
  const [showDislike, setShowDislike] = useState(false);
  const [expected, setExpected] = useState('');

  const isOut = message.direction === 1;
  const canFeedback = message.from_bot && !message.feedbacked;

  const handleDislikeSend = () => {
    if (!expected.trim()) {
      toast.error('Введите правильный ответ');
      return;
    }
    onFeedback(message, 'dislike', expected.trim());
  };

  return (
    <div className={`flex ${isOut ? 'justify-end' : 'justify-start'}`}>
      <div className={`max-w-[75%] rounded-2xl px-4 py-3 shadow-subtle ${isOut ? 'bg-slate-900 text-white' : 'bg-slate-50 text-slate-900'}`}>
        <div className="whitespace-pre-wrap text-sm">{message.text}</div>
        <div className="mt-2 flex items-center justify-between text-xs text-slate-400">
          <span>{message.ts ? new Date(message.ts).toLocaleTimeString() : ''}</span>
          <span>{message.status || ''}</span>
        </div>
        {canFeedback && (
          <div className="mt-3 space-y-2">
            <div className="flex gap-2">
              <button
                className="inline-flex items-center gap-1 rounded-lg bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-700"
                onClick={() => onFeedback(message, 'like')}
              >
                👍 Нормально
              </button>
              <button
                className="inline-flex items-center gap-1 rounded-lg bg-rose-50 px-3 py-1 text-xs font-semibold text-rose-700"
                onClick={() => setShowDislike((prev) => !prev)}
              >
                👎 Плохо
              </button>
            </div>
            {showDislike && (
              <div className="space-y-2">
                <textarea
                  className="textarea"
                  rows={2}
                  placeholder="Как нужно было ответить"
                  value={expected}
                  onChange={(e) => setExpected(e.target.value)}
                />
                <button className="btn" onClick={handleDislikeSend}>Отправить</button>
              </div>
            )}
          </div>
        )}
        {message.feedbacked && (
          <div className="mt-2 text-xs text-emerald-500">Фидбэк сохранён</div>
        )}
      </div>
    </div>
  );
};

export default TrainingTab;
