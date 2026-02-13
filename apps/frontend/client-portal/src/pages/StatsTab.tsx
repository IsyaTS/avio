import React, { useEffect, useState } from 'react';
import { useClient } from '../context/ClientContext';
import { buildUrl, requestJson } from '../lib/api';
import Hint from '../components/Hint';

type FeedbackCounts = { like: number; dislike: number };
type QualityItem = {
  id: number | string;
  message_id?: number | string;
  lead_id?: number | string;
  user_text: string;
  bot_text: string;
  expected: string;
  created_at?: string | null;
};
type TenantQueueStats = {
  outbox_total: number;
  outbox_tenant: number;
  followup_scheduled_len: number;
  sampled: number;
};
type AnalyticsSummary = {
  period_days: number;
  messages: {
    incoming: number;
    outgoing: number;
    by_day: { date: string; incoming: number; outgoing: number }[];
    by_channel: Record<string, { incoming: number; outgoing: number }>;
  };
  response_time: { avg_seconds: number; median_seconds: number; samples: number };
  outgoing_mix: { bot: number; manager: number; followup: number };
  top_questions: { text: string; count: number }[];
};

const StatsTab: React.FC = () => {
  const { api, bootstrap } = useClient();
  const [counts, setCounts] = useState<FeedbackCounts | null>(null);
  const [quality, setQuality] = useState<QualityItem[]>([]);
  const [loadingQuality, setLoadingQuality] = useState(false);
  const [showFix, setShowFix] = useState<string | null>(null);
  const [fixText, setFixText] = useState('');
  const [queueStats, setQueueStats] = useState<TenantQueueStats | null>(null);
  const [summary, setSummary] = useState<AnalyticsSummary | null>(null);

  useEffect(() => {
    const load = async () => {
      if (!api.tenantId || !api.key) return;
      const url = buildUrl(bootstrap.urls?.feedback_stats || '/api/feedback/stats', api);
      const data = await requestJson<{ counts: FeedbackCounts }>(url);
      if (data.counts) setCounts(data.counts);
    };
    load().catch(() => undefined);
  }, [api.tenantId, api.key, bootstrap.urls]);

  const refreshQuality = async () => {
    if (!api.tenantId || !api.key) return;
    setLoadingQuality(true);
    try {
      const url = buildUrl(bootstrap.urls?.feedback_quality || '/api/feedback/quality', api);
      const data = await requestJson<{ items: QualityItem[] }>(url);
      setQuality(data.items || []);
    } finally {
      setLoadingQuality(false);
    }
  };

  const sendFix = async (item: QualityItem) => {
    if (!fixText.trim()) return;
    if (!item.message_id) return;
    const url = buildUrl(bootstrap.urls?.feedback || '/api/feedback', api);
    await requestJson(url, {
      method: 'POST',
      body: JSON.stringify({ message_id: item.message_id, rating: 'dislike', expected_answer: fixText.trim() }),
      headers: { 'Content-Type': 'application/json' },
    } as any);
    setShowFix(null);
    setFixText('');
    refreshQuality().catch(() => undefined);
  };

  useEffect(() => {
    const load = async () => {
      if (!api.tenantId || !api.key) return;
      const url = buildUrl(bootstrap.urls?.tenant_stats || '/api/tenant/stats', api, { _: Date.now() });
      const data = await requestJson<TenantQueueStats>(url);
      if (data) setQueueStats(data);
    };
    load().catch(() => undefined);
    if (!api.tenantId || !api.key) return;
    const timer = window.setInterval(() => {
      load().catch(() => undefined);
    }, 5000);
    return () => window.clearInterval(timer);
  }, [api.tenantId, api.key, bootstrap.urls]);

  useEffect(() => {
    const load = async () => {
      if (!api.tenantId || !api.key) return;
      const url = buildUrl(bootstrap.urls?.analytics_summary || '/api/analytics/summary', api, { _: Date.now() });
      const data = await requestJson<AnalyticsSummary>(url);
      if (data) setSummary(data);
    };
    load().catch(() => undefined);
    if (!api.tenantId || !api.key) return;
    const timer = window.setInterval(() => {
      load().catch(() => undefined);
    }, 10000);
    return () => window.clearInterval(timer);
  }, [api.tenantId, api.key, bootstrap.urls]);

  const formatSeconds = (value?: number) => {
    const seconds = typeof value === 'number' ? value : 0;
    if (seconds <= 0) return '—';
    if (seconds < 60) return `${Math.round(seconds)} сек`;
    const mins = Math.round(seconds / 60);
    if (mins < 60) return `${mins} мин`;
    const hours = Math.round(mins / 60);
    return `${hours} ч`;
  };

  const mixTotal = (summary?.outgoing_mix?.bot || 0) + (summary?.outgoing_mix?.manager || 0) + (summary?.outgoing_mix?.followup || 0);

  return (
    <div className="space-y-6">
      <div className="grid gap-6 md:grid-cols-2">
      <div className="card">
        <div className="card-title flex items-center gap-2">Фидбэк по ответам <Hint text="Общее количество лайков и дизлайков по ответам бота." /></div>
        <div className="card-subtitle">Сводка лайков и дизлайков</div>
        <div className="mt-4 text-3xl font-semibold text-slate-900">
          {counts ? `${counts.like} 👍 / ${counts.dislike} 👎` : '—'}
        </div>
      </div>

      <div className="card space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <div className="card-title flex items-center gap-2">Качество обучения <Hint text="Список проблемных ответов с возможностью быстро задать правильный вариант." /></div>
            <div className="card-subtitle">Последние неверные ответы и быстрые исправления</div>
          </div>
          <button className="btn-secondary" onClick={refreshQuality} disabled={loadingQuality}>
            {loadingQuality ? 'Обновляем…' : 'Обновить'}
          </button>
        </div>
        {quality.length === 0 && !loadingQuality && (
          <div className="text-sm text-slate-400">Пока нет ошибок. Дизлайки появятся здесь.</div>
        )}
        {quality.map((item) => (
          <div key={String(item.id)} className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm space-y-2">
            <div className="text-xs text-slate-500">Лид: {item.lead_id || '—'}</div>
            <div>
              <div className="text-xs font-semibold text-slate-500">Вопрос клиента</div>
              <div className="text-sm text-slate-900 whitespace-pre-wrap">{item.user_text || '—'}</div>
            </div>
            <div>
              <div className="text-xs font-semibold text-slate-500">Ответ бота</div>
              <div className="text-sm text-slate-900 whitespace-pre-wrap">{item.bot_text || '—'}</div>
            </div>
            <div>
              <div className="text-xs font-semibold text-slate-500">Как надо отвечать</div>
              <div className="text-sm text-slate-900 whitespace-pre-wrap">{item.expected || '—'}</div>
            </div>
            <div className="flex flex-wrap gap-2">
              <button className="btn-ghost" onClick={() => { setShowFix(String(item.id)); setFixText(item.expected || ''); }}>
                Исправить ответ
              </button>
            </div>
            {showFix === String(item.id) && (
              <div className="space-y-2">
                <textarea className="textarea" rows={2} value={fixText} onChange={(e) => setFixText(e.target.value)} />
                <div className="flex gap-2">
                  <button className="btn" onClick={() => sendFix(item)}>Сохранить</button>
                  <button className="btn-ghost" onClick={() => setShowFix(null)}>Отмена</button>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>
        <div className="card space-y-4">
          <div>
            <div className="card-title flex items-center gap-2">Сообщения за {summary?.period_days || 7} дней <Hint text="Динамика входящих и исходящих сообщений за выбранный период." /></div>
            <div className="card-subtitle">Входящие/исходящие и распределение по каналам</div>
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            <div className="rounded-2xl border border-slate-200 bg-white p-4">
              <div className="text-xs text-slate-500">Входящие</div>
              <div className="mt-2 text-2xl font-semibold text-slate-900">{summary ? summary.messages.incoming : '—'}</div>
            </div>
            <div className="rounded-2xl border border-slate-200 bg-white p-4">
              <div className="text-xs text-slate-500">Исходящие</div>
              <div className="mt-2 text-2xl font-semibold text-slate-900">{summary ? summary.messages.outgoing : '—'}</div>
            </div>
          </div>
          <div className="space-y-2 text-sm text-slate-700">
            {(summary?.messages.by_channel && Object.keys(summary.messages.by_channel).length > 0) ? (
              Object.entries(summary.messages.by_channel).map(([channel, values]) => (
                <div key={channel} className="flex items-center justify-between rounded-xl border border-slate-100 bg-slate-50 px-3 py-2">
                  <span className="font-semibold capitalize">{channel}</span>
                  <span>
                    {values.incoming} вход / {values.outgoing} исход
                  </span>
                </div>
              ))
            ) : (
              <div className="text-slate-400">Пока нет данных.</div>
            )}
          </div>
        </div>
        <div className="card space-y-4">
          <div>
            <div className="card-title flex items-center gap-2">Скорость ответа <Hint text="Среднее и медианное время между сообщением клиента и ответом бота/менеджера." /></div>
            <div className="card-subtitle">Среднее и медиана ответа на входящие сообщения</div>
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            <div className="rounded-2xl border border-slate-200 bg-white p-4">
              <div className="text-xs text-slate-500">Среднее время</div>
              <div className="mt-2 text-2xl font-semibold text-slate-900">{formatSeconds(summary?.response_time.avg_seconds)}</div>
            </div>
            <div className="rounded-2xl border border-slate-200 bg-white p-4">
              <div className="text-xs text-slate-500">Медиана</div>
              <div className="mt-2 text-2xl font-semibold text-slate-900">{formatSeconds(summary?.response_time.median_seconds)}</div>
            </div>
          </div>
          <div className="text-xs text-slate-500">Замер по {summary?.response_time.samples || 0} диалогам</div>
        </div>
        <div className="card space-y-4">
          <div>
            <div className="card-title flex items-center gap-2">Кто отвечает <Hint text="Распределение исходящих сообщений: бот, менеджер и отложенные сообщения." /></div>
            <div className="card-subtitle">Доля исходящих сообщений по типам</div>
          </div>
          <div className="space-y-3 text-sm text-slate-700">
            {(['bot', 'manager', 'followup'] as const).map((key) => {
              const value = summary?.outgoing_mix?.[key] || 0;
              const pct = mixTotal ? Math.round((value / mixTotal) * 100) : 0;
              const label = key === 'bot' ? 'Бот' : key === 'manager' ? 'Менеджер' : 'Отложенные';
              return (
                <div key={key} className="space-y-1">
                  <div className="flex items-center justify-between">
                    <span>{label}</span>
                    <span className="text-xs text-slate-500">{value} сообщений</span>
                  </div>
                  <div className="h-2 rounded-full bg-slate-100">
                    <div className="h-2 rounded-full bg-blue-500" style={{ width: `${pct}%` }} />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
        <div className="card space-y-4">
          <div>
            <div className="card-title flex items-center gap-2">Частые вопросы клиентов <Hint text="Топ повторяющихся вопросов клиентов для настройки FAQ и персоны." /></div>
            <div className="card-subtitle">Топ вопросов за период</div>
          </div>
          <div className="space-y-2 text-sm text-slate-700">
            {(summary?.top_questions && summary.top_questions.length > 0) ? (
              summary.top_questions.map((item, idx) => (
                <div key={`${item.text}-${idx}`} className="flex items-start justify-between gap-3 rounded-xl border border-slate-100 bg-white px-3 py-2">
                  <div className="text-slate-900">{item.text}</div>
                  <div className="text-xs text-slate-500 whitespace-nowrap">{item.count} раз</div>
                </div>
              ))
            ) : (
              <div className="text-slate-400">Пока нет данных.</div>
            )}
          </div>
        </div>
        <div className="card">
          <div className="card-title flex items-center gap-2">Очереди <Hint text="Текущее состояние очереди исходящих и количество запланированных отложенных сообщений." /></div>
          <div className="card-subtitle">Сводка по исходящим сообщениям.</div>
          <div className="mt-4 space-y-2 text-sm text-slate-700">
            <div>
              В очереди всего: <strong>{queueStats ? queueStats.outbox_total : '—'}</strong>
            </div>
            <div>
              Ваши сообщения: <strong>{queueStats ? queueStats.outbox_tenant : '—'}</strong>
              {queueStats ? ` из ${queueStats.sampled || 0} просмотренных` : ''}
            </div>
            <div>
              Запланировано отложенных сообщений:{' '}
              <strong>{queueStats ? queueStats.followup_scheduled_len : '—'}</strong>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default StatsTab;
