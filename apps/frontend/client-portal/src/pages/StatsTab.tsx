import React, { useEffect, useState } from 'react';
import { useClient } from '../context/ClientContext';
import { buildUrl, requestJson } from '../lib/api';

type FeedbackCounts = { like: number; dislike: number };

const StatsTab: React.FC = () => {
  const { api, bootstrap } = useClient();
  const [counts, setCounts] = useState<FeedbackCounts | null>(null);

  useEffect(() => {
    const load = async () => {
      if (!api.tenantId || !api.key) return;
      const url = buildUrl(bootstrap.urls?.feedback_stats || '/api/feedback/stats', api);
      const data = await requestJson<{ counts: FeedbackCounts }>(url);
      if (data.counts) setCounts(data.counts);
    };
    load().catch(() => undefined);
  }, [api.tenantId, api.key, bootstrap.urls]);

  return (
    <div className="space-y-6">
      <div className="grid gap-6 md:grid-cols-2">
        <div className="card">
          <div className="card-title">Фидбэк по ответам</div>
          <div className="card-subtitle">Сводка лайков и дизлайков</div>
          <div className="mt-4 text-3xl font-semibold text-slate-900">
            {counts ? `${counts.like} 👍 / ${counts.dislike} 👎` : '—'}
          </div>
        </div>
        <div className="card">
          <div className="card-title">Активность</div>
          <div className="card-subtitle">Графики появятся позже</div>
          <div className="mt-6 h-40 rounded-2xl border border-dashed border-slate-200 bg-slate-50" />
        </div>
      </div>
    </div>
  );
};

export default StatsTab;
