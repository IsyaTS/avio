import React, { useEffect, useMemo, useRef, useState } from 'react';
import toast from 'react-hot-toast';
import { useClient } from '../context/ClientContext';
import { buildUrl, postJson, requestJson } from '../lib/api';
import Hint from '../components/Hint';

type DialogItem = {
  id: string;
  channel: string;
  title: string;
  contact?: string | null;
  last_message?: string | null;
  last_ts?: string | null;
  avito_account_id?: number | string | null;
  avito_account_display_name?: string | null;
  avito_account_login?: string | null;
  avito_item_city?: string | null;
  avito_item_city_status?: string | null;
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
  attachments?: Array<{
    type?: string;
    url?: string;
    filename?: string;
    photo_id?: string;
  }>;
  source?: string;
  tg_slot?: number | null;
};
type TelegramAccount = { slot: number; label: string; username?: string | null; phone?: string | null };

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

const avitoAccountLabel = (dialog: DialogItem) => {
  const displayName = String(dialog.avito_account_display_name || '').trim();
  if (displayName) return displayName;
  const login = String(dialog.avito_account_login || '').trim();
  if (login) return login;
  const accountId = dialog.avito_account_id ? String(dialog.avito_account_id) : '';
  return accountId ? `ID ${accountId}` : '';
};

const avitoCityLabel = (dialog: DialogItem) => {
  const city = String(dialog.avito_item_city || '').trim();
  if (city) return city;
  return dialog.avito_item_city_status === 'error' ? 'город не определён' : '';
};

const avitoConnectedAccountLabel = (account: Record<string, any>) => {
  const displayName = String(account.display_name || '').trim();
  if (displayName) return displayName;
  const login = String(account.account_login || '').trim();
  if (login) return login;
  return `ID ${String(account.account_id || '').slice(-8)}`;
};

const AvitoDialogMeta = ({ dialog, compact = false }: { dialog: DialogItem; compact?: boolean }) => {
  if (dialog.channel !== 'avito') return null;
  const account = avitoAccountLabel(dialog);
  const city = avitoCityLabel(dialog);
  if (!account && !city) return null;
  const chipClass = compact
    ? 'rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-600'
    : 'rounded-full bg-slate-100 px-2.5 py-1 text-xs font-medium text-slate-600';
  return (
    <div className={`flex flex-wrap items-center gap-1.5 ${compact ? 'mt-2' : 'mt-2'}`}>
      {account && <span className={chipClass}>Аккаунт: {account}</span>}
      {city && <span className={chipClass}>Город объявления: {city}</span>}
    </div>
  );
};

type AvitoHistoryProbeJob = {
  job_id?: string;
  status?: string;
  chats_seen?: number;
  messages_seen?: number;
  error_code?: string | null;
};

type AvitoHistoryExportJob = {
  job_id?: string;
  status?: string;
  target_dialogs?: number;
  candidates_seen?: number;
  dialogs_accepted?: number;
  dialogs_rejected?: number;
  reject_reasons?: Record<string, number>;
  file_available?: boolean;
  file_name?: string | null;
  file_size?: number;
  dialog_dataset_file_available?: boolean;
  dialog_dataset_file_name?: string | null;
  dialog_dataset_file_size?: number;
  dialog_dataset_count?: number;
  dialog_dataset_active?: boolean;
  dialog_dataset_active_count?: number;
  dialog_dataset_index_sha1?: string | null;
  export_summary_file_available?: boolean;
  export_summary_file_name?: string | null;
  export_summary_file_size?: number;
  export_pipeline_version?: string | null;
  ai_schema_calls_count?: number;
  legacy_contextual_enabled?: boolean;
  checkpoint_available?: boolean;
  checkpoint_stage?: string | null;
  contextual_file_available?: boolean;
  contextual_file_name?: string | null;
  contextual_file_size?: number;
  contextual_cases_count?: number;
  review_cases_file_available?: boolean;
  review_cases_file_name?: string | null;
  review_cases_file_size?: number;
  review_cases_count?: number;
  rejected_cases_summary_available?: boolean;
  rejected_cases_summary_name?: string | null;
  rejected_cases_summary_size?: number;
  domain_schema_file_available?: boolean;
  domain_schema_file_name?: string | null;
  domain_schema_file_size?: number;
  business_rules_draft_file_available?: boolean;
  business_rules_draft_file_name?: string | null;
  business_rules_draft_file_size?: number;
  domain_key?: string | null;
  domain_label?: string | null;
  domain_slots_count?: number;
  domain_schema_summary?: Record<string, unknown>;
  contextual_quality_summary?: Record<string, unknown>;
  contextual_mode?: string | null;
  ai_extracted_count?: number;
  rule_fallback_count?: number;
  context_bound_count?: number;
  direct_example_count?: number;
  clarify_first_count?: number;
  style_only_count?: number;
  review_count?: number;
  reject_count?: number;
  rejected_examples_count?: number;
  hard_rejected_count?: number;
  ai_rejected_count?: number;
  ai_reviewed_count?: number;
  ai_failed_count?: number;
  quality_summary?: Record<string, unknown>;
  quality_mode?: string | null;
  selected_account_id?: number | null;
  selected_account_login?: string | null;
  account_count?: number;
  accounts_processed?: number;
  created_at?: string | null;
  finished_at?: string | null;
  updated_at?: string | null;
  error_code?: string | null;
};

type TestMessage = {
  role: 'user' | 'assistant';
  text: string;
};

type TestReplyPart = {
  text: string;
  at_ms?: number;
};

const sleep = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, Math.max(0, ms)));

const TrainingTab: React.FC = () => {
  const { api, bootstrap } = useClient();
  const [lightboxUrl, setLightboxUrl] = useState<string | null>(null);
  const [avitoHistoryLoading, setAvitoHistoryLoading] = useState(false);
  const [avitoHistoryJob, setAvitoHistoryJob] = useState<AvitoHistoryProbeJob | null>(null);
  const [avitoHistoryStatus, setAvitoHistoryStatus] = useState('');
  const [avitoExportTarget, setAvitoExportTarget] = useState('100');
  const [avitoExportLoading, setAvitoExportLoading] = useState(false);
  const [avitoExportJob, setAvitoExportJob] = useState<AvitoHistoryExportJob | null>(null);
  const [avitoExportFiles, setAvitoExportFiles] = useState<AvitoHistoryExportJob[]>([]);
  const [avitoExportDeletingId, setAvitoExportDeletingId] = useState<string | null>(null);
  const [avitoExportActivatingId, setAvitoExportActivatingId] = useState<string | null>(null);
  const [avitoExportDeactivatingId, setAvitoExportDeactivatingId] = useState<string | null>(null);
  const [avitoExportDeleteCandidate, setAvitoExportDeleteCandidate] = useState<AvitoHistoryExportJob | null>(null);
  const [avitoExportStopping, setAvitoExportStopping] = useState(false);
  const [avitoExportStatus, setAvitoExportStatus] = useState('');
  const [avitoAccounts, setAvitoAccounts] = useState<Array<Record<string, any>>>([]);
  const [avitoExportAccount, setAvitoExportAccount] = useState('all');
  const [dialogs, setDialogs] = useState<DialogItem[]>([]);
  const [activeDialog, setActiveDialog] = useState<DialogItem | null>(null);
  const [messages, setMessages] = useState<DialogMessage[]>([]);
  const [allMessages, setAllMessages] = useState<DialogMessage[]>([]);
  const [telegramAccounts, setTelegramAccounts] = useState<TelegramAccount[]>([]);
  const [selectedTelegramSlot, setSelectedTelegramSlot] = useState<number | null>(null);
  const [silenceInfo, setSilenceInfo] = useState<SilenceInfo | null>(null);
  const [loadingDialogs, setLoadingDialogs] = useState(false);
  const [loadingMessages, setLoadingMessages] = useState(false);
  const [sendText, setSendText] = useState('');
  const [photos, setPhotos] = useState<PhotoItem[]>([]);
  const [selectedPhoto, setSelectedPhoto] = useState('');
  const [feedbackCounts, setFeedbackCounts] = useState<FeedbackCounts | null>(null);
  const [testMessages, setTestMessages] = useState<TestMessage[]>([]);
  const [testInput, setTestInput] = useState('');
  const [testChannel, setTestChannel] = useState('telegram');
  const [testDelayEnabled, setTestDelayEnabled] = useState(true);
  const [testLoading, setTestLoading] = useState(false);
  const [tgPairs, setTgPairs] = useState<Array<{ q_text: string; a_text: string }>>([]);
  const [tgLoading, setTgLoading] = useState(false);

  const messagesRef = useRef<HTMLDivElement | null>(null);
  const stickToBottomRef = useRef(true);
  const avitoExportSamplesRef = useRef<Array<{ at: number; candidates: number; accepted: number }>>([]);

  const dialogsListUrl = useMemo(() => bootstrap.urls?.dialogs_list || '/api/dialogs', [bootstrap.urls]);
  const dialogsDetailUrl = useMemo(() => bootstrap.urls?.dialogs_detail || '/api/dialogs/{lead_id}', [bootstrap.urls]);
  const dialogsSendUrl = useMemo(() => bootstrap.urls?.dialogs_send || '/api/dialogs/{lead_id}/send', [bootstrap.urls]);
  const dialogsUnsilenceUrl = useMemo(
    () => bootstrap.urls?.dialogs_unsilence || '/api/dialogs/{lead_id}/unsilence',
    [bootstrap.urls]
  );
  const feedbackUrl = useMemo(() => bootstrap.urls?.feedback || '/api/feedback', [bootstrap.urls]);
  const feedbackStatsUrl = useMemo(() => bootstrap.urls?.feedback_stats || '/api/feedback/stats', [bootstrap.urls]);
  const photosListUrl = useMemo(() => bootstrap.urls?.photos_list || '/pub/files/photos/list', [bootstrap.urls]);
  const dialogsTestUrl = useMemo(() => bootstrap.urls?.dialogs_test || '/api/dialogs/test', [bootstrap.urls]);
  const tgHarvestUrl = useMemo(
    () => bootstrap.urls?.training_tg_harvest || `/client/${api.tenantId}/training/telegram/harvest`,
    [bootstrap.urls, api.tenantId]
  );
  const tgAcceptUrl = useMemo(
    () => bootstrap.urls?.training_tg_accept || `/client/${api.tenantId}/training/telegram/accept`,
    [bootstrap.urls, api.tenantId]
  );

  const avitoHistoryErrorLabel = (code?: string | null) => {
    const labels: Record<string, string> = {
      not_connected: 'Avito не подключен',
      unauthorized: 'Avito требует повторного подключения',
      no_permission: 'Нет доступа к Messenger API',
      rate_limited: 'Avito ограничил запросы',
      unexpected_error: 'Не удалось проверить историю',
    };
    return code ? labels[code] || 'Не удалось проверить историю' : 'Не удалось проверить историю';
  };

  const avitoExportErrorLabel = (code?: string | null) => {
    const labels: Record<string, string> = {
      not_connected: 'Avito не подключен',
      unauthorized: 'Avito требует повторного подключения',
      no_permission: 'Нет доступа к Messenger API',
      rate_limited: 'Avito ограничил запросы',
      empty: 'Не найдено пригодных диалогов',
      cancelled: 'Подготовка остановлена',
      unexpected_error: 'Не удалось подготовить файл',
    };
    return code ? labels[code] || 'Не удалось подготовить файл' : 'Не удалось подготовить файл';
  };

  const avitoHistoryStatusEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_probe_status ||
      `/client/${api.tenantId}/avito/history/probe/{job_id}`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportStatusEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_status ||
      `/client/${api.tenantId}/avito/history/export/{job_id}`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportDownloadEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_download ||
      `/client/${api.tenantId}/avito/history/export/{job_id}/download`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportDialogDatasetDownloadEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_dialog_dataset_download ||
      `/client/${api.tenantId}/avito/history/export/{job_id}/dialog-dataset/download`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportExportSummaryDownloadEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_export_summary_download ||
      `/client/${api.tenantId}/avito/history/export/{job_id}/export-summary/download`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportContextualDownloadEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_contextual_download ||
      `/client/${api.tenantId}/avito/history/export/{job_id}/contextual/download`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportReviewCasesDownloadEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_review_cases_download ||
      `/client/${api.tenantId}/avito/history/export/{job_id}/review-cases/download`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportRejectedSummaryDownloadEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_rejected_summary_download ||
      `/client/${api.tenantId}/avito/history/export/{job_id}/rejected-summary/download`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportDomainSchemaDownloadEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_domain_schema_download ||
      `/client/${api.tenantId}/avito/history/export/{job_id}/domain-schema/download`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportBusinessRulesDraftDownloadEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_business_rules_draft_download ||
      `/client/${api.tenantId}/avito/history/export/{job_id}/business-rules-draft/download`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportActiveEndpoint = () =>
    bootstrap.urls?.avito_history_export_active ||
    `/client/${api.tenantId}/avito/history/export/active`;

  const avitoExportFilesEndpoint = () =>
    bootstrap.urls?.avito_history_export_files ||
    `/client/${api.tenantId}/avito/history/export/files`;

  const avitoAccountsEndpoint = () =>
    bootstrap.urls?.avito_oauth_accounts || `/v1/oauth/avito/accounts`;

  const avitoExportDeleteEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_delete ||
      `/client/${api.tenantId}/avito/history/export/{job_id}`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportCancelEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_cancel ||
      `/client/${api.tenantId}/avito/history/export/{job_id}/cancel`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportActivateDatasetEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_activate_dataset ||
      `/client/${api.tenantId}/avito/history/export/{job_id}/activate-dataset`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportDeactivateDatasetEndpoint = (jobId: string) => {
    const template =
      bootstrap.urls?.avito_history_export_deactivate_dataset ||
      `/client/${api.tenantId}/avito/history/export/{job_id}/deactivate-dataset`;
    return template.replace('{job_id}', encodeURIComponent(jobId));
  };

  const avitoExportProgressText = (job?: AvitoHistoryExportJob | null) => {
    if (!job) return 'Подготавливаем файл...';
    if (job.status === 'queued') return 'Файл в очереди на подготовку...';
    const mode = String(job.contextual_mode || '');
    const target = job.target_dialogs || Number.parseInt(avitoExportTarget || '0', 10) || 0;
    const accepted = job.dialogs_accepted || 0;
    const candidates = job.candidates_seen || 0;
    const percent = target > 0 ? Math.min(100, Math.round((accepted / target) * 100)) : 0;
    if (accepted >= target && target > 0 && ['writing_markdown', 'discovering_domain', 'writing_dialog_dataset', 'writing_artifacts', 'schema_only', 'domain_ready', 'building_cases', 'rule_extracted', 'ai_running', 'classifying_cases', 'writing_files'].includes(mode)) {
      const labels: Record<string, string> = {
        writing_markdown: 'Диалоги скачаны. Записываем Markdown',
        discovering_domain: 'Диалоги скачаны. Определяем нишу',
        writing_dialog_dataset: 'Диалоги скачаны. Готовим обучающий набор',
        writing_artifacts: 'Диалоги скачаны. Записываем файлы',
        schema_only: 'Диалоги скачаны. Обучающий набор подготовлен',
        domain_ready: 'Диалоги скачаны. Ниша определена',
        building_cases: 'Диалоги скачаны. Готовим legacy-кейсы',
        rule_extracted: 'Диалоги скачаны. Анализируем контекст',
        ai_running: 'Диалоги скачаны. Размечаем legacy-кейсы',
        classifying_cases: 'Диалоги скачаны. Проверяем применимость',
        writing_files: 'Диалоги скачаны. Записываем файлы',
      };
      return labels[mode] || 'Диалоги скачаны. Формируем файлы';
    }
    return target > 0
      ? `Скачано ${accepted} из ${target} (${percent}%). Просмотрено: ${candidates}`
      : `Скачано ${accepted}. Просмотрено: ${candidates}`;
  };

  const avitoExportProgressPercent = (job?: AvitoHistoryExportJob | null) => {
    if (!job) return 0;
    if (['completed', 'partial'].includes(String(job.status || ''))) return 100;
    const target = job.target_dialogs || Number.parseInt(avitoExportTarget || '1', 10) || 1;
    const accepted = job.dialogs_accepted || 0;
    if (target > 0 && accepted >= target) return 100;
    const scanPercent = Math.min(70, Math.round((accepted / Math.max(1, target)) * 70));
    if (accepted < target) return scanPercent;
    const mode = String(job.contextual_mode || '');
    if (mode === 'writing_markdown') return 72;
    if (mode === 'discovering_domain') return 72;
    if (mode === 'writing_dialog_dataset') return 84;
    if (mode === 'writing_artifacts') return 96;
    if (mode === 'schema_only') return 99;
    if (mode === 'domain_ready') return 73;
    if (mode === 'building_cases') return 74;
    if (mode === 'rule_extracted') return 80;
    if (mode === 'ai_running') {
      const extracted = job.ai_extracted_count || job.ai_reviewed_count || 0;
      const total = Math.max(1, job.candidates_seen || extracted || 1);
      return Math.min(92, 82 + Math.round((extracted / total) * 10));
    }
    if (mode === 'classifying_cases') return 94;
    if (mode === 'writing_files') return 98;
    return Math.max(scanPercent, 72);
  };

  const isAvitoExportActive = (job?: AvitoHistoryExportJob | null) =>
    Boolean(job && ['queued', 'running'].includes(String(job.status || '')));

  const recordAvitoExportSample = (job?: AvitoHistoryExportJob | null) => {
    if (!job) return;
    const candidates = job.candidates_seen || 0;
    const accepted = job.dialogs_accepted || 0;
    avitoExportSamplesRef.current = [
      ...avitoExportSamplesRef.current,
      { at: Date.now(), candidates, accepted },
    ].slice(-8);
  };

  const formatAvitoEtaDuration = (seconds: number) => {
    const safe = Math.max(1, Math.round(seconds));
    const minutes = Math.floor(safe / 60);
    const rest = safe % 60;
    if (minutes <= 0) return `${rest} сек`;
    if (minutes < 60) return rest > 0 ? `${minutes} мин ${rest} сек` : `${minutes} мин`;
    const hours = Math.floor(minutes / 60);
    const minuteRest = minutes % 60;
    return minuteRest > 0 ? `${hours} ч ${minuteRest} мин` : `${hours} ч`;
  };

  const avitoExportEtaText = (job?: AvitoHistoryExportJob | null) => {
    if (!isAvitoExportActive(job)) return '';
    const target = job?.target_dialogs || Number.parseInt(avitoExportTarget || '0', 10) || 0;
    const accepted = job?.dialogs_accepted || 0;
    const candidates = job?.candidates_seen || 0;
    const samples = avitoExportSamplesRef.current;
    if (target <= 0 || accepted >= target) return '';
    if (accepted < 3 || candidates < 10 || samples.length < 2) return 'Считаем время...';
    const first = samples[0];
    const last = samples[samples.length - 1];
    const elapsedSeconds = Math.max(0, (last.at - first.at) / 1000);
    if (elapsedSeconds < 10) return 'Считаем время...';
    const candidateSpeed = (last.candidates - first.candidates) / elapsedSeconds;
    if (candidateSpeed <= 0) return 'Сканируем историю...';
    const acceptanceRate = accepted / candidates;
    if (acceptanceRate <= 0) return 'Сканируем историю...';
    const remainingDialogs = Math.max(0, target - accepted);
    const expectedCandidatesLeft = remainingDialogs / acceptanceRate;
    const etaSeconds = expectedCandidatesLeft / candidateSpeed;
    if (!Number.isFinite(etaSeconds) || etaSeconds <= 0) return '';
    return `Примерно осталось: ${formatAvitoEtaDuration(etaSeconds)}`;
  };

  const avitoExportStatusText = (job?: AvitoHistoryExportJob | null) => {
    const eta = avitoExportEtaText(job);
    const progress = avitoExportProgressText(job);
    return eta ? `${progress} · ${eta}` : progress;
  };

  const avitoExportFileLabel = (job?: AvitoHistoryExportJob | null) => {
    if (!job) return '';
    if (job.file_name) return job.file_name;
    const accepted = job.dialogs_accepted || job.target_dialogs || Number.parseInt(avitoExportTarget || '0', 10) || 0;
    const rawDate = job.finished_at || job.created_at || new Date().toISOString();
    const date = new Date(rawDate);
    const stamp = Number.isNaN(date.getTime()) ? '' : date.toISOString().slice(0, 10);
    return stamp ? `dialogs_${accepted}_${stamp}.md` : `dialogs_${accepted}.md`;
  };

  const avitoContextualFileLabel = (job?: AvitoHistoryExportJob | null) => {
    if (!job) return '';
    if (job.contextual_file_name) return job.contextual_file_name;
    const count = job.contextual_cases_count || job.dialogs_accepted || job.target_dialogs || 0;
    const rawDate = job.finished_at || job.created_at || new Date().toISOString();
    const date = new Date(rawDate);
    const stamp = Number.isNaN(date.getTime()) ? '' : date.toISOString().slice(0, 10);
    return stamp ? `contextual_cases_${count}_${stamp}.jsonl` : `contextual_cases_${count}.jsonl`;
  };

  const avitoDialogDatasetFileLabel = (job?: AvitoHistoryExportJob | null) => {
    if (!job) return '';
    if (job.dialog_dataset_file_name) return job.dialog_dataset_file_name;
    const count = job.dialog_dataset_count || job.dialogs_accepted || job.target_dialogs || 0;
    const rawDate = job.finished_at || job.created_at || new Date().toISOString();
    const date = new Date(rawDate);
    const stamp = Number.isNaN(date.getTime()) ? '' : date.toISOString().slice(0, 10);
    return stamp ? `dialog_dataset_${count}_${stamp}.jsonl` : `dialog_dataset_${count}.jsonl`;
  };

  const avitoReviewFileLabel = (job?: AvitoHistoryExportJob | null) => {
    if (!job) return '';
    if (job.review_cases_file_name) return job.review_cases_file_name;
    const count = job.review_cases_count || 0;
    const rawDate = job.finished_at || job.created_at || new Date().toISOString();
    const date = new Date(rawDate);
    const stamp = Number.isNaN(date.getTime()) ? '' : date.toISOString().slice(0, 10);
    return stamp ? `review_cases_${count}_${stamp}.jsonl` : `review_cases_${count}.jsonl`;
  };

  const avitoSummaryFileLabel = (job?: AvitoHistoryExportJob | null) => {
    if (!job) return '';
    if (job.rejected_cases_summary_name) return job.rejected_cases_summary_name;
    const rawDate = job.finished_at || job.created_at || new Date().toISOString();
    const date = new Date(rawDate);
    const stamp = Number.isNaN(date.getTime()) ? '' : date.toISOString().slice(0, 10);
    return stamp ? `rejected_cases_summary_${stamp}.json` : 'rejected_cases_summary.json';
  };

  const avitoExportSummaryFileLabel = (job?: AvitoHistoryExportJob | null) => {
    if (!job) return '';
    if (job.export_summary_file_name) return job.export_summary_file_name;
    const rawDate = job.finished_at || job.created_at || new Date().toISOString();
    const date = new Date(rawDate);
    const stamp = Number.isNaN(date.getTime()) ? '' : date.toISOString().slice(0, 10);
    return stamp ? `export_summary_${stamp}.json` : 'export_summary.json';
  };

  const avitoDomainSchemaFileLabel = (job?: AvitoHistoryExportJob | null) => {
    if (!job) return '';
    if (job.domain_schema_file_name) return job.domain_schema_file_name;
    const rawDate = job.finished_at || job.created_at || new Date().toISOString();
    const date = new Date(rawDate);
    const stamp = Number.isNaN(date.getTime()) ? '' : date.toISOString().slice(0, 10);
    return stamp ? `domain_schema_${stamp}.json` : 'domain_schema.json';
  };

  const avitoBusinessRulesDraftFileLabel = (job?: AvitoHistoryExportJob | null) => {
    if (!job) return '';
    if (job.business_rules_draft_file_name) return job.business_rules_draft_file_name;
    const rawDate = job.finished_at || job.created_at || new Date().toISOString();
    const date = new Date(rawDate);
    const stamp = Number.isNaN(date.getTime()) ? '' : date.toISOString().slice(0, 10);
    return stamp ? `business_rules_draft_${stamp}.json` : 'business_rules_draft.json';
  };

  const waitForAvitoHistoryJob = async (initialJob: AvitoHistoryProbeJob) => {
    let job = initialJob;
    const jobId = String(job.job_id || '');
    if (!jobId || job.status !== 'running') return job;

    for (let attempt = 0; attempt < 900; attempt += 1) {
      await sleep(2000);
      const data = await requestJson<{ ok: boolean; job: AvitoHistoryProbeJob }>(
        buildUrl(avitoHistoryStatusEndpoint(jobId), api, { _: Date.now() })
      );
      job = data.job || {};
      setAvitoHistoryJob(job);
      if (job.status && job.status !== 'running') return job;
      setAvitoHistoryStatus('Проверяем доступ...');
    }

    return job;
  };

  const waitForAvitoExportJob = async (initialJob: AvitoHistoryExportJob) => {
    let job = initialJob;
    const jobId = String(job.job_id || '');
    if (!jobId || !['queued', 'running'].includes(String(job.status || ''))) return job;
    let pollErrors = 0;
    recordAvitoExportSample(job);

    for (let attempt = 0; attempt < 1800; attempt += 1) {
      await sleep(2000);
      try {
        const data = await requestJson<{ ok: boolean; job: AvitoHistoryExportJob }>(
          buildUrl(avitoExportStatusEndpoint(jobId), api, { _: Date.now() })
        );
        job = data.job || {};
        pollErrors = 0;
      } catch {
        pollErrors += 1;
        setAvitoExportStatus('Файл готовится, связь со статусом временно пропала...');
        if (pollErrors < 5) continue;
        return job;
      }
      setAvitoExportJob(job);
      recordAvitoExportSample(job);
      if (job.status && !['queued', 'running'].includes(String(job.status))) return job;
      setAvitoExportStatus(avitoExportStatusText(job));
    }

    return job;
  };

  const handleAvitoHistoryProbe = async () => {
    const endpoint =
      bootstrap.urls?.avito_history_probe || `/client/${api.tenantId}/avito/history/probe`;
    if (!api.tenantId || !api.key) return;
    setAvitoHistoryLoading(true);
    setAvitoHistoryStatus('Проверяем доступ...');
    setAvitoHistoryJob(null);
    try {
      const data = await postJson<{ ok: boolean; job: AvitoHistoryProbeJob }>(
        buildUrl(endpoint, api),
        { chat_limit: 100 }
      );
      const job = await waitForAvitoHistoryJob(data.job || {});
      setAvitoHistoryJob(job);
      if (job.status === 'running') {
        setAvitoHistoryStatus('Проверка ещё выполняется, обновите статус позже');
        toast('Проверка истории Avito ещё выполняется');
      } else if (job.status === 'failed') {
        setAvitoHistoryStatus(avitoHistoryErrorLabel(job.error_code));
        toast.error(avitoHistoryErrorLabel(job.error_code));
      } else if ((job.chats_seen || 0) <= 0 || (job.messages_seen || 0) <= 0) {
        setAvitoHistoryStatus('Доступ есть, но диалоги не найдены');
        toast('Доступ есть, но диалоги не найдены');
      } else {
        setAvitoHistoryStatus('Доступ к диалогам есть');
        toast.success('История Avito доступна');
      }
    } catch (error) {
      setAvitoHistoryStatus('Не удалось проверить историю');
      toast.error('Не удалось проверить историю Avito');
    } finally {
      setAvitoHistoryLoading(false);
    }
  };

  const handleAvitoHistoryExport = async () => {
    const endpoint =
      bootstrap.urls?.avito_history_export || `/client/${api.tenantId}/avito/history/export`;
    if (!api.tenantId || !api.key) return;
    if (avitoExportJob && ['queued', 'running'].includes(String(avitoExportJob.status || ''))) {
      toast('Файл уже подготавливается');
      return;
    }
    const target = Number.parseInt(avitoExportTarget || '100', 10);
    if (!Number.isFinite(target) || target < 1 || target > 10000) {
      toast.error('Укажите количество от 1 до 10000');
      return;
    }
    setAvitoExportLoading(true);
    setAvitoExportStatus('Подготавливаем файл...');
    setAvitoExportJob(null);
    avitoExportSamplesRef.current = [];
    try {
      const data = await postJson<{ ok: boolean; job: AvitoHistoryExportJob }>(
        buildUrl(endpoint, api),
        {
          target_dialogs: target,
          quality_review: true,
          all_accounts: avitoExportAccount === 'all',
          account_id: avitoExportAccount === 'all' ? null : avitoExportAccount,
        }
      );
      const job = await waitForAvitoExportJob(data.job || {});
      setAvitoExportJob(job);
      if (job.status === 'queued' || job.status === 'running') {
        setAvitoExportStatus('Файл ещё подготавливается, обновите статус позже');
        toast('Файл диалогов ещё подготавливается');
      } else if (job.status === 'completed') {
        setAvitoExportStatus('Файл готов');
        toast.success('Файл диалогов готов');
      } else if (job.status === 'partial') {
        setAvitoExportStatus('Файл готов частично: хороших диалогов меньше цели');
        toast('Файл готов частично');
      } else {
        setAvitoExportStatus(avitoExportErrorLabel(job.error_code));
        toast.error(avitoExportErrorLabel(job.error_code));
      }
      if (
        job.file_available ||
        job.dialog_dataset_file_available ||
        job.export_summary_file_available ||
        job.contextual_file_available ||
        job.review_cases_file_available ||
        job.rejected_cases_summary_available ||
        job.domain_schema_file_available ||
        job.business_rules_draft_file_available
      ) {
        await loadAvitoExportFiles();
      }
    } catch (error) {
      setAvitoExportStatus('Не удалось подготовить файл');
      toast.error('Не удалось подготовить файл диалогов');
    } finally {
      setAvitoExportLoading(false);
    }
  };

  const handleStopAvitoHistoryExport = async () => {
    const jobId = String(avitoExportJob?.job_id || '');
    if (!jobId || !api.tenantId || !api.key) return;
    setAvitoExportStopping(true);
    try {
      const data = await postJson<{ ok: boolean; job?: AvitoHistoryExportJob }>(
        buildUrl(avitoExportCancelEndpoint(jobId), api),
        {}
      );
      const job = data.job || { ...avitoExportJob, status: 'cancelled', error_code: 'cancelled' };
      setAvitoExportJob(job);
      setAvitoExportStatus('Подготовка остановлена');
      setAvitoExportLoading(false);
      toast('Подготовка файла остановлена');
    } catch {
      toast.error('Не удалось остановить подготовку');
    } finally {
      setAvitoExportStopping(false);
    }
  };

  const loadActiveAvitoExport = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<{ ok: boolean; job?: AvitoHistoryExportJob | null }>(
        buildUrl(avitoExportActiveEndpoint(), api, { _: Date.now() })
      );
      const job = data.job || null;
      if (!job?.job_id) return;
      setAvitoExportJob(job);
      avitoExportSamplesRef.current = [];
      recordAvitoExportSample(job);
      setAvitoExportLoading(true);
      setAvitoExportStatus(avitoExportStatusText(job));
      const finalJob = await waitForAvitoExportJob(job);
      setAvitoExportJob(finalJob);
      if (
        finalJob.file_available ||
        finalJob.dialog_dataset_file_available ||
        finalJob.export_summary_file_available ||
        finalJob.contextual_file_available ||
        finalJob.review_cases_file_available ||
        finalJob.rejected_cases_summary_available ||
        finalJob.domain_schema_file_available ||
        finalJob.business_rules_draft_file_available
      ) {
        setAvitoExportStatus('Файл готов');
        await loadAvitoExportFiles();
      } else if (finalJob.status === 'cancelled') {
        setAvitoExportStatus('Подготовка остановлена');
      } else if (finalJob.status && !['queued', 'running'].includes(String(finalJob.status))) {
        setAvitoExportStatus(avitoExportErrorLabel(finalJob.error_code));
      }
    } catch {
      return;
    } finally {
      setAvitoExportLoading(false);
    }
  };

  const loadAvitoExportFiles = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<{ ok: boolean; jobs?: AvitoHistoryExportJob[] }>(
        buildUrl(avitoExportFilesEndpoint(), api, { _: Date.now() })
      );
      setAvitoExportFiles(
        (data.jobs || []).filter(
          (job) =>
            (job.file_available ||
              job.dialog_dataset_file_available ||
              job.export_summary_file_available ||
              job.contextual_file_available ||
              job.review_cases_file_available ||
              job.rejected_cases_summary_available ||
              job.domain_schema_file_available ||
              job.business_rules_draft_file_available) &&
            job.job_id
        )
      );
    } catch {
      return;
    }
  };

  const loadAvitoAccounts = async () => {
    if (!api.tenantId || !api.key) return;
    try {
      const data = await requestJson<{ accounts?: Array<Record<string, any>> }>(
        buildUrl(avitoAccountsEndpoint(), api, { _: Date.now() })
      );
      setAvitoAccounts((data.accounts || []).filter((item) => item.status === 'active'));
    } catch {
      setAvitoAccounts([]);
    }
  };

  const handleDeleteAvitoExportFile = async (job: AvitoHistoryExportJob) => {
    setAvitoExportDeleteCandidate(job);
  };

  const closeAvitoExportDeleteModal = () => {
    if (avitoExportDeletingId) return;
    setAvitoExportDeleteCandidate(null);
  };

  const confirmDeleteAvitoExportFile = async () => {
    const job = avitoExportDeleteCandidate;
    if (!job) return;
    const jobId = String(job.job_id || '');
    if (!jobId || !api.tenantId || !api.key) return;
    setAvitoExportDeletingId(jobId);
    try {
      await requestJson<{ ok: boolean }>(
        buildUrl(avitoExportDeleteEndpoint(jobId), api),
        { method: 'DELETE' }
      );
      setAvitoExportFiles((prev) => prev.filter((item) => String(item.job_id || '') !== jobId));
      setAvitoExportDeleteCandidate(null);
      toast.success('Файл удалён');
    } catch {
      toast.error('Не удалось удалить файл');
    } finally {
      setAvitoExportDeletingId(null);
    }
  };

  const handleActivateAvitoDialogDataset = async (job: AvitoHistoryExportJob) => {
    const jobId = String(job.job_id || '');
    if (!jobId || !api.tenantId || !api.key) return;
    setAvitoExportActivatingId(jobId);
    try {
      const data = await postJson<{
        ok: boolean;
        dialog_dataset?: {
          dialogs_count?: number;
          index_sha1?: string;
        };
      }>(buildUrl(avitoExportActivateDatasetEndpoint(jobId), api), {});
      const count = data.dialog_dataset?.dialogs_count || job.dialog_dataset_count || 0;
      setAvitoExportFiles((prev) =>
        prev.map((item) =>
          String(item.job_id || '') === jobId
            ? {
                ...item,
                dialog_dataset_active: true,
                dialog_dataset_active_count: count,
                dialog_dataset_index_sha1: data.dialog_dataset?.index_sha1 || item.dialog_dataset_index_sha1,
              }
            : item
        )
      );
      toast.success(count > 0 ? `Обучающий набор подключён: ${count}` : 'Обучающий набор подключён');
    } catch {
      toast.error('Не удалось подключить обучающий набор');
    } finally {
      setAvitoExportActivatingId(null);
    }
  };

  const handleDeactivateAvitoDialogDataset = async (job: AvitoHistoryExportJob) => {
    const jobId = String(job.job_id || '');
    if (!jobId || !api.tenantId || !api.key) return;
    setAvitoExportDeactivatingId(jobId);
    try {
      await postJson<{ ok: boolean }>(buildUrl(avitoExportDeactivateDatasetEndpoint(jobId), api), {});
      setAvitoExportFiles((prev) =>
        prev.map((item) =>
          String(item.job_id || '') === jobId
            ? {
                ...item,
                dialog_dataset_active: false,
                dialog_dataset_active_count: 0,
              }
            : item
        )
      );
      toast.success('Обучающий набор отключён');
    } catch {
      toast.error('Не удалось отключить обучающий набор');
    } finally {
      setAvitoExportDeactivatingId(null);
    }
  };

  const channelBadge = (channel?: string) => {
    const value = (channel || '').toLowerCase();
    if (value === 'avito') {
      return { label: 'Avito', className: 'bg-orange-100 text-orange-700' };
    }
    if (value === 'telegram') {
      return { label: 'telegram', className: 'bg-sky-100 text-sky-700' };
    }
    if (value === 'max') {
      return { label: 'max', className: 'bg-indigo-100 text-indigo-700' };
    }
    if (value === 'whatsapp') {
      return { label: 'whatsapp', className: 'bg-emerald-100 text-emerald-700' };
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
        return 'Отправлено отложенное сообщение';
      case 'silence_active':
      default:
        return 'Тишина включена';
    }
  };

  const handleHarvestTelegram = async () => {
    if (!api.tenantId || !api.key) return;
    setTgLoading(true);
    try {
      const data = await postJson(buildUrl(tgHarvestUrl, api), { limit_dialogs: 50, limit_messages: 800 });
      const list = Array.isArray(data?.items) ? data.items : [];
      setTgPairs(list);
      if (list.length === 0) {
        toast.info('Пары не найдены');
      }
    } catch (error) {
      toast.error('Не удалось скачать диалоги Telegram');
    } finally {
      setTgLoading(false);
    }
  };

  const handleAcceptPair = async (pair: { q_text: string; a_text: string }, idx: number) => {
    if (!api.tenantId || !api.key) return;
    try {
      await postJson(buildUrl(tgAcceptUrl, api), { items: [pair] });
      setTgPairs((prev) => prev.filter((_, i) => i !== idx));
      toast.success('Сохранено');
    } catch (error) {
      toast.error('Не удалось сохранить пару');
    }
  };

  const handleRejectPair = (idx: number) => {
    setTgPairs((prev) => prev.filter((_, i) => i !== idx));
  };

  useEffect(() => {
    if (!api.tenantId || !api.key) return;
    loadAvitoAccounts().catch(() => undefined);
    loadAvitoExportFiles().catch(() => undefined);
    loadActiveAvitoExport().catch(() => undefined);
  }, [api.tenantId, api.key]);

  useEffect(() => {
    if (!avitoExportDeleteCandidate) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        closeAvitoExportDeleteModal();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [avitoExportDeleteCandidate, avitoExportDeletingId]);

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
      const data = await requestJson<{
        messages: DialogMessage[];
        silence?: SilenceInfo;
        telegram_accounts?: TelegramAccount[];
        selected_tg_slot?: number | null;
        avito_account_id?: number | string | null;
        avito_account_display_name?: string | null;
        avito_account_login?: string | null;
        avito_item_city?: string | null;
        avito_item_city_status?: string | null;
      }>(url);
      const incomingMessages = data.messages || [];
      setAllMessages(incomingMessages);
      setMessages(incomingMessages);
      const accounts = Array.isArray(data.telegram_accounts) ? data.telegram_accounts : [];
      setTelegramAccounts(accounts);
      const preferredSlot = typeof data.selected_tg_slot === 'number' ? data.selected_tg_slot : null;
      setSelectedTelegramSlot((prev) => {
        const available = new Set(accounts.map((account) => account.slot));
        if (prev && available.has(prev)) {
          return prev;
        }
        if (preferredSlot && available.has(preferredSlot)) {
          return preferredSlot;
        }
        return accounts.length > 0 ? accounts[0].slot : null;
      });
      setSilenceInfo(data.silence || null);
      if (dialog.channel === 'avito') {
        setActiveDialog((current) =>
          current && current.id === dialog.id
            ? {
                ...current,
                avito_account_id: data.avito_account_id ?? current.avito_account_id,
                avito_account_display_name: data.avito_account_display_name ?? current.avito_account_display_name,
                avito_account_login: data.avito_account_login ?? current.avito_account_login,
                avito_item_city: data.avito_item_city ?? current.avito_item_city,
                avito_item_city_status: data.avito_item_city_status ?? current.avito_item_city_status,
              }
            : current,
        );
      }
    } catch (error) {
      toast.error('Не удалось загрузить сообщения');
    } finally {
      setLoadingMessages(false);
    }
  };

  useEffect(() => {
    if (!activeDialog || activeDialog.channel !== 'telegram') {
      setMessages(allMessages);
      return;
    }
    if (!selectedTelegramSlot) {
      setMessages(allMessages);
      return;
    }
    setMessages(
      allMessages.filter((msg) => {
        if (typeof msg.tg_slot !== 'number') return true;
        return msg.tg_slot === selectedTelegramSlot;
      })
    );
  }, [allMessages, selectedTelegramSlot, activeDialog]);

  const handleUnsilence = async () => {
    if (!activeDialog) return;
    try {
      const url = buildUrl(dialogsUnsilenceUrl.replace('{lead_id}', String(activeDialog.id)), api);
      await requestJson(url, { method: 'POST' });
      await fetchMessages(activeDialog);
      toast.success('Тишина снята');
    } catch (error) {
      toast.error('Не удалось снять тишину');
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
    if (!activeDialog || activeDialog.channel !== 'telegram') {
      setTelegramAccounts([]);
      setSelectedTelegramSlot(null);
    }
  }, [activeDialog?.id]);

  useEffect(() => {
    if (!api.tenantId || !api.key) return;
    const timer = window.setInterval(() => {
      fetchDialogs().catch(() => undefined);
      if (activeDialog) {
        fetchMessages(activeDialog).catch(() => undefined);
      }
    }, 300);
    return () => window.clearInterval(timer);
  }, [api.tenantId, api.key, activeDialog]);

  const handleMessagesScroll = () => {
    const container = messagesRef.current;
    if (!container) return;
    const distance = container.scrollHeight - container.scrollTop - container.clientHeight;
    stickToBottomRef.current = distance < 64;
  };

  useEffect(() => {
    stickToBottomRef.current = true;
  }, [activeDialog?.id]);

  useEffect(() => {
    const container = messagesRef.current;
    if (!container) return;
    if (!stickToBottomRef.current) return;
    container.scrollTop = container.scrollHeight;
  }, [messages]);

  useEffect(() => {
    const handler = (evt: Event) => {
      const detail = (evt as CustomEvent).detail || {};
      if (detail.url) setLightboxUrl(String(detail.url));
    };
    window.addEventListener('avio:lightbox', handler as EventListener);
    return () => window.removeEventListener('avio:lightbox', handler as EventListener);
  }, []);

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
        tg_slot: activeDialog.channel === 'telegram' ? selectedTelegramSlot || undefined : undefined,
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

  const handleTestSend = async () => {
    const text = testInput.trim();
    if (!text) {
      toast.error('Введите сообщение');
      return;
    }
    const history = testMessages.map((msg) => ({ role: msg.role, text: msg.text }));
    setTestMessages((prev) => [...prev, { role: 'user', text }]);
    setTestInput('');
    setTestLoading(true);
    try {
      const data = await postJson(buildUrl(dialogsTestUrl, api), {
        text,
        channel: testChannel,
        history,
        delay_enabled: testDelayEnabled,
        force_delay: testDelayEnabled,
        emulate_channels: true,
      });
      const rawParts: TestReplyPart[] = Array.isArray(data?.replies) ? data.replies : [];
      if (rawParts.length > 0) {
        let prevAt = 0;
        for (const item of rawParts) {
          const textPart = String(item?.text || '').trim();
          if (!textPart) continue;
          const atMs = Math.max(0, Number(item?.at_ms || 0));
          const waitMs = Math.max(0, atMs - prevAt);
          prevAt = atMs;
          if (waitMs > 0) {
            await sleep(waitMs);
          }
          setTestMessages((prev) => [...prev, { role: 'assistant', text: textPart }]);
        }
      } else {
        const replyText = String(data?.reply || '').trim();
        setTestMessages((prev) => [...prev, { role: 'assistant', text: replyText || 'Ответ не получен' }]);
      }
    } catch (error) {
      toast.error('Не удалось получить ответ');
    } finally {
      setTestLoading(false);
    }
  };

  const handleTestClear = () => {
    setTestMessages([]);
    setTestInput('');
  };

  return (
    <div className="space-y-6">
      <div className="card space-y-6">
        <div>
          <div className="card-title">История Avito</div>
          <div className="card-subtitle">
            Проверьте доступ к старым диалогам Avito и подготовьте файл с хорошими диалогами для обучения.
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <button
            className="btn-secondary"
            type="button"
            onClick={handleAvitoHistoryProbe}
            disabled={avitoHistoryLoading}
          >
            {avitoHistoryLoading ? 'Проверяем...' : 'Проверить доступ'}
          </button>
          {avitoHistoryStatus && (
            <span
              className={
                avitoHistoryJob &&
                avitoHistoryJob.status !== 'running' &&
                avitoHistoryJob.status !== 'failed' &&
                (avitoHistoryJob.chats_seen || 0) > 0 &&
                (avitoHistoryJob.messages_seen || 0) > 0
                  ? 'rounded-full bg-emerald-50 px-3 py-1 text-sm font-medium text-emerald-700'
                  : 'text-sm text-slate-500'
              }
            >
              {avitoHistoryStatus}
            </span>
          )}
        </div>
        <div className="border-t border-slate-200 pt-6">
          <div className="mb-4">
            <div className="text-sm font-semibold text-slate-800">Скачать диалоги Avito</div>
            <div className="text-sm text-slate-500">
              Система будет искать пригодные диалоги и продолжит сканирование, пока не наберёт нужное количество или не закончится доступная история.
            </div>
          </div>
          <div className="grid gap-4 md:grid-cols-[minmax(220px,320px),1fr]">
            <label className="space-y-2">
              <span className="text-sm font-medium text-slate-600">Сколько хороших диалогов скачать</span>
              <input
                className="input"
                type="number"
                min={1}
                max={10000}
                value={avitoExportTarget}
                onChange={(e) => setAvitoExportTarget(e.target.value)}
              />
            </label>
            <label className="space-y-2">
              <span className="text-sm font-medium text-slate-600">Аккаунт Avito</span>
              <select
                className="input"
                value={avitoExportAccount}
                onChange={(e) => setAvitoExportAccount(e.target.value)}
              >
                <option value="all">Все аккаунты</option>
                {avitoAccounts.map((account) => (
                  <option key={String(account.account_id)} value={String(account.account_id)}>
                    {avitoConnectedAccountLabel(account)}
                  </option>
                ))}
              </select>
            </label>
            <div className="flex flex-wrap items-end gap-3">
              <button
                className="btn-secondary"
                type="button"
                onClick={handleAvitoHistoryExport}
                disabled={avitoExportLoading}
              >
                {avitoExportLoading ? 'Подготавливаем...' : 'Подготовить файл диалогов'}
              </button>
              {avitoExportJob && ['queued', 'running'].includes(String(avitoExportJob.status || '')) && (
                <button
                  className="inline-flex items-center justify-center gap-2 rounded-xl border border-rose-200 bg-white px-4 py-2 text-sm font-semibold text-rose-700 shadow-subtle transition hover:bg-rose-50 focus:outline-none focus:ring-2 focus:ring-rose-100 disabled:cursor-not-allowed disabled:opacity-50"
                  type="button"
                  onClick={handleStopAvitoHistoryExport}
                  disabled={avitoExportStopping}
                >
                  {avitoExportStopping ? 'Останавливаем...' : 'Остановить'}
                </button>
              )}
              {avitoExportStatus && (
                <span className="pb-2 text-sm text-slate-500">{avitoExportStatus}</span>
              )}
            </div>
          </div>
          {avitoExportJob && isAvitoExportActive(avitoExportJob) && (
            <div className="mt-4 space-y-4 rounded-2xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-600">
              <div className="space-y-2">
                <div className="flex items-center justify-between gap-3 text-xs uppercase tracking-wide text-slate-400">
                  <span>Прогресс файла</span>
                  <span>
                    {avitoExportJob.dialogs_accepted || 0} / {avitoExportJob.target_dialogs || avitoExportTarget}
                  </span>
                </div>
                <div className="h-2 rounded-full bg-slate-200">
                  <div
                    className="h-2 rounded-full bg-emerald-500 transition-all"
                    style={{ width: `${avitoExportProgressPercent(avitoExportJob)}%` }}
                  />
                </div>
                <div className="grid gap-2 text-xs text-slate-500 sm:grid-cols-3">
                  <span>Диалогов: {avitoExportJob.dialogs_accepted || 0}</span>
                  <span>Dataset: {avitoExportJob.dialog_dataset_count || 0}</span>
                  <span>{avitoExportEtaText(avitoExportJob)}</span>
                </div>
              </div>
            </div>
          )}
          {avitoExportFiles.length > 0 && (
            <div className="mt-4 divide-y divide-slate-200 rounded-xl border border-slate-200 bg-white">
              {avitoExportFiles.map((job) => {
                const jobId = String(job.job_id || '');
                return (
                  <React.Fragment key={jobId}>
                    {job.domain_label && (
                      <div className="flex items-center justify-between gap-3 bg-slate-50 px-3 py-2 text-xs font-medium text-slate-500">
                        <span>Ниша определена: {job.domain_label}</span>
                      </div>
                    )}
                    {job.file_available && (
                      <div className="flex items-center justify-between gap-3 px-3 py-2">
                        <a
                          className="min-w-0 inline-flex items-center gap-2 text-sm font-medium text-emerald-700 hover:text-emerald-800"
                          href={buildUrl(avitoExportDownloadEndpoint(jobId), api)}
                        >
                          <span aria-hidden="true" className="inline-flex h-5 w-7 shrink-0 items-center justify-center rounded-sm border border-emerald-300 bg-emerald-50 text-[9px] font-bold leading-none text-emerald-700">
                            MD
                          </span>
                          <span className="truncate">{avitoExportFileLabel(job)}</span>
                        </a>
                        <button
                          className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-xl leading-none text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
                          type="button"
                          title="Удалить"
                          aria-label="Удалить"
                          onClick={() => handleDeleteAvitoExportFile(job)}
                          disabled={avitoExportDeletingId === jobId}
                        >
                          ×
                        </button>
                      </div>
                    )}
                    {job.dialog_dataset_file_available && (
                      <div className="flex items-center justify-between gap-3 px-3 py-2">
                        <a
                          className="min-w-0 inline-flex items-center gap-2 text-sm font-medium text-sky-700 hover:text-sky-800"
                          href={buildUrl(avitoExportDialogDatasetDownloadEndpoint(jobId), api)}
                        >
                          <span aria-hidden="true" className="inline-flex h-5 w-14 shrink-0 items-center justify-center rounded-sm border border-sky-300 bg-sky-50 text-[9px] font-bold leading-none text-sky-700">
                            DATASET
                          </span>
                          <span className="truncate">{avitoDialogDatasetFileLabel(job)}</span>
                        </a>
                        <button
                          className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-xl leading-none text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
                          type="button"
                          title="Удалить"
                          aria-label="Удалить"
                          onClick={() => handleDeleteAvitoExportFile(job)}
                          disabled={avitoExportDeletingId === jobId}
                        >
                          ×
                        </button>
                      </div>
                    )}
                    {job.contextual_file_available && job.legacy_contextual_enabled && (
                      <div className="flex items-center justify-between gap-3 px-3 py-2">
                        <a
                          className="min-w-0 inline-flex items-center gap-2 text-sm font-medium text-sky-700 hover:text-sky-800"
                          href={buildUrl(avitoExportContextualDownloadEndpoint(jobId), api)}
                        >
                          <span aria-hidden="true" className="inline-flex h-5 w-10 shrink-0 items-center justify-center rounded-sm border border-sky-300 bg-sky-50 text-[9px] font-bold leading-none text-sky-700">
                            CASES
                          </span>
                          <span className="truncate">{avitoContextualFileLabel(job)}</span>
                        </a>
                        <button
                          className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-xl leading-none text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
                          type="button"
                          title="Удалить"
                          aria-label="Удалить"
                          onClick={() => handleDeleteAvitoExportFile(job)}
                          disabled={avitoExportDeletingId === jobId}
                        >
                          ×
                        </button>
                      </div>
                    )}
                    {job.review_cases_file_available && job.legacy_contextual_enabled && (
                      <div className="flex items-center justify-between gap-3 px-3 py-2">
                        <a
                          className="min-w-0 inline-flex items-center gap-2 text-sm font-medium text-amber-700 hover:text-amber-800"
                          href={buildUrl(avitoExportReviewCasesDownloadEndpoint(jobId), api)}
                        >
                          <span aria-hidden="true" className="inline-flex h-5 w-12 shrink-0 items-center justify-center rounded-sm border border-amber-300 bg-amber-50 text-[9px] font-bold leading-none text-amber-700">
                            REVIEW
                          </span>
                          <span className="truncate">{avitoReviewFileLabel(job)}</span>
                        </a>
                        <button
                          className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-xl leading-none text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
                          type="button"
                          title="Удалить"
                          aria-label="Удалить"
                          onClick={() => handleDeleteAvitoExportFile(job)}
                          disabled={avitoExportDeletingId === jobId}
                        >
                          ×
                        </button>
                      </div>
                    )}
                    {job.rejected_cases_summary_available && job.legacy_contextual_enabled && (
                      <div className="flex items-center justify-between gap-3 px-3 py-2">
                        <a
                          className="min-w-0 inline-flex items-center gap-2 text-sm font-medium text-slate-700 hover:text-slate-900"
                          href={buildUrl(avitoExportRejectedSummaryDownloadEndpoint(jobId), api)}
                        >
                          <span aria-hidden="true" className="inline-flex h-5 w-14 shrink-0 items-center justify-center rounded-sm border border-slate-300 bg-slate-50 text-[9px] font-bold leading-none text-slate-700">
                            SUMMARY
                          </span>
                          <span className="truncate">{avitoSummaryFileLabel(job)}</span>
                        </a>
                        <button
                          className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-xl leading-none text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
                          type="button"
                          title="Удалить"
                          aria-label="Удалить"
                          onClick={() => handleDeleteAvitoExportFile(job)}
                          disabled={avitoExportDeletingId === jobId}
                        >
                          ×
                        </button>
                      </div>
                    )}
                    {job.domain_schema_file_available && (
                      <div className="flex items-center justify-between gap-3 px-3 py-2">
                        <a
                          className="min-w-0 inline-flex items-center gap-2 text-sm font-medium text-violet-700 hover:text-violet-800"
                          href={buildUrl(avitoExportDomainSchemaDownloadEndpoint(jobId), api)}
                        >
                          <span aria-hidden="true" className="inline-flex h-5 w-12 shrink-0 items-center justify-center rounded-sm border border-violet-300 bg-violet-50 text-[9px] font-bold leading-none text-violet-700">
                            DOMAIN
                          </span>
                          <span className="truncate">{avitoDomainSchemaFileLabel(job)}</span>
                        </a>
                        <button
                          className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-xl leading-none text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
                          type="button"
                          title="Удалить"
                          aria-label="Удалить"
                          onClick={() => handleDeleteAvitoExportFile(job)}
                          disabled={avitoExportDeletingId === jobId}
                        >
                          ×
                        </button>
                      </div>
                    )}
                    {job.business_rules_draft_file_available && (
                      <div className="flex items-center justify-between gap-3 px-3 py-2">
                        <a
                          className="min-w-0 inline-flex items-center gap-2 text-sm font-medium text-indigo-700 hover:text-indigo-800"
                          href={buildUrl(avitoExportBusinessRulesDraftDownloadEndpoint(jobId), api)}
                        >
                          <span aria-hidden="true" className="inline-flex h-5 w-10 shrink-0 items-center justify-center rounded-sm border border-indigo-300 bg-indigo-50 text-[9px] font-bold leading-none text-indigo-700">
                            RULES
                          </span>
                          <span className="truncate">{avitoBusinessRulesDraftFileLabel(job)}</span>
                        </a>
                        <button
                          className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-xl leading-none text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
                          type="button"
                          title="Удалить"
                          aria-label="Удалить"
                          onClick={() => handleDeleteAvitoExportFile(job)}
                          disabled={avitoExportDeletingId === jobId}
                        >
                          ×
                        </button>
                      </div>
                    )}
                    {job.export_summary_file_available && (
                      <div className="flex items-center justify-between gap-3 px-3 py-2">
                        <a
                          className="min-w-0 inline-flex items-center gap-2 text-sm font-medium text-slate-700 hover:text-slate-900"
                          href={buildUrl(avitoExportExportSummaryDownloadEndpoint(jobId), api)}
                        >
                          <span aria-hidden="true" className="inline-flex h-5 w-14 shrink-0 items-center justify-center rounded-sm border border-slate-300 bg-slate-50 text-[9px] font-bold leading-none text-slate-700">
                            SUMMARY
                          </span>
                          <span className="truncate">{avitoExportSummaryFileLabel(job)}</span>
                        </a>
                        <button
                          className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-xl leading-none text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
                          type="button"
                          title="Удалить"
                          aria-label="Удалить"
                          onClick={() => handleDeleteAvitoExportFile(job)}
                          disabled={avitoExportDeletingId === jobId}
                        >
                          ×
                        </button>
                      </div>
                    )}
	                    {job.dialog_dataset_file_available && (
	                      <div className="flex flex-wrap items-center justify-between gap-3 bg-slate-50 px-3 py-3">
		                        <div className="text-sm text-slate-600">
		                          {job.dialog_dataset_active
		                            ? `Обучающий набор подключён к ответам${job.dialog_dataset_active_count ? `: ${job.dialog_dataset_active_count}` : ''}`
		                            : 'Обучающий набор подготовлен.'}
		                        </div>
		                        {job.dialog_dataset_active ? (
		                          <button
		                            className="btn-secondary"
		                            type="button"
		                            onClick={() => handleDeactivateAvitoDialogDataset(job)}
		                            disabled={avitoExportDeactivatingId === jobId}
		                          >
		                            {avitoExportDeactivatingId === jobId ? 'Отключаем...' : 'Отключить'}
		                          </button>
		                        ) : (
		                          <button
		                            className="btn-secondary"
		                            type="button"
		                            onClick={() => handleActivateAvitoDialogDataset(job)}
		                            disabled={avitoExportActivatingId === jobId}
		                          >
		                            {avitoExportActivatingId === jobId ? 'Подключаем...' : 'Подключить к ответам'}
		                          </button>
		                        )}
		                      </div>
	                    )}
                  </React.Fragment>
                );
              })}
            </div>
          )}
        </div>
      </div>

      <div className="card space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="card-title">Telegram: скачать и проанализировать диалоги</div>
            <div className="card-subtitle">
              Мы соберём пары «вопрос → ответ» из истории Telegram, вы сможете подтвердить правильные.
              <span className="ml-2 inline-flex align-middle"><Hint text="Подтверждённые пары сразу попадают в обучение и начинают влиять на ответы." /></span>
            </div>
          </div>
          <button className="btn-secondary" onClick={handleHarvestTelegram} disabled={tgLoading}>
            {tgLoading ? 'Скачиваем...' : 'Скачать и проанализировать'}
          </button>
        </div>
        {tgPairs.length === 0 && !tgLoading && (
          <div className="text-sm text-slate-400">Пока нет найденных пар.</div>
        )}
        {tgPairs.length > 0 && (
          <div className="space-y-3">
            {tgPairs.map((pair, idx) => (
              <div
                key={`${pair.q_text}-${idx}`}
                className="flex flex-col gap-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm"
              >
                <div className="text-sm font-semibold text-slate-700">Вопрос клиента</div>
                <div className="text-sm text-slate-900 whitespace-pre-wrap">{pair.q_text}</div>
                <div className="text-sm font-semibold text-slate-700">Ответ менеджера</div>
                <div className="text-sm text-slate-900 whitespace-pre-wrap">{pair.a_text}</div>
                <div className="flex gap-2">
                  <button className="btn-secondary" onClick={() => handleAcceptPair(pair, idx)}>
                    👍 Подходит
                  </button>
                  <button className="btn-ghost" onClick={() => handleRejectPair(idx)}>
                    👎 Не подходит
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="card space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <div className="card-title">Диалоги</div>
            <div className="card-subtitle flex items-center gap-2">Единое окно Avito и Telegram. <Hint text="Здесь видны входящие/исходящие, источник ответа и можно вручную отвечать от менеджера." /></div>
          </div>
          <div className="text-sm text-slate-500">
            {feedbackCounts ? `Лайков: ${feedbackCounts.like} · Дизлайков: ${feedbackCounts.dislike}` : '—'}
          </div>
        </div>

        <div className="grid gap-4 lg:grid-cols-[320px,1fr]">
          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3 h-[620px] overflow-y-auto space-y-2">
            {loadingDialogs && dialogs.length === 0 && <div className="text-sm text-slate-400">Загрузка…</div>}
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
                <AvitoDialogMeta dialog={dialog} compact />
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
                    <AvitoDialogMeta dialog={activeDialog} />
                    {activeDialog.channel === 'telegram' && telegramAccounts.length > 0 && (
                      <div className="mt-2 flex items-center gap-2">
                        <span className="text-xs text-slate-500">Аккаунт:</span>
                        <select
                          className="input max-w-xs"
                          value={selectedTelegramSlot ?? ''}
                          onChange={(e) => setSelectedTelegramSlot(Number(e.target.value) || null)}
                        >
                          {telegramAccounts.map((account) => (
                            <option key={account.slot} value={account.slot}>
                              {account.label}
                            </option>
                          ))}
                        </select>
                      </div>
                    )}
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
                  <div className="flex items-center gap-2">
                    {silenceInfo?.active && (
                      <button className="btn-ghost" onClick={handleUnsilence}>
                        Снять тишину
                      </button>
                    )}
                    <button className="btn-secondary" onClick={() => fetchMessages(activeDialog)}>
                      Обновить диалог
                    </button>
                  </div>
                </div>
                <div
                  ref={messagesRef}
                  className="flex-1 overflow-y-auto py-4 space-y-4"
                  onScroll={handleMessagesScroll}
                >
                  {loadingMessages && messages.length === 0 && (
                    <div className="text-sm text-slate-400">Загрузка…</div>
                  )}
                  {!loadingMessages && messages.length === 0 && (
                    <div className="text-sm text-slate-400">Сообщений нет.</div>
                  )}
                {messages.map((msg) => (
                  <MessageBubble
                    key={msg.id}
                    message={msg}
                    channel={activeDialog?.channel || null}
                    onFeedback={handleFeedback}
                  />
                ))}
                </div>
                {lightboxUrl && (
                  <div
                    className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/80 p-4"
                    onClick={() => setLightboxUrl(null)}
                  >
                    <img
                      src={lightboxUrl}
                      alt="full"
                      className="max-h-[92vh] max-w-[92vw] rounded-2xl shadow-2xl"
                    />
                  </div>
                )}
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

      <div className="card space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="card-title">Тестовый диалог</div>
            <div className="card-subtitle flex items-center gap-2">Ответы не отправляются в реальные каналы. <Hint text="Безопасная песочница: проверка персоны и логики без отправки клиентам." /></div>
          </div>
          <div className="flex items-center gap-2">
            <select
              className="input"
              value={testChannel}
              onChange={(e) => setTestChannel(e.target.value)}
            >
              <option value="telegram">Telegram</option>
              <option value="avito">Avito</option>
              <option value="max">MAX</option>
              <option value="whatsapp">WhatsApp</option>
            </select>
            <label className="inline-flex items-center gap-2 rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs text-slate-600">
              <input
                type="checkbox"
                checked={testDelayEnabled}
                onChange={(e) => setTestDelayEnabled(e.target.checked)}
              />
              Задержка ответа
            </label>
            <button className="btn-ghost" onClick={handleTestClear}>Очистить</button>
          </div>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 h-64 overflow-y-auto space-y-3">
          {testMessages.length === 0 && (
            <div className="text-sm text-slate-400">Введите фразу клиента, чтобы увидеть ответ.</div>
          )}
          {testMessages.map((msg, idx) => (
            <div key={idx} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
              <div
                className={`max-w-[80%] rounded-2xl px-4 py-2 text-sm shadow-subtle ${
                  msg.role === 'user' ? 'bg-slate-900 text-white' : 'bg-white text-slate-900'
                }`}
              >
                <div className="whitespace-pre-wrap">{msg.text}</div>
              </div>
            </div>
          ))}
          {testLoading && <div className="text-xs text-slate-400">Генерируем ответ…</div>}
        </div>
        <div className="flex flex-col gap-3">
          <textarea
            className="textarea"
            rows={2}
            placeholder="Введите сообщение клиента…"
            value={testInput}
            onChange={(e) => setTestInput(e.target.value)}
          />
          <button className="btn" onClick={handleTestSend}>Отправить</button>
        </div>
      </div>

      {avitoExportDeleteCandidate && (
        <div
          className="avito-delete-overlay fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 px-4 py-6 backdrop-blur"
          role="dialog"
          aria-modal="true"
          aria-labelledby="avito-delete-title"
          onClick={closeAvitoExportDeleteModal}
        >
          <div
            className="avito-delete-modal w-full max-w-md rounded-2xl border border-slate-200 bg-white p-6 shadow-2xl"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="flex items-start gap-4">
              <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl bg-rose-50 text-2xl font-semibold leading-none text-rose-600">
                ×
              </div>
              <div className="min-w-0 flex-1">
                <div id="avito-delete-title" className="text-lg font-semibold text-slate-900">
                  Удалить файлы диалогов?
                </div>
                <div className="mt-2 text-sm leading-6 text-slate-500">
                  Markdown, training JSONL, review JSONL и summary исчезнут из списка и будут удалены с сервера. Это действие нельзя отменить.
                </div>
                <div className="mt-3 truncate rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm font-medium text-slate-700">
                  {avitoExportFileLabel(avitoExportDeleteCandidate)}
                </div>
              </div>
            </div>
            <div className="mt-6 flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
              <button
                className="btn-secondary"
                type="button"
                onClick={closeAvitoExportDeleteModal}
                disabled={Boolean(avitoExportDeletingId)}
                autoFocus
              >
                Отмена
              </button>
              <button
                className="inline-flex items-center justify-center gap-2 rounded-xl bg-rose-600 px-4 py-2 text-sm font-semibold text-white shadow-subtle transition hover:bg-rose-700 focus:outline-none focus:ring-2 focus:ring-rose-200 disabled:cursor-not-allowed disabled:bg-rose-300"
                type="button"
                onClick={confirmDeleteAvitoExportFile}
                disabled={Boolean(avitoExportDeletingId)}
              >
                {avitoExportDeletingId ? 'Удаляем...' : 'Удалить'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

const MessageBubble: React.FC<{
  message: DialogMessage;
  channel?: string | null;
  onFeedback: (msg: DialogMessage, rating: 'like' | 'dislike', expected?: string) => void;
}> = ({ message, channel, onFeedback }) => {
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

  const attachments = message.attachments || [];
  const isImageUrl = (url: string) =>
    /\\.(png|jpe?g|webp|gif|bmp)$/i.test(url) || url.includes('/pub/files/photos/') || url.includes('/pub/tg/media/');
  const imageAttachments = attachments.filter(
    (att) => att.url && (att.type?.includes('photo') || att.type?.includes('image') || isImageUrl(att.url))
  );
  const fileAttachments = attachments.filter(
    (att) => att.url && !imageAttachments.includes(att)
  );

  const shouldHideText = message.text?.trim() === '[Фото]';
  const sourceLabel = (() => {
    const raw = (message.source || '').toLowerCase();
    if (raw === 'followup') return 'Отложенное';
    if (raw === 'manager') return 'Менеджер';
    if (raw === 'incoming') return 'Клиент';
    if (raw === 'bot' || raw === 'llm') return 'Бот';
    if (raw) return raw;
    if (message.direction === 0) return 'Клиент';
    if (message.direction === 1 && message.from_bot) return 'Бот';
    if (message.direction === 1) return 'Менеджер';
    return '';
  })();

  return (
    <div className={`flex ${isOut ? 'justify-end' : 'justify-start'}`}>
      <div className={`max-w-[75%] rounded-2xl px-4 py-3 shadow-subtle ${isOut ? 'bg-slate-900 text-white' : 'bg-slate-50 text-slate-900'}`}>
        {imageAttachments.length > 0 && (
          <div className="mb-2 space-y-2">
            {imageAttachments.map((att, idx) => (
              <img
                key={`${att.url}-${idx}`}
                src={att.url}
                alt={att.filename || 'photo'}
                className="max-h-40 max-w-[220px] w-auto rounded-xl object-cover"
                loading="lazy"
                style={{ cursor: 'zoom-in' }}
                onClick={() => {
                  if (att.url) {
                    const evt = new CustomEvent('avio:lightbox', { detail: { url: att.url } });
                    window.dispatchEvent(evt);
                  }
                }}
              />
            ))}
          </div>
        )}
        {!shouldHideText && <div className="whitespace-pre-wrap text-sm">{message.text}</div>}
        {fileAttachments.length > 0 && (
          <div className="mt-2 space-y-1 text-xs">
            {fileAttachments.map((att, idx) => (
              <a
                key={`${att.url}-${idx}`}
                href={att.url}
                target="_blank"
                rel="noreferrer"
                className={isOut ? 'text-slate-200 underline' : 'text-slate-600 underline'}
              >
                {att.filename || att.url}
              </a>
            ))}
          </div>
        )}
        <div className="mt-2 flex items-center justify-between text-xs text-slate-400">
          <span>{message.ts ? new Date(message.ts).toLocaleTimeString() : ''}</span>
          <span className="flex items-center gap-2">
            {sourceLabel && (
              <span className={isOut ? 'text-slate-200' : 'text-slate-500'}>{sourceLabel}</span>
            )}
            <span>{message.status || ''}</span>
          </span>
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
