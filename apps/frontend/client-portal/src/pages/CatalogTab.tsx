import React, { useEffect, useMemo, useState } from 'react';
import toast from 'react-hot-toast';
import { useClient } from '../context/ClientContext';
import { buildUrl, requestJson } from '../lib/api';

type CsvPayload = {
  ok?: boolean;
  columns: string[];
  rows: string[][];
  csv_text?: string;
  delimiter?: string;
  path?: string;
};

type CatalogStatus = {
  state?: string;
  error?: string;
  message?: string;
  csv_path?: string;
  filename?: string;
  job_id?: string;
};

type PhotoEntry = {
  id: string;
  filename?: string;
  original?: string;
  mime?: string;
  size?: number;
  uploaded_at?: number;
  url?: string;
  title?: string;
  tags?: string[];
  usage?: string;
  channels?: string[];
  auto?: boolean;
  priority?: number;
};

const PHOTO_MAX_BYTES = 24 * 1024 * 1024;
const PHOTO_EXTS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.heic'];

const CatalogTab: React.FC = () => {
  const { api, settings, refreshSettings } = useClient();
  const [file, setFile] = useState<File | null>(null);
  const [status, setStatus] = useState('');
  const [jobId, setJobId] = useState<string | null>(null);
  const [polling, setPolling] = useState(false);

  const [csvColumns, setCsvColumns] = useState<string[]>([]);
  const [csvRows, setCsvRows] = useState<string[][]>([]);
  const [csvMessage, setCsvMessage] = useState('');
  const [photoFiles, setPhotoFiles] = useState<File[]>([]);
  const [photoStatus, setPhotoStatus] = useState('');
  const [photos, setPhotos] = useState<PhotoEntry[]>([]);
  const [photoDraftInitialized, setPhotoDraftInitialized] = useState(false);
  const [photoTagInputs, setPhotoTagInputs] = useState<Record<string, string>>({});
  const [showPhotoList, setShowPhotoList] = useState(false);

  const uploadUrl = useMemo(() => buildUrl('/pub/catalog/upload', api), [api]);
  const statusUrl = useMemo(() => buildUrl('/pub/catalog/status', api), [api]);
  const csvGetUrl = useMemo(() => buildUrl('/pub/catalog/csv', api), [api]);
  const csvSaveUrl = useMemo(() => buildUrl('/pub/catalog/csv', api), [api]);
  const photosListUrl = useMemo(() => buildUrl('/pub/files/photos/list', api), [api]);
  const photosUploadUrl = useMemo(() => buildUrl('/pub/files/photos/upload', api), [api]);
  const photosDeleteTemplate = '/pub/files/photos/{photo_id}';
  const photosMetaTemplate = '/pub/files/photos/{photo_id}/meta';

  const internalDownloadUrl = (path?: string) => {
    if (!path || !api.webhookSecret || !api.tenantId) return '';
    const url = new URL(`/internal/tenant/${api.tenantId}/catalog-file`, window.location.origin);
    url.searchParams.set('path', path);
    url.searchParams.set('token', api.webhookSecret);
    return url.toString();
  };

  const uploadedMeta = useMemo(() => {
    const cfg = settings?.cfg as Record<string, any> | undefined;
    if (!cfg) return null;
    const integrations = cfg.integrations || {};
    if (integrations.uploaded_catalog) return integrations.uploaded_catalog;
    if (Array.isArray(cfg.catalogs) && cfg.catalogs.length > 0) return cfg.catalogs[0];
    return null;
  }, [settings]);

  const refreshCsv = async () => {
    try {
      const data = await requestJson<CsvPayload>(csvGetUrl);
      setCsvColumns(data.columns || []);
      setCsvRows(data.rows || []);
      setCsvMessage('CSV загружен');
    } catch (error) {
      setCsvMessage('CSV пока недоступен');
    }
  };

  useEffect(() => {
    refreshCsv().catch(() => undefined);
  }, [csvGetUrl]);

  const fetchPhotos = async () => {
    try {
      const data = await requestJson<{ photos: PhotoEntry[] }>(photosListUrl);
      setPhotos(data.photos || []);
    } catch (error) {
      setPhotos([]);
    }
  };

  useEffect(() => {
    fetchPhotos().catch(() => undefined);
  }, [photosListUrl]);

  useEffect(() => {
    setPhotoTagInputs((prev) => {
      const next = { ...prev };
      photos.forEach((photo) => {
        if (next[photo.id] === undefined) {
          next[photo.id] = (photo.tags || []).join(', ');
        }
      });
      return next;
    });
  }, [photos]);

  useEffect(() => {
    if (photoDraftInitialized) return;
    if (!api.tenantId) {
      setPhotoDraftInitialized(true);
      return;
    }
    const raw = sessionStorage.getItem(`photo-draft:${api.tenantId}`);
    if (!raw) {
      setPhotoDraftInitialized(true);
      return;
    }
    try {
      const draft = JSON.parse(raw) as Record<string, Partial<PhotoEntry>>;
      setPhotos((prev) =>
        prev.map((photo) => (draft[photo.id] ? { ...photo, ...draft[photo.id] } : photo))
      );
    } catch {
      // ignore invalid draft
    } finally {
      setPhotoDraftInitialized(true);
    }
  }, [photoDraftInitialized, api.tenantId]);

  useEffect(() => {
    if (!photoDraftInitialized || !api.tenantId) return;
    const draft: Record<string, Partial<PhotoEntry>> = {};
    photos.forEach((photo) => {
      draft[photo.id] = {
        title: photo.title,
        tags: photo.tags,
        usage: photo.usage,
        channels: photo.channels,
        auto: photo.auto,
        priority: photo.priority,
      };
    });
    sessionStorage.setItem(`photo-draft:${api.tenantId}`, JSON.stringify(draft));
  }, [photos, photoDraftInitialized, api.tenantId]);

  const pollStatus = async (id: string) => {
    setPolling(true);
    try {
      const data = await requestJson<CatalogStatus>(buildUrl(statusUrl, api, { job: id }));
      if (data.state === 'done') {
        setStatus('Каталог обработан');
        setJobId(null);
        setPolling(false);
        refreshSettings().catch(() => undefined);
        refreshCsv().catch(() => undefined);
        return;
      }
      if (data.state === 'failed') {
        setStatus(`Ошибка: ${data.error || data.message || 'unknown'}`);
        setJobId(null);
        setPolling(false);
        return;
      }
      setStatus(`Статус: ${data.state || 'processing'}`);
    } catch (error) {
      setStatus('Не удалось получить статус');
      setPolling(false);
    }
  };

  useEffect(() => {
    if (!jobId) return;
    pollStatus(jobId).catch(() => undefined);
    const timer = window.setInterval(() => {
      pollStatus(jobId).catch(() => undefined);
    }, 2000);
    return () => window.clearInterval(timer);
  }, [jobId]);

  const handleUpload = async () => {
    if (!file) {
      toast.error('Выберите файл');
      return;
    }
    setStatus('Загрузка…');
    try {
      const formData = new FormData();
      formData.append('file', file);
      const response = await fetch(uploadUrl, { method: 'POST', body: formData });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data: CatalogStatus = await response.json();
      if (data.job_id) {
        setJobId(String(data.job_id));
        setStatus('Файл принят, обрабатываем');
      } else {
        setStatus('Каталог загружен');
        refreshSettings().catch(() => undefined);
        refreshCsv().catch(() => undefined);
      }
      setFile(null);
    } catch (error) {
      setStatus('Ошибка загрузки');
      toast.error('Не удалось загрузить каталог');
    }
  };

  const handlePhotoUpload = async () => {
    if (!photoFiles.length) {
      toast.error('Выберите фото');
      return;
    }
    setPhotoStatus('Загрузка…');
    try {
      let uploaded = 0;
      for (const file of photoFiles) {
        const lowerName = file.name.toLowerCase();
        const ext = PHOTO_EXTS.find((suffix) => lowerName.endsWith(suffix));
        if (!ext) {
          toast.error(`Формат не поддерживается: ${file.name}`);
          continue;
        }
        if (file.size > PHOTO_MAX_BYTES) {
          toast.error(`Слишком большой файл: ${file.name}`);
          continue;
        }
        const formData = new FormData();
        formData.append('file', file);
        const response = await fetch(photosUploadUrl, { method: 'POST', body: formData });
        if (!response.ok) {
          throw new Error(await response.text());
        }
        const data = await response.json();
        if (data.ok === false) {
          throw new Error(data.error || 'upload_failed');
        }
        uploaded += 1;
      }
      setPhotoStatus(uploaded ? `Загружено: ${uploaded}` : 'Ничего не загружено');
      setPhotoFiles([]);
      fetchPhotos().catch(() => undefined);
    } catch (error) {
      setPhotoStatus('Ошибка загрузки');
      toast.error('Не удалось загрузить фото');
    }
  };

  const handlePhotoDelete = async (photoId: string) => {
    try {
      const url = buildUrl(
        photosDeleteTemplate.replace('{photo_id}', encodeURIComponent(photoId)),
        api
      );
      await requestJson(url, { method: 'DELETE' });
      setPhotos((prev) => prev.filter((item) => item.id !== photoId));
    } catch (error) {
      toast.error('Не удалось удалить фото');
    }
  };

  const updatePhotoField = (photoId: string, patch: Partial<PhotoEntry>) => {
    setPhotos((prev) =>
      prev.map((photo) => (photo.id === photoId ? { ...photo, ...patch } : photo))
    );
  };

  const updatePhotoTagInput = (photoId: string, value: string) => {
    setPhotoTagInputs((prev) => ({ ...prev, [photoId]: value }));
  };

  const parseTags = (value: string) =>
    value
      .split(',')
      .map((tag) => tag.trim())
      .filter(Boolean);

  const handlePhotoMetaSave = async (photo: PhotoEntry) => {
    try {
      const tags = parseTags(photoTagInputs[photo.id] ?? (photo.tags || []).join(', '));
      updatePhotoField(photo.id, { tags });
      const url = buildUrl(
        photosMetaTemplate.replace('{photo_id}', encodeURIComponent(photo.id)),
        api
      );
      await requestJson(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: photo.title || '',
          usage: photo.usage || '',
          tags,
          channels: photo.channels || [],
          auto: Boolean(photo.auto),
          priority: photo.priority || 0,
        }),
      });
      toast.success('Фото обновлено');
      fetchPhotos().catch(() => undefined);
    } catch (error) {
      toast.error('Не удалось сохранить фото');
    }
  };

  const handleAddRow = () => {
    setCsvRows((prev) => [...prev, csvColumns.map(() => '')]);
  };

  const handleUpdateCell = (rowIdx: number, colIdx: number, value: string) => {
    setCsvRows((prev) =>
      prev.map((row, rIdx) =>
        rIdx === rowIdx ? row.map((cell, cIdx) => (cIdx === colIdx ? value : cell)) : row
      )
    );
  };

  const handleSaveCsv = async () => {
    try {
      await requestJson(csvSaveUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ columns: csvColumns, rows: csvRows }),
      });
      setCsvMessage('CSV сохранён');
      toast.success('CSV сохранён');
    } catch (error) {
      setCsvMessage('Не удалось сохранить CSV');
      toast.error('Ошибка сохранения CSV');
    }
  };

  const downloadLink = internalDownloadUrl(uploadedMeta?.csv_path || uploadedMeta?.path);

  return (
    <div className="space-y-6">
      <div className="card space-y-4">
        <div>
          <div className="card-title">Каталог</div>
          <div className="card-subtitle">Поддерживаются CSV, XLSX, PDF</div>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <input
            className="input"
            type="file"
            accept=".csv,.xlsx,.xls,.pdf"
            onChange={(e) => setFile(e.target.files?.[0] || null)}
          />
          <button className="btn" onClick={handleUpload}>Загрузить</button>
          {downloadLink && (
            <a className="btn-secondary" href={downloadLink} target="_blank" rel="noreferrer">
              Скачать CSV
            </a>
          )}
        </div>
        {status && <div className="text-sm text-slate-500">{status}</div>}
        {polling && <div className="text-xs text-slate-400">Проверяем статус обработки…</div>}
      </div>

      <div className="card space-y-4">
        <div>
          <div className="card-title">Фото</div>
          <div className="card-subtitle">Поддерживаются JPG, PNG, GIF, BMP, HEIC до 24 МБ.</div>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <input
            className="input"
            type="file"
            accept=".jpg,.jpeg,.png,.gif,.bmp,.heic"
            multiple
            onChange={(e) => setPhotoFiles(Array.from(e.target.files || []))}
          />
          <button className="btn" onClick={handlePhotoUpload}>Загрузить фото</button>
          <button className="btn-secondary" onClick={() => fetchPhotos().catch(() => undefined)}>
            Обновить
          </button>
        </div>
        {photoStatus && <div className="text-sm text-slate-500">{photoStatus}</div>}
        <button
          className="w-full rounded-2xl border border-slate-200 bg-white px-4 py-3 text-left text-sm font-semibold text-slate-700 shadow-sm transition hover:border-slate-300"
          onClick={() => setShowPhotoList((prev) => !prev)}
        >
          📁 Фото ({photos.length})
        </button>
        {showPhotoList && (
          <>
            {photos.length === 0 ? (
              <div className="text-sm text-slate-400">Фото пока не загружены.</div>
            ) : (
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                {photos.map((photo) => (
                  <div key={photo.id} className="rounded-2xl border border-slate-200 bg-white p-3 space-y-2">
                    {photo.url && (
                      <img
                        src={photo.url}
                        alt={photo.original || photo.filename || 'photo'}
                        className="h-40 w-full rounded-xl object-cover"
                        loading="lazy"
                      />
                    )}
                    <div className="text-sm font-semibold text-slate-900">
                      {photo.original || photo.filename || photo.id}
                    </div>
                    <div className="text-xs text-slate-400">
                      {photo.size ? `${Math.round(photo.size / 1024)} KB` : '—'}
                    </div>
                    <label className="space-y-1 text-xs text-slate-500">
                      <span className="font-semibold text-slate-600">Название</span>
                      <input
                        className="input"
                        value={photo.title || ''}
                        onChange={(e) => updatePhotoField(photo.id, { title: e.target.value })}
                      />
                    </label>
                    <label className="space-y-1 text-xs text-slate-500">
                      <span className="font-semibold text-slate-600">Теги (через запятую)</span>
                      <input
                        className="input"
                        value={photoTagInputs[photo.id] ?? (photo.tags || []).join(', ')}
                        onChange={(e) => updatePhotoTagInput(photo.id, e.target.value)}
                        onBlur={(e) => updatePhotoField(photo.id, { tags: parseTags(e.target.value) })}
                      />
                    </label>
                    <label className="space-y-1 text-xs text-slate-500">
                      <span className="font-semibold text-slate-600">Когда использовать</span>
                      <textarea
                        className="textarea"
                        rows={2}
                        value={photo.usage || ''}
                        onChange={(e) => updatePhotoField(photo.id, { usage: e.target.value })}
                      />
                    </label>
                    <div className="space-y-1 text-xs text-slate-500">
                      <div className="font-semibold text-slate-600">Каналы</div>
                      <div className="flex flex-wrap gap-2">
                        {['telegram', 'avito'].map((channel) => {
                          const selected = (photo.channels || []).includes(channel);
                          return (
                            <label key={channel} className="flex items-center gap-2 text-xs text-slate-600">
                              <input
                                type="checkbox"
                                checked={selected}
                                onChange={(e) => {
                                  const next = new Set(photo.channels || []);
                                  if (e.target.checked) {
                                    next.add(channel);
                                  } else {
                                    next.delete(channel);
                                  }
                                  updatePhotoField(photo.id, { channels: Array.from(next) });
                                }}
                              />
                              {channel}
                            </label>
                          );
                        })}
                      </div>
                    </div>
                    <div className="grid gap-2 sm:grid-cols-2">
                      <label className="flex items-center gap-2 text-xs text-slate-600">
                        <input
                          type="checkbox"
                          checked={Boolean(photo.auto)}
                          onChange={(e) => updatePhotoField(photo.id, { auto: e.target.checked })}
                        />
                        Авто‑отправка
                      </label>
                      <label className="space-y-1 text-xs text-slate-500">
                        <span className="font-semibold text-slate-600">Приоритет</span>
                        <input
                          className="input"
                          type="number"
                          value={photo.priority ?? 0}
                          onChange={(e) => updatePhotoField(photo.id, { priority: Number(e.target.value) })}
                        />
                      </label>
                    </div>
                    <div className="flex gap-2">
                      {photo.url && (
                        <a className="btn-secondary" href={photo.url} target="_blank" rel="noreferrer">
                          Открыть
                        </a>
                      )}
                      <button className="btn-secondary" onClick={() => handlePhotoMetaSave(photo)}>
                        Сохранить
                      </button>
                      <button className="btn-ghost text-rose-600" onClick={() => handlePhotoDelete(photo.id)}>
                        Удалить
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </>
        )}
      </div>

      <div className="card space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <div className="card-title">CSV редактор</div>
            <div className="card-subtitle">Редактируйте каталог прямо в браузере.</div>
          </div>
          <div className="flex gap-2">
            <button className="btn-secondary" onClick={refreshCsv}>Обновить</button>
            <button className="btn-secondary" onClick={handleAddRow}>Добавить строку</button>
            <button className="btn" onClick={handleSaveCsv}>Сохранить</button>
          </div>
        </div>
        {csvColumns.length === 0 ? (
          <div className="text-sm text-slate-400">CSV пока не загружен.</div>
        ) : (
          <div className="overflow-auto rounded-2xl border border-slate-200">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-400">
                <tr>
                  {csvColumns.map((col, idx) => (
                    <th key={idx} className="px-3 py-2">
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {csvRows.map((row, rowIdx) => (
                  <tr key={rowIdx} className="border-t border-slate-100">
                    {row.map((cell, colIdx) => (
                      <td key={colIdx} className="px-2 py-2">
                        <input
                          className="input"
                          value={cell}
                          onChange={(e) => handleUpdateCell(rowIdx, colIdx, e.target.value)}
                        />
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {csvMessage && <div className="text-sm text-slate-500">{csvMessage}</div>}
      </div>
    </div>
  );
};

export default CatalogTab;
