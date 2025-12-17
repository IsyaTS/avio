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

const CatalogTab: React.FC = () => {
  const { api, settings, refreshSettings } = useClient();
  const [file, setFile] = useState<File | null>(null);
  const [status, setStatus] = useState('');
  const [jobId, setJobId] = useState<string | null>(null);
  const [polling, setPolling] = useState(false);

  const [csvColumns, setCsvColumns] = useState<string[]>([]);
  const [csvRows, setCsvRows] = useState<string[][]>([]);
  const [csvMessage, setCsvMessage] = useState('');

  const uploadUrl = useMemo(() => buildUrl('/pub/catalog/upload', api), [api]);
  const statusUrl = useMemo(() => buildUrl('/pub/catalog/status', api), [api]);
  const csvGetUrl = useMemo(() => buildUrl('/pub/catalog/csv', api), [api]);
  const csvSaveUrl = useMemo(() => buildUrl('/pub/catalog/csv', api), [api]);

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
          <div className="card-title">Загрузка каталога</div>
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
