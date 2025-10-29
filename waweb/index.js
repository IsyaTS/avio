const express = require('express');
const bodyParser = require('body-parser');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const QRCode = require('qrcode');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

process.on('uncaughtException', (err) => {
  const message = err && err.message ? err.message : String(err);
  if (message.includes('Protocol error') && message.includes('Target closed')) {
    console.warn('[waweb]', `uncaught_protocol_error message=${message}`);
    return;
  }
  if (message && (message.includes('SingletonLock') || message.includes('ProcessSingleton'))) {
    const match = message.match(/session-tenant-([a-zA-Z0-9_-]+)/);
    const tenant = match && match[1] ? match[1] : null;
    if (tenant) {
      console.warn('[waweb]', `uncaught_singleton_conflict tenant=${tenant}`);
      clearChromeProfileLocks(tenant);
      scheduleSessionReset(tenant, 'uncaught_singleton');
      return;
    }
  }
  console.error('[waweb]', 'uncaught_exception', message);
  process.exit(1);
});

const PORT = process.env.PORT || 8088;
const STATE_DIR = path.resolve(process.env.STATE_DIR || path.join(__dirname, '.wwebjs_auth'));
const APP_WEBHOOK = (process.env.APP_WEBHOOK || '').trim();
const RAW_ADMIN_TOKEN = (process.env.ADMIN_TOKEN || '').trim();
const WAWEB_ADMIN_TOKEN = (process.env.WAWEB_ADMIN_TOKEN || '').trim();
if (WAWEB_ADMIN_TOKEN && RAW_ADMIN_TOKEN && WAWEB_ADMIN_TOKEN !== RAW_ADMIN_TOKEN) {
  console.error('[waweb]', 'admin_token_mismatch');
  process.exit(1);
}
const ADMIN_TOKEN = (WAWEB_ADMIN_TOKEN || RAW_ADMIN_TOKEN);
const WEBHOOK_SECRET = (process.env.WEBHOOK_SECRET || '').trim();
const APP_BASE_URL = (() => {
  const raw = (process.env.APP_BASE_URL || '').trim();
  const fallback = 'http://app:8000';
  const normalized = (raw || fallback).replace(/\/$/, '');
  return normalized || fallback;
})();
const LAST_QR_META_PATH = path.join(STATE_DIR, 'last-qr.json');
const PROVIDER_WEBHOOK_URL = (() => {
  const raw = (APP_WEBHOOK || `${APP_BASE_URL}/webhook`).trim();
  try {
    return new URL(raw).toString();
  } catch (_) {
    try {
      return new URL('/webhook', APP_BASE_URL).toString();
    } catch (_) {
      return `${APP_BASE_URL.replace(/\/$/, '')}/webhook`;
    }
  }
})();
const PROVIDER_TOKEN_REFRESH_INTERVAL_MS = Math.max(
  60,
  Number(process.env.PROVIDER_TOKEN_REFRESH_INTERVAL || '300') || 300,
) * 1000;
const WEB_VERSION_REMOTE_PATH = (() => {
  const raw = (process.env.WEB_VERSION_REMOTE_PATH || '').trim();
  if (raw) return raw;
  return 'https://raw.githubusercontent.com/WhiskeySockets/WhatsAppWebVersions/main/latest.json';
})();
const WINDOWS_CHROME_USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36';

/** @type {{ tenant: string, ts: number, svg: string, png: string, qrId: string | null } | null } */
let lastQrCache = null;

let messageInTotal = 0;
let messageOutTotal = 0;
const sendFailTotal = Object.create(null);
const waSendTotal = Object.create(null);
const waToAppTotals = Object.create(null);
const deprecatedNoticeTs = Object.create(null);
/** sessions[tenant] = { client, browser, page, webhook, state_dir, provider_token, provider_token_ts, qrSvg, qrText, qrPng, ready, lastTs, lastEvent } */
const sessions = Object.create(null);
// Temporary alias to avoid touching legacy helpers that still reference `tenants`
const tenants = sessions;
function logProviderWebhook(eventName, tenantKey, statusCode, tokenPresent) {
  const flag = tokenPresent ? 'true' : 'false';
  try {
    console.log('[waweb]', `wa_to_app event=${eventName} tenant=${tenantKey} code=${statusCode} token_present=${flag}`);
  } catch (_) {}
}
function buildSyncBaseList() {
  const raw = [
    process.env.APP_INTERNAL_URLS || '',
    process.env.APP_INTERNAL_URL || '',
    process.env.APP_PUBLIC_URL || '',
    'http://app:8000',
    'http://localhost:8000',
    'http://127.0.0.1:8000',
  ];
  const bases = [];
  for (const entry of raw) {
    if (!entry) continue;
    const parts = String(entry)
      .split(',')
      .map((value) => value.trim())
      .filter(Boolean);
    for (const part of parts) {
      try {
        const normalized = part.startsWith('http')
          ? new URL(part).toString()
          : new URL(`http://${part}`).toString();
        const cleaned = normalized.replace(/\/$/, '');
        if (!bases.includes(cleaned)) bases.push(cleaned);
      } catch (_) {
        // ignore malformed entries
      }
    }
  }
  return bases;
}
const TENANT_SYNC_BASES = buildSyncBaseList();
const INTERNAL_SYNC_TOKEN = process.env.WA_WEB_TOKEN || process.env.WEBHOOK_SECRET || '';

function incSendFail(reason) {
  const key = String(reason || 'unknown');
  sendFailTotal[key] = (sendFailTotal[key] || 0) + 1;
}

function incWaSend(result) {
  const key = String(result || 'unknown');
  waSendTotal[key] = (waSendTotal[key] || 0) + 1;
}

function incWaToApp(eventName, status) {
  const eventKey = String(eventName || 'unknown');
  const statusKey = String(status || 'unknown');
  if (!waToAppTotals[eventKey]) waToAppTotals[eventKey] = Object.create(null);
  waToAppTotals[eventKey][statusKey] = (waToAppTotals[eventKey][statusKey] || 0) + 1;
}

function recordDeprecated(route) {
  const nowTs = Date.now();
  if ((deprecatedNoticeTs[route] || 0) + 3600 * 1000 <= nowTs) {
    deprecatedNoticeTs[route] = nowTs;
    console.warn('[waweb]', `deprecated_route=${route}`);
  }
}

function sanitizeReason(reason) {
  return String(reason || 'unknown').replace(/[^a-z0-9_]/gi, '_');
}

function logSendResult(tenant, to, result) {
  let jid = '-';
  if (to !== undefined && to !== null) {
    jid = String(to);
  }
  const payload = `event=message_out channel=whatsapp tenant=${tenant} to=${jid} result=${result}`;
  try { console.log('[waweb]', payload); } catch (_) {}
}

function renderMetrics() {
  const lines = [];
  lines.push('# TYPE message_in_total counter');
  lines.push(`message_in_total{channel="whatsapp"} ${messageInTotal}`);
  lines.push('# TYPE messages_out_total counter');
  lines.push(`messages_out_total{channel="whatsapp"} ${messageOutTotal}`);
  lines.push('# TYPE wa_send_total counter');
  const sendResults = Object.keys(waSendTotal);
  if (!sendResults.length) {
    lines.push('wa_send_total{result="success"} 0');
  } else {
    for (const result of sendResults) {
      const value = waSendTotal[result] || 0;
      lines.push(`wa_send_total{result="${sanitizeReason(result)}"} ${value}`);
    }
  }
  lines.push('# TYPE send_fail_total counter');
  const reasons = Object.keys(sendFailTotal);
  if (!reasons.length) {
    lines.push('send_fail_total{channel="whatsapp",reason="unknown"} 0');
  } else {
    for (const reason of reasons) {
      const value = sendFailTotal[reason] || 0;
      lines.push(`send_fail_total{channel="whatsapp",reason="${sanitizeReason(reason)}"} ${value}`);
    }
  }
  lines.push('# TYPE wa_to_app_total counter');
  const toAppEvents = Object.keys(waToAppTotals);
  if (!toAppEvents.length) {
    lines.push('wa_to_app_total{event="unknown",status="none"} 0');
  } else {
    for (const eventName of toAppEvents) {
      const statuses = waToAppTotals[eventName] || {};
      const statusKeys = Object.keys(statuses);
      if (!statusKeys.length) {
        lines.push(`wa_to_app_total{event="${sanitizeReason(eventName)}",status="none"} 0`);
        continue;
      }
      for (const status of statusKeys) {
        const value = statuses[status] || 0;
        lines.push(`wa_to_app_total{event="${sanitizeReason(eventName)}",status="${sanitizeReason(status)}"} ${value}`);
      }
    }
  }
  return `${lines.join('\n')}\n`;
}

/* ---------- helpers ---------- */
function postJson(url, payload) {
  try {
    const u = new URL(url);
    const data = Buffer.from(JSON.stringify(payload), 'utf8');
    const mod = u.protocol === 'https:' ? https : http;
    const req = mod.request({
      hostname: u.hostname, port: u.port || (u.protocol === 'https:' ? 443 : 80),
      path: u.pathname + (u.search || ''), method: 'POST',
      headers: { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': data.length },
      timeout: 8000
    }, res => res.on('data', ()=>{}));
    req.on('error', ()=>{});
    req.write(data); req.end();
  } catch (_) {}
}

function loadLastQrFromDisk() {
  try {
    if (!fs.existsSync(LAST_QR_META_PATH)) return null;
    const raw = fs.readFileSync(LAST_QR_META_PATH, 'utf8');
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return null;
    const tenant = typeof parsed.tenant === 'string' ? parsed.tenant : String(parsed.tenant || '');
    const ts = Number(parsed.ts || parsed.timestamp || 0) || 0;
    const svg = typeof parsed.qr_svg === 'string' ? parsed.qr_svg : '';
    const png = typeof parsed.qr_png === 'string' ? parsed.qr_png : '';
    const qrIdRaw = parsed.qr_id ?? parsed.qrId ?? null;
    let qrId = null;
    if (typeof qrIdRaw === 'string' && qrIdRaw.trim()) qrId = qrIdRaw.trim();
    else if (qrIdRaw !== null && qrIdRaw !== undefined) qrId = String(qrIdRaw);
    if (!qrId && ts) qrId = String(ts);
    if (!svg) return null;
    return { tenant, ts, svg, png, qrId };
  } catch (_) {
    return null;
  }
}

function persistLastQr(tenant, svg, png, ts, qrId) {
  const resolvedTs = Number(ts || 0) || Date.now();
  const resolvedQrId = qrId ? String(qrId) : String(resolvedTs);
  lastQrCache = {
    tenant: String(tenant || ''),
    ts: resolvedTs,
    svg: typeof svg === 'string' ? svg : '',
    png: typeof png === 'string' ? png : '',
    qrId: resolvedQrId,
  };
  try {
    ensureDir(STATE_DIR);
    const payload = {
      tenant: lastQrCache.tenant,
      ts: lastQrCache.ts,
      qr_svg: lastQrCache.svg,
      qr_png: lastQrCache.png,
      qr_id: lastQrCache.qrId,
    };
    fs.writeFileSync(LAST_QR_META_PATH, JSON.stringify(payload));
  } catch (_) {}
}

function getLastQrSnapshot() {
  if (lastQrCache && lastQrCache.svg) return lastQrCache;
  const restored = loadLastQrFromDisk();
  if (restored && restored.svg) {
    lastQrCache = restored;
  }
  return lastQrCache;
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function sessionStatusPayload(tenant, session) {
  const normalizedTenant = String(tenant || '');
  const ready = !!(session && session.ready);
  const hasQr = !!(session && session.qrSvg);
  const lastEvent = session && session.lastEvent ? String(session.lastEvent) : null;
  let qrId = session && session.qrId ? String(session.qrId) : null;
  if (!qrId) {
    const snapshot = getLastQrSnapshot();
    if (snapshot && snapshot.qrId && snapshot.tenant === normalizedTenant) {
      qrId = String(snapshot.qrId);
    }
  }
  let state = null;
  if (hasQr) state = 'qr';
  else if (ready) state = 'ready';
  else if (lastEvent) state = lastEvent;
  const payload = {
    ok: true,
    tenant: normalizedTenant,
    ready,
    qr: hasQr,
    last: lastEvent,
    need_qr: !ready,
  };
  if (state) payload.state = state;
  if (qrId) payload.qr_id = qrId;
  return payload;
}

let requestJsonOverride = null;

function setRequestJsonOverride(fn) {
  requestJsonOverride = typeof fn === 'function' ? fn : null;
}

function requestJson(method, url, payload, extraHeaders) {
  if (requestJsonOverride) {
    return requestJsonOverride(method, url, payload, extraHeaders);
  }
  return new Promise((resolve, reject) => {
    try {
      const u = new URL(url);
      const body = payload ? Buffer.from(JSON.stringify(payload), 'utf8') : null;
      const mod = u.protocol === 'https:' ? https : http;
      const headers = Object.assign(
        { 'Content-Type': 'application/json; charset=utf-8' },
        extraHeaders || {}
      );
      if (body) headers['Content-Length'] = Buffer.byteLength(body);
      const req = mod.request(
        {
          hostname: u.hostname,
          port: u.port || (u.protocol === 'https:' ? 443 : 80),
          path: u.pathname + (u.search || ''),
          method,
          headers,
          timeout: 8000,
        },
        (res) => {
          const chunks = [];
          res.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
          res.on('end', () => {
            const responseBody = Buffer.concat(chunks).toString('utf8');
            resolve({ statusCode: res.statusCode || 0, body: responseBody });
          });
        }
      );
      req.on('error', (err) => reject(err));
      req.on('timeout', () => {
        try { req.destroy(); } catch (_) {}
        reject(new Error('timeout'));
      });
      if (body) req.write(body);
      req.end();
    } catch (err) {
      reject(err);
    }
  });
}

function sessionKey(tenant) {
  return String(tenant ?? '');
}

function sessionEntry(tenant) {
  const key = sessionKey(tenant);
  return tenants[key];
}

function ensureSessionHandles(tenant) {
  const session = sessionEntry(tenant);
  if (!session) return;
  if (!session.client) {
    session.browser = null;
    session.page = null;
    return;
  }
  try {
    session.browser = session.client.pupBrowser || null;
  } catch (_) {
    session.browser = null;
  }
  try {
    session.page = session.client.pupPage || null;
  } catch (_) {
    session.page = null;
  }
}

async function ensureProviderTokenViaInternalEnsure(tenant, nowTs) {
  const key = sessionKey(tenant);
  let url;
  try {
    url = new URL(`/internal/tenant/${encodeURIComponent(key)}/ensure`, APP_BASE_URL).toString();
  } catch (_) {
    url = `${APP_BASE_URL.replace(/\/$/, '')}/internal/tenant/${encodeURIComponent(key)}/ensure`;
  }

  const headers = {};
  const authToken = INTERNAL_SYNC_TOKEN || ADMIN_TOKEN;
  if (authToken) headers['X-Auth-Token'] = authToken;

  try {
    const { statusCode, body } = await requestJson('POST', url, null, headers);
    if (statusCode >= 200 && statusCode < 300 && body) {
      let parsed;
      try {
        parsed = JSON.parse(body);
      } catch (err) {
        const reason = err && err.message ? err.message : err;
        console.warn('[waweb]', `provider_token_ensure_parse_error tenant=${key} reason=${reason}`);
        parsed = null;
      }
      const nextToken = parsed && typeof parsed === 'object'
        ? (parsed.provider_token || parsed.token || '')
        : '';
      if (nextToken) {
        const session = sessionEntry(key);
        if (session) {
          session.provider_token = String(nextToken);
          session.provider_token_ts = typeof nowTs === 'number' && nowTs > 0 ? nowTs : Date.now();
        }
        console.log('[waweb]', `provider_token_ensure_ok tenant=${key}`);
        return String(nextToken);
      }
    } else if (statusCode === 401) {
      console.warn('[waweb]', `provider_token_ensure_unauthorized tenant=${key}`);
    } else {
      console.warn('[waweb]', `provider_token_ensure_http tenant=${key} status=${statusCode}`);
    }
  } catch (err) {
    const reason = err && err.code ? err.code : err && err.message ? err.message : String(err);
    console.warn('[waweb]', `provider_token_ensure_failed tenant=${key} reason=${reason}`);
  }
  return '';
}

async function fetchProviderTokenFromAdmin(tenant, nowTs) {
  const key = sessionKey(tenant);
  let url;
  try {
    url = new URL(`/admin/provider-token/${encodeURIComponent(key)}`, APP_BASE_URL).toString();
  } catch (_) {
    url = `${APP_BASE_URL.replace(/\/$/, '')}/admin/provider-token/${encodeURIComponent(key)}`;
  }

  const headers = {};
  if (ADMIN_TOKEN) headers['X-Admin-Token'] = ADMIN_TOKEN;

  try {
    const { statusCode, body } = await requestJson('GET', url, null, headers);
    if (statusCode >= 200 && statusCode < 300 && body) {
      let parsed;
      try {
        parsed = JSON.parse(body);
      } catch (err) {
        const reason = err && err.message ? err.message : err;
        console.warn('[waweb]', `provider_token_parse_error tenant=${key} reason=${reason}`);
        parsed = null;
      }
      const nextToken = (() => {
        if (!parsed || typeof parsed !== 'object') return '';
        if (parsed.provider_token) return String(parsed.provider_token);
        if (parsed.token) return String(parsed.token);
        return '';
      })();
      if (nextToken) {
        const session = sessionEntry(key);
        if (session) {
          session.provider_token = nextToken;
          session.provider_token_ts = typeof nowTs === 'number' && nowTs > 0 ? nowTs : Date.now();
        }
        return nextToken;
      }
    } else if (statusCode === 404) {
      console.warn('[waweb]', `provider_token_missing tenant=${key}`);
      return ensureProviderTokenViaInternalEnsure(key, nowTs);
    } else if (statusCode === 401) {
      console.warn('[waweb]', `provider_token_unauthorized tenant=${key}`);
      return ensureProviderTokenViaInternalEnsure(key, nowTs);
    } else {
      console.warn('[waweb]', `provider_token_http tenant=${key} status=${statusCode}`);
    }
  } catch (err) {
    const reason = err && err.code ? err.code : err && err.message ? err.message : String(err);
    console.warn('[waweb]', `provider_token_request_failed tenant=${key} reason=${reason}`);
    return ensureProviderTokenViaInternalEnsure(key, nowTs);
  }
  return '';
}

async function ensureSessionProviderToken(tenant, force = false) {
  const session = sessionEntry(tenant);
  if (!session) return '';
  const now = Date.now();
  const ts = Number(session.provider_token_ts || 0);
  if (!force && session.provider_token && now - ts < PROVIDER_TOKEN_REFRESH_INTERVAL_MS) {
    return session.provider_token;
  }
  const token = await fetchProviderTokenFromAdmin(tenant, now);
  if (token) return token;
  session.provider_token_ts = now;
  return session.provider_token || '';
}

async function ensureProviderToken(tenant, force = false) {
  return ensureSessionProviderToken(tenant, force);
}

async function sendProviderEvent(tenant, payload, attempt = 1) {
  const tenantKey = sessionKey(tenant);
  const session = sessionEntry(tenantKey);
  const eventName = payload && typeof payload === 'object' && payload.event
    ? String(payload.event)
    : 'unknown';
  if (!session) {
    incWaToApp(eventName, 'no_session');
    console.warn('[waweb]', `wa_to_app event=${eventName} tenant=${tenantKey} reason=no_session`);
    return { statusCode: 0, body: '' };
  }

  const enrichedPayload = Object.assign({}, payload || {}, {
    tenant: Number(tenantKey),
    channel: 'whatsapp',
  });

  let tries = Math.max(1, Number(attempt) || 1);
  let forceRefresh = tries > 1;

  while (tries <= 3) {
    const token = await ensureSessionProviderToken(tenantKey, forceRefresh);
    forceRefresh = false;
    if (!token) {
      incWaToApp(eventName, 'no_token');
      console.warn('[waweb]', `wa_to_app event=${eventName} code=0 tenant=${tenantKey} no_token`);
      return { statusCode: 0, body: '' };
    }

    let urlWithToken;
    try {
      const u = new URL(PROVIDER_WEBHOOK_URL);
      u.searchParams.set('token', token);
      urlWithToken = u.toString();
    } catch (_) {
      const separator = PROVIDER_WEBHOOK_URL.includes('?') ? '&' : '?';
      urlWithToken = `${PROVIDER_WEBHOOK_URL}${separator}token=${encodeURIComponent(token)}`;
    }

    ensureSessionHandles(tenantKey);
    const tokenTail = token.length <= 6 ? token : token.slice(-6);
    try {
      console.log('[waweb]', `waweb webhook_out tenant=${tenantKey} token_tail=${tokenTail || 'missing'} event=${eventName}`);
    } catch (_) {}

    try {
      const { statusCode, body } = await requestJson('POST', urlWithToken, enrichedPayload, {});
      let statusLabel = 'error';
      if (statusCode >= 200 && statusCode < 300) statusLabel = 'ok';
      else if (statusCode === 401) statusLabel = 'unauthorized';
      else if (statusCode === 422) statusLabel = 'invalid';
      incWaToApp(eventName, statusLabel);
      logProviderWebhook(eventName, tenantKey, statusCode, !!token);
      if (statusCode === 401) {
        forceRefresh = true;
        tries += 1;
        await wait(Math.min(1500, 250 * Math.pow(2, tries - 2)));
        continue;
      }
      if (statusCode >= 500 && tries < 3) {
        tries += 1;
        await wait(Math.min(2500, 400 * Math.pow(2, tries - 2)));
        continue;
      }
      return { statusCode, body };
    } catch (err) {
      const reason = err && err.code ? err.code : err && err.message ? err.message : String(err);
      incWaToApp(eventName, 'exception');
      console.warn('[waweb]', `wa_to_app_exception event=${eventName} tenant=${tenantKey} reason=${reason}`);
      logProviderWebhook(eventName, tenantKey, 0, !!token);
      if (tries < 3) {
        tries += 1;
        forceRefresh = true;
        await wait(Math.min(1500, 250 * Math.pow(2, tries - 2)));
        continue;
      }
      return { statusCode: 0, body: '' };
    }
  }

  return { statusCode: 0, body: '' };
}

function truncateBody(body, limit = 200) {
  if (!body) return '';
  const text = String(body);
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

async function notifyTenantQr(tenant, svg, qrId) {
  if (!svg) {
    console.warn('[waweb]', `wa_qr_callback_skip tenant=${tenant} reason=no_svg`);
    return;
  }
  const qrIdValue = (() => {
    if (typeof qrId === 'string') {
      const trimmed = qrId.trim();
      if (trimmed) return trimmed;
    } else if (qrId !== null && qrId !== undefined) {
      const stringified = String(qrId);
      if (stringified.trim()) return stringified.trim();
    }
    return String(Date.now());
  })();
  const payload = {
    provider: 'whatsapp',
    event: 'qr',
    tenant: Number(tenant),
    channel: 'whatsapp',
    qr_id: qrIdValue,
    svg,
  };
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const { statusCode, body } = await sendProviderEvent(tenant, payload);
    console.log('[waweb]', `wa_qr_callback tenant=${tenant} status=${statusCode} attempt=${attempt}`);
    if (statusCode === 204 || statusCode === 200) {
      return;
    }
    if (statusCode >= 400 && statusCode < 500 && statusCode !== 401) {
      console.warn('[waweb]', `wa_qr_callback_invalid tenant=${tenant} status=${statusCode} body=${truncateBody(body)}`);
      return;
    }
    if (attempt >= 3) {
      console.warn('[waweb]', `wa_qr_callback_error tenant=${tenant} status=${statusCode} body=${truncateBody(body)}`);
      return;
    }
    await wait(500 * attempt);
  }
}
function ensureDir(p){ try{ fs.mkdirSync(p,{recursive:true}); } catch(_){} }
function clearChromeProfileLocks(tenant) {
  try {
    const base = path.join(STATE_DIR, `session-tenant-${tenant}`);
    const lockFiles = ['SingletonLock', 'SingletonSocket', 'SingletonCookie'];
    for (const name of lockFiles) {
      const target = path.join(base, name);
      if (!fs.existsSync(target)) continue;
      try {
        fs.rmSync(target, { force: true });
        console.log('[waweb]', `chrome_lock_removed tenant=${tenant} file=${name}`);
      } catch (err) {
        const reason = err && err.message ? err.message : err;
        console.warn('[waweb]', `chrome_lock_remove_failed tenant=${tenant} file=${name} reason=${reason}`);
      }
    }
  } catch (_) {}
}

function updateSessionState(session, state) {
  if (!session) return;
  const normalized = typeof state === 'string' ? state.toLowerCase() : null;
  if (normalized && session._lastState === normalized) return;
  session._lastState = normalized;
  session._stateSince = now();
}
function clearTenantStateDir(tenant){
  const dir = path.join(STATE_DIR, `session-tenant-${tenant}`);
  const maxAttempts = 5;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      if (!fs.existsSync(dir)) return;
      fs.rmSync(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 200 });
      if (!fs.existsSync(dir)) {
        console.log('[waweb]', `state_dir_cleanup_ok tenant=${tenant} attempt=${attempt}`);
        return;
      }
    } catch (err) {
      const reason = err && err.message ? err.message : String(err);
      console.warn('[waweb]', `state_dir_cleanup_failed tenant=${tenant} attempt=${attempt} reason=${reason}`);
    }
    try { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 200); } catch (_) {}
  }
  if (fs.existsSync(dir)) {
    console.warn('[waweb]', `state_dir_cleanup_incomplete tenant=${tenant}`);
  }
}
function now(){ return Math.floor(Date.now()/1000); }

const UNPAIRED_STATES = new Set(['UNPAIRED', 'UNPAIRED_IDLE', 'LOGGED_OUT', 'LOGOUT']);
const LOGOUT_REASONS = new Set(['LOGOUT', 'LOGGED_OUT']);

function isUnpairedState(state) {
  if (!state) return false;
  const normalized = String(state).trim().toUpperCase();
  if (!normalized) return false;
  if (UNPAIRED_STATES.has(normalized)) return true;
  return normalized.includes('UNPAIRED') || normalized.includes('LOGGED_OUT');
}

function scheduleSessionReset(tenant, source) {
  tenant = String(tenant);
  const session = tenants[tenant];
  if (!session) {
    console.log('[waweb]', `session_reset_skip tenant=${tenant} source=${source} reason=no_session`);
    return;
  }
  if (session._resetScheduled) {
    console.log('[waweb]', `session_reset_skip tenant=${tenant} source=${source} reason=already_scheduled`);
    return;
  }
  session._resetScheduled = true;
  session._stateProbeTs = now();
  const webhookUrl = session.webhook || '';
  console.log('[waweb]', `session_reset_schedule tenant=${tenant} source=${source}`);
  setImmediate(() => {
    try {
      resetSession(tenant, webhookUrl);
    } catch (err) {
      const reason = err && err.message ? err.message : err;
      console.warn('[waweb]', `session_reset_failed tenant=${tenant} source=${source} reason=${reason}`);
      try {
        const current = tenants[tenant];
        if (current) current._resetScheduled = false;
      } catch (_) {}
    }
  });
}

function syncTenantFiles(tenant){
  return new Promise((resolve) => {
    const bases = TENANT_SYNC_BASES.length ? TENANT_SYNC_BASES : ['http://app:8000'];
    let index = 0;

    const attempt = () => {
      if (index >= bases.length) {
        return resolve(false);
      }
      const base = bases[index];
      let url;
      try {
        url = new URL(`${base}/internal/tenant/${tenant}/ensure`);
      } catch (_) {
        log(tenant, `tenant_sync_skip base=${base}`);
        index += 1;
        return attempt();
      }
      const body = Buffer.from(JSON.stringify({ source: 'waweb' }), 'utf8');
      const mod = url.protocol === 'https:' ? https : http;
      const headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Content-Length': body.length,
      };
      if (INTERNAL_SYNC_TOKEN) headers['X-Auth-Token'] = INTERNAL_SYNC_TOKEN;
      const req = mod.request({
        hostname: url.hostname,
        port: url.port || (url.protocol === 'https:' ? 443 : 80),
        path: url.pathname + (url.search || ''),
        method: 'POST',
        headers,
        timeout: 5000
      }, (res) => {
        res.on('data', ()=>{});
        res.on('end', () => {
          if (res.statusCode && res.statusCode >= 400) {
            log(tenant, `tenant_sync_http_${res.statusCode} base=${base}`);
            index += 1;
            attempt();
          } else {
            log(tenant, `tenant_sync_ok base=${base}`);
            resolve(true);
          }
        });
      });
      const fail = (reason) => {
        try { req.destroy(); } catch(_) {}
        log(tenant, `tenant_sync_retry base=${base} reason=${reason}`);
        index += 1;
        attempt();
      };
      req.on('error', (err) => fail(err && err.code ? err.code : 'error'));
      req.on('timeout', () => fail('timeout'));
      req.write(body);
      req.end();
    };

    attempt();
  });
}
function triggerTenantSync(tenant, attempt = 1){
  syncTenantFiles(tenant).then((ok) => {
    if (ok) return;
    log(tenant, `tenant_sync_failed${attempt > 1 ? ' #' + attempt : ''}`);
    if (attempt < 3) {
      setTimeout(() => triggerTenantSync(tenant, attempt + 1), attempt * 2000);
    }
  });
}

function normalizeIncomingMessage(tenant, msg, client){
  const attachments = [];
  if (msg && msg.hasMedia) {
    const mediaId = msg.id && msg.id._serialized ? msg.id._serialized : `media-${Date.now()}`;
    const raw = msg._data || {};
    attachments.push({
      type: (msg.type || 'media').toLowerCase(),
      url: `whatsapp://${tenant}/${mediaId}`,
      name: raw.filename || null,
      mime: raw.mimetype || null,
      size: raw.size || null,
    });
  }
  const ts = Number(msg.timestamp || Math.floor(Date.now() / 1000));
  const providerRaw = typeof msg.toJSON === 'function' ? msg.toJSON() : (msg._data || {});
  const selfId = client && client.info && client.info.wid ? client.info.wid._serialized : '';
  const rawFrom = msg.from || '';
  const fromDigits = typeof rawFrom === 'string' ? rawFrom.replace(/\D/g, '') : '';
  const messageId = (() => {
    if (msg.id && msg.id._serialized) return msg.id._serialized;
    if (msg.id) return String(msg.id);
    if (providerRaw && providerRaw.id) return String(providerRaw.id);
    return `msg-${Date.now()}`;
  })();
  return {
    tenant: Number(tenant),
    channel: 'whatsapp',
    provider: 'whatsapp',
    from: fromDigits,
    from_id: rawFrom || '',
    from_jid: rawFrom || '',
    to: msg.to || selfId || '',
    message_id: messageId,
    text: typeof msg.body === 'string' ? msg.body : '',
    attachments,
    media: attachments,
    ts,
    provider_raw: providerRaw,
  };
}

function normalizeAttachment(raw){
  if (!raw || typeof raw !== 'object') return null;
  const url = raw.url || raw.href;
  if (!url) return null;
  return {
    type: (raw.type || 'file').toString(),
    url: String(url),
    name: raw.name || raw.filename || null,
    mime: raw.mime || raw.mime_type || null,
    size: raw.size || raw.length || null,
  };
}

function normalizeWhatsAppRecipient(value) {
  if (value === null || value === undefined) return null;
  let raw = value;
  if (typeof raw === 'number') raw = String(raw);
  else raw = String(raw).trim();
  if (!raw) return null;
  let local = raw;
  const lowered = raw.toLowerCase();
  if (raw.includes('@')) {
    if (!lowered.endsWith('@c.us')) return null;
    local = raw.split('@', 1)[0];
  }
  let digits = local.replace(/\D/g, '');
  if (!digits) return null;
  if (digits.startsWith('8') && digits.length === 11) {
    digits = `7${digits.slice(1)}`;
  }
  if (digits.length < 10 || digits.length > 15) return null;
  return { digits, jid: `${digits}@c.us` };
}

async function sendTransportMessage(tenant, transport){
  tenant = String(tenant);
  const s = tenants[tenant];
  if (!s || !s.client) {
    const err = new Error('no_session');
    err.normalizedJid = null;
    throw err;
  }

  let target = transport.to;
  if (typeof target === 'string' && target.trim().toLowerCase() === 'me') {
    const me = s.client.info && s.client.info.wid ? s.client.info.wid._serialized : '';
    target = me || '';
  }
  const normalized = normalizeWhatsAppRecipient(target);
  if (!normalized) {
    const err = new Error('invalid_to');
    err.normalizedJid = null;
    throw err;
  }
  const { jid } = normalized;
  transport.to = jid;

  const text = typeof transport.text === 'string' ? transport.text : '';
  const attachments = Array.isArray(transport.attachments) ? transport.attachments : [];
  let textSent = false;
  for (const attachment of attachments) {
    if (!attachment || typeof attachment !== 'object') continue;
    if (!attachment.url) continue;
    try {
      const media = await MessageMedia.fromUrl(String(attachment.url), { unsafeMime: true });
      if (attachment.mime) media.mimetype = attachment.mime;
      if (attachment.name) media.filename = attachment.name;
      const opts = {};
      if (text && !textSent) {
        opts.caption = text;
        textSent = true;
      }
      await s.client.sendMessage(jid, media, opts);
    } catch (err) {
      const error = new Error('media_fetch');
      error.normalizedJid = jid;
      throw error;
    }
  }
  if (text && !textSent) {
    try {
      await s.client.sendMessage(jid, text);
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      error.normalizedJid = jid;
      throw error;
    }
  }
  messageOutTotal += 1;
  return jid;
}
function pickChromePath(){
  const cand = [process.env.CHROME_PATH, '/usr/bin/chromium', '/usr/bin/chromium-browser', '/usr/bin/google-chrome']
    .filter(Boolean);
  for (const c of cand) { try { if (fs.existsSync(c)) return c; } catch(_){} }
  return undefined;
}
function log(t, s){ console.log('[waweb]', s, 't='+t); }

ensureDir(STATE_DIR);
lastQrCache = loadLastQrFromDisk();
refreshProviderTokens(true).catch((err) => {
  const reason = err && err.message ? err.message : err;
  console.warn('[waweb]', `provider_token_initial_refresh_failed reason=${reason}`);
});

async function refreshProviderTokens(force = false) {
  const list = Object.keys(tenants);
  for (const tenant of list) {
    if (!tenant) continue;
    try {
      await ensureSessionProviderToken(tenant, force);
    } catch (err) {
      const reason = err && err.message ? err.message : err;
      console.warn('[waweb]', `provider_token_refresh_failed tenant=${tenant} reason=${reason}`);
    }
  }
}

async function safeDestroy(client) {
  if (!client) return;
  try { await client.destroy(); } catch(_) {}
  try { if (client.pupBrowser) await client.pupBrowser.close(); } catch(_) {}
}

function handleClientInitError(tenant, err) {
  const message = err && err.message ? err.message : String(err);
  if (message && message.includes('Singleton')) {
    clearChromeProfileLocks(tenant);
    console.warn('[waweb]', `client_init_singleton_conflict tenant=${tenant}`);
    scheduleSessionReset(tenant, 'init_singleton_lock');
    return;
  }
  if (message && message.includes('Failed to launch the browser process')) {
    console.warn('[waweb]', `client_init_browser_launch_failed tenant=${tenant} reason=${message}`);
    scheduleSessionReset(tenant, 'init_browser_failed');
    return;
  }
  console.warn('[waweb]', `client_init_error tenant=${tenant} reason=${message}`);
}

function initializeClient(tenant, session) {
  if (!session || !session.client) return;
  try {
    const initResult = session.client.initialize();
    if (initResult && typeof initResult.catch === 'function') {
      initResult.catch((err) => handleClientInitError(tenant, err));
    }
  } catch (err) {
    handleClientInitError(tenant, err);
  }
}

function buildClient(tenant) {
  const chromePath = pickChromePath();
  const opts = {
    authStrategy: new LocalAuth({ clientId: 'tenant-'+tenant, dataPath: STATE_DIR }),
    webVersionCache: {
      type: 'remote',
      remotePath: WEB_VERSION_REMOTE_PATH,
    },
    takeoverOnConflict: true,
    takeoverTimeoutMs: 0,
    userAgent: WINDOWS_CHROME_USER_AGENT,
    puppeteer: {
      headless: true,
      executablePath: chromePath,
      defaultViewport: {
        width: 1280,
        height: 800,
      },
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--window-size=1280,800',
        '--no-zygote',
        '--disable-gpu',
        '--disable-background-timer-throttling',
        '--disable-backgrounding-occluded-windows',
        '--disable-renderer-backgrounding'
      ]
    }
  };
  const c = new Client(opts);
  const updateHandles = () => ensureSessionHandles(tenant);

  c.on('loading_screen', (p, t) => log(tenant, `loading ${p}% ${t||''}`));
  c.on('qr', async (qr) => {
    updateHandles();
    const qrId = Date.now();
    let svg = '';
    let png = '';
    try {
      svg = await QRCode.toString(qr, { type: 'svg' });
    } catch (err) {
      console.warn('[waweb]', `qr_svg_render_failed t=${tenant} reason=${err && err.message ? err.message : err}`);
    }
    try {
      const dataUrl = await QRCode.toDataURL(qr, { type: 'image/png' });
      png = (dataUrl || '').split(',').pop() || '';
    } catch (err) {
      console.warn('[waweb]', `qr_png_render_failed t=${tenant} reason=${err && err.message ? err.message : err}`);
    }
    if (svg) tenants[tenant].qrSvg = svg;
    tenants[tenant].qrText = qr;
    tenants[tenant].qrPng = png || null;
    tenants[tenant].ready = false;
    tenants[tenant].lastEvent = 'qr';
    tenants[tenant].lastTs = now();
    tenants[tenant].qrId = String(qrId);
    tenants[tenant]._resetScheduled = false;
    tenants[tenant]._stateProbeTs = now();
    updateSessionState(tenants[tenant], 'qr');
    if (svg || png) persistLastQr(tenant, svg, png, qrId, qrId);
    try {
      await notifyTenantQr(tenant, svg, qrId);
    } catch (_) {}
    log(tenant, 'qr');
    triggerTenantSync(tenant);
  });
  c.on('authenticated', () => {
    updateHandles();
    tenants[tenant].lastEvent = 'authenticated';
    tenants[tenant].lastTs = now();
    tenants[tenant].qrPng = null;
    tenants[tenant].qrId = null;
    tenants[tenant]._resetScheduled = false;
    tenants[tenant]._stateProbeTs = now();
    updateSessionState(tenants[tenant], 'authenticated');
    log(tenant, 'authenticated');
    triggerTenantSync(tenant);
  });
  c.on('auth_failure', (m) => {
    updateHandles();
    tenants[tenant].ready = false;
    tenants[tenant].qrSvg = null;
    tenants[tenant].qrPng = null;
    tenants[tenant].qrId = null;
    tenants[tenant].lastEvent = 'auth_failure';
    tenants[tenant].lastTs = now();
    tenants[tenant]._resetScheduled = false;
    tenants[tenant]._stateProbeTs = now();
    updateSessionState(tenants[tenant], 'auth_failure');
    log(tenant, 'auth_failure ' + (m||''));
  });
  c.on('ready', () => {
    updateHandles();
    tenants[tenant].ready = true;
    tenants[tenant].qrSvg = null;
    tenants[tenant].qrPng = null;
    tenants[tenant].qrId = null;
    tenants[tenant].lastEvent = 'ready';
    tenants[tenant].lastTs = now();
     tenants[tenant]._resetScheduled = false;
    tenants[tenant]._stateProbeTs = now();
    updateSessionState(tenants[tenant], 'ready');
    log(tenant, 'ready');
    triggerTenantSync(tenant);
    (async () => {
      try {
        await sendProviderEvent(tenant, {
          event: 'ready',
          tenant: Number(tenant),
          channel: 'whatsapp',
          provider: 'whatsapp',
          state: 'ready',
          ts: Date.now(),
        });
      } catch (err) {
        const reason = err && err.message ? err.message : err;
        console.warn('[waweb]', `ready_event_send_failed tenant=${tenant} reason=${reason}`);
      }
    })();
  });
  c.on('disconnected', async (reason) => {
    updateHandles();
    const reasonKey = String(reason || '').toUpperCase();
    tenants[tenant].ready = false;
    tenants[tenant].qrPng = null;
    tenants[tenant].qrId = null;
    tenants[tenant].lastEvent = 'disconnected';
    tenants[tenant].lastTs = now();
    tenants[tenant]._stateProbeTs = now();
    updateSessionState(tenants[tenant], 'disconnected');
    log(tenant, 'disconnected ' + reasonKey);
    if (LOGOUT_REASONS.has(reasonKey)) {
      scheduleSessionReset(tenant, `disconnected:${reasonKey || 'unknown'}`);
      return;
    }
    setTimeout(() => { try { c.initialize(); } catch(_){} }, 1500);
  });
  c.on('change_state', (state) => {
    updateHandles();
    const rawState = String(state || '');
    const lowered = rawState.toLowerCase();
    tenants[tenant].lastTs = now();
    tenants[tenant].lastEvent = `state:${lowered}`;
    tenants[tenant]._stateProbeTs = now();
    updateSessionState(tenants[tenant], lowered);
    log(tenant, `state ${lowered}`);
    if (isUnpairedState(rawState)) {
      tenants[tenant].ready = false;
      tenants[tenant].qrSvg = null;
      tenants[tenant].qrPng = null;
      tenants[tenant].qrId = null;
      scheduleSessionReset(tenant, `state:${rawState}`);
    }
  });
  c.on('message', (msg) => {
    updateHandles();
    if (msg && typeof msg.from === 'string' && msg.from.toLowerCase() === 'status@broadcast') {
      return;
    }
    tenants[tenant].lastTs = now();
    tenants[tenant]._stateProbeTs = now();
    const normalized = normalizeIncomingMessage(tenant, msg, c);
    if (
      normalized &&
      typeof normalized.from_jid === 'string' &&
      normalized.from_jid.toLowerCase() === 'status@broadcast'
    ) {
      return;
    }
    messageInTotal += 1;
    try { console.log('[waweb]', `event=message_in channel=whatsapp tenant=${tenant} from=${normalized.from_jid || normalized.from || '-'}`); } catch(_){}
    (async () => {
      const payload = {
        event: 'messages.incoming',
        tenant: Number(tenant),
        channel: 'whatsapp',
        provider: 'whatsapp',
        message_id: normalized.message_id,
        from: normalized.from,
        from_jid: normalized.from_jid || normalized.from_id || '',
        text: normalized.text || '',
        ts: normalized.ts,
      };
      if (Array.isArray(normalized.media) && normalized.media.length) payload.media = normalized.media;
      if (normalized.to) payload.to = normalized.to;
      if (normalized.provider_raw) payload.provider_raw = normalized.provider_raw;
      try {
        await sendProviderEvent(tenant, payload);
      } catch (err) {
        const reason = err && err.message ? err.message : err;
        console.warn('[waweb]', `message_event_send_failed tenant=${tenant} reason=${reason}`);
      }
    })();
  });
  return c;
}

function ensureSession(tenant, webhookUrl) {
  tenant = String(tenant);
  if (!tenants[tenant]) {
    ensureDir(STATE_DIR);
    const stateDir = path.join(STATE_DIR, `session-tenant-${tenant}`);
    ensureDir(stateDir);
    clearChromeProfileLocks(tenant);
    tenants[tenant] = {
      client: null,
      browser: null,
      page: null,
      webhook: webhookUrl || '',
      qrSvg: null,
      qrText: null,
      qrPng: null,
      qrId: null,
      ready: false,
      lastTs: now(),
      lastEvent: 'init',
      _resetScheduled: false,
      _stateProbeTs: 0,
      _lastState: null,
      _stateSince: now(),
      state_dir: stateDir,
      provider_token: null,
      provider_token_ts: 0,
    };
    tenants[tenant].client = buildClient(tenant);
    ensureSessionHandles(tenant);
    initializeClient(tenant, tenants[tenant]);
    updateSessionState(tenants[tenant], 'init');
    log(tenant, 'init');
    triggerTenantSync(tenant);
    ensureSessionProviderToken(tenant).catch((err) => {
      const reason = err && err.message ? err.message : err;
      console.warn('[waweb]', `provider_token_ensure_failed tenant=${tenant} reason=${reason}`);
    });
  }
  if (webhookUrl) tenants[tenant].webhook = webhookUrl;

  const s = tenants[tenant];
  // анти-зависание: если >25с нет qr и не ready, и последнее событие не 'qr' — мягкий реиниц.
  if (!s.ready && !s.qrSvg && s.lastEvent !== 'qr' && (now() - (s.lastTs||0) > 25)) {
    (async () => {
      log(tenant, 'reinit');
      await safeDestroy(s.client);
      clearChromeProfileLocks(tenant);
      s.client = buildClient(tenant);
      s.lastTs = now(); s.lastEvent = 'reinit'; s._stateProbeTs = 0;
      updateSessionState(s, 'reinit');
      initializeClient(tenant, s);
    })();
  }
  return s;
}

// Periodic watchdog: re-init stuck sessions without relying on external calls
setInterval(() => {
  try {
    const ts = now();
    for (const t of Object.keys(tenants)) {
      const s = tenants[t];
      if (!s) continue;
      const client = s.client;
      try {
        const browser = client && client.pupBrowser;
        const page = client && client.pupPage;
        const browserDisconnected =
          !!(browser && typeof browser.isConnected === 'function' && !browser.isConnected());
        const pageClosed = !!(page && typeof page.isClosed === 'function' && page.isClosed());
        if (client && (browserDisconnected || pageClosed)) {
          const reason = browserDisconnected ? 'browser_disconnected' : 'page_closed';
          log(t, `watchdog_reset ${reason}`);
          scheduleSessionReset(t, `watchdog:${reason}`);
          continue;
        }
      } catch (err) {
        const reason = err && err.message ? err.message : err;
        console.warn('[waweb]', `watchdog_probe_failed tenant=${t} reason=${reason}`);
      }
      if (s.ready && client && typeof client.getState === 'function') {
        const lastProbe = s._stateProbeTs || 0;
        if (ts - lastProbe > 20) {
          s._stateProbeTs = ts;
          (async () => {
            const prevState = s._lastState;
            const prevSince = s._stateSince || ts;
            try {
              const state = await client.getState();
              const normalized = typeof state === 'string' ? state.toLowerCase() : '';
              if (normalized) {
                const duration = prevState === normalized ? ts - prevSince : 0;
                updateSessionState(s, normalized);
                if (normalized === 'opening' && duration > 45) {
                  log(t, `watchdog_state_stuck ${state} duration=${duration}s`);
                  scheduleSessionReset(t, 'state_probe:opening_timeout');
                  return;
                }
                if (normalized !== 'connected' && normalized !== 'open' && normalized !== 'opening') {
                  log(t, `watchdog_state ${state}`);
                  scheduleSessionReset(t, `state_probe:${state || 'unknown'}`);
                  return;
                }
              } else {
                updateSessionState(s, state);
              }
            } catch (err) {
              const reason = err && err.message ? err.message : err;
              console.warn('[waweb]', `watchdog_state_failed tenant=${t} reason=${reason}`);
              scheduleSessionReset(t, 'state_probe_error');
            }
          })();
        }
      }
      // If not ready and no QR for >25s and last event wasn't QR -> soft reinit
      if (!s.ready && !s.qrSvg && s.lastEvent !== 'qr' && (ts - (s.lastTs || 0) > 25)) {
        (async () => {
          log(t, 'reinit_timer');
          await safeDestroy(s.client);
          clearChromeProfileLocks(t);
          s.client = buildClient(t);
          s.lastTs = now();
          s.lastEvent = 'reinit';
          s._stateProbeTs = 0;
          updateSessionState(s, 'reinit');
          initializeClient(t, s);
        })();
      }
    }
  } catch (_) {}
}, 5000);

setInterval(() => {
  refreshProviderTokens().catch((err) => {
    const reason = err && err.message ? err.message : err;
    console.warn('[waweb]', `provider_token_refresh_loop_error reason=${reason}`);
  });
}, PROVIDER_TOKEN_REFRESH_INTERVAL_MS);

function resetSession(tenant, webhookUrl) {
  tenant = String(tenant);
  const s = tenants[tenant];
  (async () => { try { await safeDestroy(s?.client); } catch(_) {} })();
  // снести локальные данные авторизации
  clearTenantStateDir(tenant);
  delete tenants[tenant];
  return ensureSession(tenant, webhookUrl);
}

/* ---------- server ---------- */
const app = express();
app.use(bodyParser.json({ limit: '1mb' }));

app.get('/health', (_req, res) => res.json({ ok: true, service: 'waweb' }));

app.get('/metrics', (_req, res) => {
  res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
  return res.send(renderMetrics());
});

function authorized(req){
  if (!INTERNAL_SYNC_TOKEN) return true;
  const h = (req.headers['x-auth-token'] || '').toString().trim();
  return h && h === INTERNAL_SYNC_TOKEN;
}

app.post('/session/start', (req, res) => {
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = req.body?.tenant_id ?? req.body?.tenant;
  const hook = req.body?.webhook_url || req.body?.webhook || '';
  if (!t) return res.status(400).json({ ok:false, error:'no_tenant' });
  const s = ensureSession(t, hook);
  return res.json(sessionStatusPayload(t, s));
});

// Preferred explicit tenant start endpoint
app.post('/session/:tenant/start', (req, res) => {
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = String(req.params.tenant||'');
  const hook = req.body?.webhook_url || req.body?.webhook || '';
  if (!t) return res.status(400).json({ ok:false, error:'no_tenant' });
  const s = ensureSession(t, hook);
  return res.json(sessionStatusPayload(t, s));
});

app.post('/session/:tenant/launch', (req, res) => {
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = String(req.params.tenant||'');
  const s = ensureSession(t, req.body?.webhook_url || '');
  try { s.client.initialize(); s.lastTs = now(); s.lastEvent = 'launch'; } catch(_) {}
  return res.json({ ok:true });
});

app.get('/session/:tenant/status', (req, res) => {
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = String(req.params.tenant||'');
  const s = tenants[t];
  if (!s) return res.status(404).json({ ok:false, error:'no_session' });
  return res.json(sessionStatusPayload(t, s));
});

app.get('/session/qr.svg', (req, res) => {
  if (!authorized(req)) return res.status(401).type('image/svg+xml').send('');
  const snapshot = getLastQrSnapshot();
  if (!snapshot || !snapshot.svg) {
    try { console.log('[waweb]', 'qr_route_last_svg_404'); } catch(_){}
    return res.status(404).type('image/svg+xml').send('');
  }
  res.setHeader('Cache-Control','no-cache, no-store, must-revalidate');
  res.setHeader('Pragma','no-cache');
  res.setHeader('Expires','0');
  if (snapshot.qrId) {
    res.setHeader('ETag', `\"${snapshot.qrId}\"`);
    res.setHeader('X-QR-Id', snapshot.qrId);
  }
  try { console.log('[waweb]', 'qr_route_last_svg_200', 't='+snapshot.tenant, 'ts='+snapshot.ts); } catch(_){}
  return res.type('image/svg+xml').send(snapshot.svg);
});

app.get('/session/:tenant/qr.svg', (req, res) => {
  if (!authorized(req)) return res.status(401).type('image/svg+xml').send('');
  const t = String(req.params.tenant||'');
  const s = tenants[t];
  let svg = s && s.qrSvg ? s.qrSvg : '';
  let qrId = s && s.qrId ? String(s.qrId) : null;
  if (!svg) {
    const snapshot = getLastQrSnapshot();
    if (snapshot && snapshot.svg && snapshot.tenant === t) {
      svg = snapshot.svg;
      if (s) {
        s.qrSvg = svg;
        s.qrId = snapshot.qrId ? String(snapshot.qrId) : s.qrId;
        qrId = s.qrId;
      }
    }
  }
  if (!svg) {
    try { console.log('[waweb]', 'qr_route_svg_404', 't='+t, 'ready='+(!!s&&!!s.ready)); } catch(_){}
    return res.status(404).type('image/svg+xml').send('');
  }
  try { console.log('[waweb]', 'qr_route_svg_200', 't='+t, 'len='+(svg?svg.length:0)); } catch(_){}
  res.setHeader('Cache-Control','no-cache, no-store, must-revalidate');
  res.setHeader('Pragma','no-cache');
  res.setHeader('Expires','0');
  if (qrId) {
    res.setHeader('ETag', `\"${qrId}\"`);
    res.setHeader('X-QR-Id', qrId);
  }
  return res.type('image/svg+xml').send(svg);
});

app.get('/session/:tenant/qr.png', async (req, res) => {
  if (!authorized(req)) return res.status(401).type('image/png').send('');
  const t = String(req.params.tenant||'');
  const s = tenants[t];
  if (!s || (!s.qrText && !s.qrPng)) {
    try { console.log('[waweb]', 'qr_route_png_404', 't='+t, 'ready='+(!!s&&!!s.ready)); } catch(_){}
    return res.status(404).type('image/png').send('');
  }
  try {
    let buf;
    if (s.qrPng) {
      const b64 = String(s.qrPng).includes(',') ? String(s.qrPng).split(',').pop() : s.qrPng;
      buf = Buffer.from(b64 || '', 'base64');
    }
    if (!buf || !buf.length) {
      buf = await QRCode.toBuffer(s.qrText, { type: 'png' });
    }
    res.setHeader('Cache-Control','no-cache, no-store, must-revalidate');
    res.setHeader('Pragma','no-cache');
    res.setHeader('Expires','0');
    if (s && s.qrId) {
      res.setHeader('ETag', `"${s.qrId}"`);
      res.setHeader('X-QR-Id', String(s.qrId));
    }
    try { console.log('[waweb]', 'qr_route_png_200', 't='+t, 'len='+(buf?buf.length:0)); } catch(_){}
    return res.type('image/png').send(buf);
  } catch(_) {
    return res.status(500).type('application/json').json({ ok:false, error:'qr_png_failed' });
  }
});

app.post('/session/:tenant/send', async (req, res) => {
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  recordDeprecated('/session/:tenant/send');
  const t = String(req.params.tenant||'');
  const tenantNum = Number(t || TENANT_DEFAULT);
  const payload = req.body || {};
  const attachmentsRaw = [];
  if (payload.attachment) attachmentsRaw.push(payload.attachment);
  if (Array.isArray(payload.attachments)) attachmentsRaw.push(...payload.attachments);
  const attachments = attachmentsRaw.map(normalizeAttachment).filter(Boolean);
  const transport = {
    tenant: tenantNum,
    channel: 'whatsapp',
    to: payload.to || payload.phone || '',
    text: typeof payload.text === 'string' ? payload.text : '',
    attachments,
  };
  if (!transport.text.trim() && !attachments.length) {
    return res.status(400).json({ ok:false, error:'empty_message' });
  }
  try {
    const jid = await sendTransportMessage(tenantNum, transport);
    incWaSend('success');
    logSendResult(tenantNum, jid, 'success');
    return res.json({ ok:true });
  } catch (e) {
    const message = e && e.message ? e.message : String(e);
    const normalizedJid = (e && e.normalizedJid) || null;
    const toValue = normalizedJid || transport.to || '-';
    if (message === 'invalid_to') {
      incWaSend('invalid_to');
      logSendResult(tenantNum, toValue, 'invalid_to');
      return res.status(400).json({ ok:false, error:'invalid_to' });
    }
    if (message === 'no_session') {
      incWaSend('no_session');
      logSendResult(tenantNum, toValue, 'no_session');
      return res.status(404).json({ ok:false, error:'no_session' });
    }
    const resultTag = sanitizeReason(message || 'error');
    incSendFail(message);
    incWaSend(resultTag);
    logSendResult(tenantNum, toValue, resultTag);
    return res.status(500).json({ ok:false, error:message });
  }
});

app.post('/send', async (req, res) => {
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const payload = req.body || {};
  const channel = (payload.channel || '').toString().toLowerCase();
  if (channel && channel !== 'whatsapp') {
    return res.status(400).json({ ok:false, error:'channel_mismatch' });
  }
  const tenantNum = Number(payload.tenant || payload.tenant_id || TENANT_DEFAULT);
  if (!tenantNum) return res.status(400).json({ ok:false, error:'no_tenant' });
  const attachments = Array.isArray(payload.attachments)
    ? payload.attachments.map(normalizeAttachment).filter(Boolean)
    : [];
  const text = typeof payload.text === 'string' ? payload.text : '';
  if (!text.trim() && !attachments.length) {
    return res.status(400).json({ ok:false, error:'empty_message' });
  }
  try {
    const jid = await sendTransportMessage(tenantNum, { to: payload.to, text, attachments });
    incWaSend('success');
    logSendResult(tenantNum, jid, 'success');
    return res.json({ ok:true });
  } catch (e) {
    const message = e && e.message ? e.message : String(e);
    const normalizedJid = (e && e.normalizedJid) || null;
    const toValue = normalizedJid || payload.to || '-';
    if (message === 'invalid_to') {
      incWaSend('invalid_to');
      logSendResult(tenantNum, toValue, 'invalid_to');
      return res.status(400).json({ ok:false, error:'invalid_to' });
    }
    if (message === 'no_session') {
      incWaSend('no_session');
      logSendResult(tenantNum, toValue, 'no_session');
      return res.status(404).json({ ok:false, error:'no_session' });
    }
    const resultTag = sanitizeReason(message || 'error');
    incSendFail(message);
    incWaSend(resultTag);
    logSendResult(tenantNum, toValue, resultTag);
    return res.status(500).json({ ok:false, error:message });
  }
});

app.post('/session/:tenant/restart', (req,res)=>{
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = String(req.params.tenant||'');
  const hook = req.body?.webhook_url || req.body?.webhook || '';
  const s = resetSession(t, hook);
  return res.json(sessionStatusPayload(t, s));
});

app.post('/session/restart', (req, res) => {
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = req.body?.tenant_id || req.body?.tenant;
  const hook = req.body?.webhook_url || req.body?.webhook || '';
  if (!t) return res.status(400).json({ ok:false, error:'no_tenant' });
  const s = resetSession(t, hook);
  return res.json(sessionStatusPayload(t, s));
});

app.post('/session/:tenant/reset', (req,res)=>{
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = String(req.params.tenant||'');
  const hook = req.body?.webhook_url || '';
  const s = resetSession(t, hook);
  return res.json({ ok:true, reset:true, qr: !!s.qrSvg, ready: !!s.ready });
});

if (require.main === module) {
  app.listen(PORT, () => console.log('waweb on :' + PORT));
}

module.exports = {
  app,
  ensureProviderToken,
  ensureSessionProviderToken,
  ensureProviderTokenViaInternalEnsure,
  requestJson,
  setRequestJsonOverride,
  sessions,
};
