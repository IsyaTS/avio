const express = require('express');
const bodyParser = require('body-parser');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const QRCode = require('qrcode');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8088;
const STATE_DIR = path.resolve(process.env.STATE_DIR || path.join(__dirname, '.wwebjs_auth'));
const APP_WEBHOOK = (process.env.APP_WEBHOOK || '').trim();
const TENANT_DEFAULT = Number(process.env.TENANT_DEFAULT || '0') || 0;
const ADMIN_TOKEN = (process.env.ADMIN_TOKEN || '').trim();
const WEBHOOK_SECRET = (process.env.WEBHOOK_SECRET || '').trim();
const APP_BASE_URL = (() => {
  const raw = (process.env.APP_BASE_URL || '').trim();
  const fallback = 'http://app:8000';
  const normalized = (raw || fallback).replace(/\/$/, '');
  return normalized || fallback;
})();
const LAST_QR_META_PATH = path.join(STATE_DIR, 'last-qr.json');

/** @type {{ tenant: string, ts: number, svg: string, png: string } | null } */
let lastQrCache = null;

let messageInTotal = 0;
let messageOutTotal = 0;
const sendFailTotal = Object.create(null);
const deprecatedNoticeTs = Object.create(null);
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

function renderMetrics() {
  const lines = [];
  lines.push('# TYPE message_in_total counter');
  lines.push(`message_in_total{channel="whatsapp"} ${messageInTotal}`);
  lines.push('# TYPE message_out_total counter');
  lines.push(`message_out_total{channel="whatsapp"} ${messageOutTotal}`);
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
    if (!svg) return null;
    return { tenant, ts, svg, png };
  } catch (_) {
    return null;
  }
}

function persistLastQr(tenant, svg, png, ts) {
  lastQrCache = {
    tenant: String(tenant || ''),
    ts: Number(ts || 0) || Date.now(),
    svg: typeof svg === 'string' ? svg : '',
    png: typeof png === 'string' ? png : '',
  };
  try {
    ensureDir(STATE_DIR);
    const payload = {
      tenant: lastQrCache.tenant,
      ts: lastQrCache.ts,
      qr_svg: lastQrCache.svg,
      qr_png: lastQrCache.png,
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

function requestJson(method, url, payload, extraHeaders) {
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

function truncateBody(body, limit = 200) {
  if (!body) return '';
  const text = String(body);
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

async function notifyTenantQr(tenant, svg, qrId, pngBase64, qrText) {
  if (!svg) {
    console.warn('[waweb]', `wa_qr_callback_skip tenant=${tenant} reason=no_svg`);
    return;
  }
  const url = `${APP_BASE_URL}/webhook/provider`;
  const payload = {
    provider: 'whatsapp',
    event: 'wa_qr',
    tenant: Number(tenant),
    qr_id: qrId,
    svg,
  };
  if (pngBase64) payload.png_base64 = pngBase64;
  if (qrText) payload.txt = qrText;
  const headers = {};
  if (WEBHOOK_SECRET) headers['X-Webhook-Token'] = WEBHOOK_SECRET;
  else if (ADMIN_TOKEN) headers['X-Webhook-Token'] = ADMIN_TOKEN;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const { statusCode, body } = await requestJson('POST', url, payload, headers);
      console.log('[waweb]', `wa_qr_callback tenant=${tenant} status=${statusCode} attempt=${attempt}`);
      if (statusCode === 204) {
        return;
      }
      if (attempt < 3) {
        console.warn('[waweb]', `wa_qr_callback_retry tenant=${tenant} status=${statusCode}`);
        await wait(500 * attempt);
        continue;
      }
      console.warn('[waweb]', `wa_qr_callback_error tenant=${tenant} status=${statusCode} body=${truncateBody(body)}`);
      return;
    } catch (err) {
      const reason = err && err.code ? err.code : err && err.message ? err.message : String(err);
      console.warn('[waweb]', `wa_qr_callback_exception tenant=${tenant} attempt=${attempt} reason=${reason}`);
      if (attempt >= 3) return;
      await wait(500 * attempt);
    }
  }
}
function ensureDir(p){ try{ fs.mkdirSync(p,{recursive:true}); } catch(_){} }
function now(){ return Math.floor(Date.now()/1000); }
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
  return {
    tenant: Number(tenant),
    channel: 'whatsapp',
    from_id: msg.from || '',
    to: msg.to || selfId || '',
    text: typeof msg.body === 'string' ? msg.body : '',
    attachments,
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

async function sendTransportMessage(tenant, transport){
  tenant = String(tenant);
  const s = tenants[tenant];
  if (!s || !s.client) throw new Error('no_session');

  let target = transport.to;
  if (typeof target === 'string' && target.trim().toLowerCase() === 'me') {
    const me = s.client.info && s.client.info.wid ? s.client.info.wid._serialized : '';
    target = me || '';
  }
  if (typeof target === 'number') target = String(target);
  if (typeof target !== 'string') throw new Error('invalid_to');

  let jid;
  if (target.includes('@')) {
    jid = target;
  } else {
    const digits = target.replace(/\D/g, '');
    if (!digits) throw new Error('invalid_to');
    jid = `${digits}@c.us`;
  }

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
      throw new Error('media_fetch');
    }
  }
  if (text && !textSent) {
    await s.client.sendMessage(jid, text);
  }
  messageOutTotal += 1;
  try { console.log('[waweb]', `event=message_out channel=whatsapp tenant=${tenant} to=${jid}`); } catch(_){}
  return true;
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

/* ---------- state ---------- */
/** tenants[tenant] = { client, webhook, qrSvg, qrText, qrPng, ready, lastTs, lastEvent } */
const tenants = Object.create(null);

async function safeDestroy(client) {
  if (!client) return;
  try { await client.destroy(); } catch(_) {}
  try { if (client.pupBrowser) await client.pupBrowser.close(); } catch(_) {}
}

function buildClient(tenant) {
  const chromePath = pickChromePath();
  const opts = {
    authStrategy: new LocalAuth({ clientId: 'tenant-'+tenant, dataPath: STATE_DIR }),
    puppeteer: {
      headless: true,
      executablePath: chromePath,
      args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--no-zygote','--disable-gpu']
    }
  };
  const c = new Client(opts);

  c.on('loading_screen', (p, t) => log(tenant, `loading ${p}% ${t||''}`));
  c.on('qr', async (qr) => {
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
    tenants[tenant].qrId = qrId;
    if (svg || png) persistLastQr(tenant, svg, png, qrId);
    try {
      await notifyTenantQr(tenant, svg, qrId, png || null, qr || '');
    } catch (_) {}
    log(tenant, 'qr');
    triggerTenantSync(tenant);
  });
  c.on('authenticated', () => {
    tenants[tenant].lastEvent = 'authenticated';
    tenants[tenant].lastTs = now();
    tenants[tenant].qrPng = null;
    log(tenant, 'authenticated');
    triggerTenantSync(tenant);
  });
  c.on('auth_failure', (m) => {
    tenants[tenant].ready = false;
    tenants[tenant].qrSvg = null;
    tenants[tenant].qrPng = null;
    tenants[tenant].lastEvent = 'auth_failure';
    tenants[tenant].lastTs = now();
    log(tenant, 'auth_failure ' + (m||''));
  });
  c.on('ready', () => {
    tenants[tenant].ready = true;
    tenants[tenant].qrSvg = null;
    tenants[tenant].qrPng = null;
    tenants[tenant].lastEvent = 'ready';
    tenants[tenant].lastTs = now();
    log(tenant, 'ready');
    triggerTenantSync(tenant);
  });
  c.on('disconnected', (reason) => {
    tenants[tenant].ready = false;
    tenants[tenant].qrPng = null;
    tenants[tenant].lastEvent = 'disconnected';
    tenants[tenant].lastTs = now();
    log(tenant, 'disconnected ' + reason);
    setTimeout(() => { try { c.initialize(); } catch(_){} }, 1500);
  });
  c.on('message', (msg) => {
    tenants[tenant].lastTs = now();
    const normalized = normalizeIncomingMessage(tenant, msg, c);
    messageInTotal += 1;
    try { console.log('[waweb]', `event=message_in channel=whatsapp tenant=${tenant} from=${normalized.from_id}`); } catch(_){}
    const hook = APP_WEBHOOK || tenants[tenant].webhook || '';
    if (hook) postJson(hook, normalized);
  });
  return c;
}

function ensureSession(tenant, webhookUrl) {
  tenant = String(tenant);
  if (!tenants[tenant]) {
    ensureDir(STATE_DIR);
    ensureDir(path.join(STATE_DIR, `session-tenant-${tenant}`));
    tenants[tenant] = { client: null, webhook: webhookUrl || '', qrSvg: null, qrText: null, qrPng: null, qrId: null, ready: false, lastTs: now(), lastEvent: 'init' };
    tenants[tenant].client = buildClient(tenant);
    tenants[tenant].client.initialize();
    log(tenant, 'init');
    triggerTenantSync(tenant);
  }
  if (webhookUrl) tenants[tenant].webhook = webhookUrl;

  const s = tenants[tenant];
  // анти-зависание: если >25с нет qr и не ready, и последнее событие не 'qr' — мягкий реиниц.
  if (!s.ready && !s.qrSvg && s.lastEvent !== 'qr' && (now() - (s.lastTs||0) > 25)) {
    (async () => {
      log(tenant, 'reinit');
      await safeDestroy(s.client);
      s.client = buildClient(tenant);
      s.lastTs = now(); s.lastEvent = 'reinit';
      try { s.client.initialize(); } catch(_) {}
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
      // If not ready and no QR for >25s and last event wasn't QR -> soft reinit
      if (!s.ready && !s.qrSvg && s.lastEvent !== 'qr' && (ts - (s.lastTs || 0) > 25)) {
        (async () => {
          log(t, 'reinit_timer');
          await safeDestroy(s.client);
          s.client = buildClient(t);
          s.lastTs = now();
          s.lastEvent = 'reinit';
          try { s.client.initialize(); } catch (_) {}
        })();
      }
    }
  } catch (_) {}
}, 5000);

function resetSession(tenant, webhookUrl) {
  tenant = String(tenant);
  const s = tenants[tenant];
  (async () => { try { await safeDestroy(s?.client); } catch(_) {} })();
  // снести локальные данные авторизации
  try {
    const authDir = path.join(STATE_DIR, `session-tenant-${tenant}`);
    if (fs.existsSync(authDir)) fs.rmSync(authDir, { recursive: true, force: true });
  } catch(_) {}
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
  return res.json({ ok:true, tenant:String(t), ready:!!s.ready, qr:!!s.qrSvg, last:s.lastEvent });
});

// Preferred explicit tenant start endpoint
app.post('/session/:tenant/start', (req, res) => {
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = String(req.params.tenant||'');
  const hook = req.body?.webhook_url || req.body?.webhook || '';
  if (!t) return res.status(400).json({ ok:false, error:'no_tenant' });
  const s = ensureSession(t, hook);
  return res.json({ ok:true, tenant:String(t), ready:!!s.ready, qr:!!s.qrSvg, last:s.lastEvent });
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
  return res.json({ ok:true, tenant:t, ready:!!s.ready, qr:!!s.qrSvg, last:s.lastEvent });
});

app.get('/session/qr.svg', (req, res) => {
  if (!authorized(req)) return res.status(401).type('image/svg+xml').send('');
  const snapshot = getLastQrSnapshot();
  if (!snapshot || !snapshot.svg) {
    try { console.log('[waweb]', 'qr_route_last_svg_404'); } catch(_){}
    return res.status(404).type('image/svg+xml').send('');
  }
  res.setHeader('Cache-Control','no-store');
  try { console.log('[waweb]', 'qr_route_last_svg_200', 't='+snapshot.tenant, 'ts='+snapshot.ts); } catch(_){}
  return res.type('image/svg+xml').send(snapshot.svg);
});

app.get('/session/:tenant/qr.svg', (req, res) => {
  if (!authorized(req)) return res.status(401).type('image/svg+xml').send('');
  const t = String(req.params.tenant||'');
  const s = tenants[t];
  if (!s || !s.qrSvg) {
    try { console.log('[waweb]', 'qr_route_svg_404', 't='+t, 'ready='+(!!s&&!!s.ready)); } catch(_){}
    return res.status(404).type('image/svg+xml').send('');
  }
  try { console.log('[waweb]', 'qr_route_svg_200', 't='+t, 'len='+(s.qrSvg?s.qrSvg.length:0)); } catch(_){}
  res.setHeader('Cache-Control','no-store');
  return res.type('image/svg+xml').send(s.qrSvg);
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
    res.setHeader('Cache-Control','no-store');
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
    await sendTransportMessage(tenantNum, transport);
    return res.json({ ok:true });
  } catch (e) {
    const message = e && e.message ? e.message : String(e);
    if (message === 'invalid_to') {
      return res.status(400).json({ ok:false, error:'invalid_to' });
    }
    if (message === 'no_session') {
      return res.status(404).json({ ok:false, error:'no_session' });
    }
    incSendFail(message);
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
    await sendTransportMessage(tenantNum, { to: payload.to, text, attachments });
    return res.json({ ok:true });
  } catch (e) {
    const message = e && e.message ? e.message : String(e);
    if (message === 'invalid_to') {
      return res.status(400).json({ ok:false, error:'invalid_to' });
    }
    if (message === 'no_session') {
      return res.status(404).json({ ok:false, error:'no_session' });
    }
    incSendFail(message);
    return res.status(500).json({ ok:false, error:message });
  }
});

app.post('/session/:tenant/restart', (req,res)=>{
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = String(req.params.tenant||'');
  const hook = req.body?.webhook_url || req.body?.webhook || '';
  const s = resetSession(t, hook);
  return res.json({ ok:true, tenant:t, ready:!!s.ready, qr:!!s.qrSvg, last:s.lastEvent });
});

app.post('/session/restart', (req, res) => {
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = req.body?.tenant_id || req.body?.tenant;
  const hook = req.body?.webhook_url || req.body?.webhook || '';
  if (!t) return res.status(400).json({ ok:false, error:'no_tenant' });
  const s = resetSession(t, hook);
  return res.json({ ok:true, tenant:String(t), ready:!!s.ready, qr:!!s.qrSvg, last:s.lastEvent });
});

app.post('/session/:tenant/reset', (req,res)=>{
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = String(req.params.tenant||'');
  const hook = req.body?.webhook_url || '';
  const s = resetSession(t, hook);
  return res.json({ ok:true, reset:true, qr: !!s.qrSvg, ready: !!s.ready });
});

app.listen(PORT, ()=> console.log('waweb on :'+PORT));
