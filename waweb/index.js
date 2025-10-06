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
function pickChromePath(){
  const cand = [process.env.CHROME_PATH, '/usr/bin/chromium', '/usr/bin/chromium-browser', '/usr/bin/google-chrome']
    .filter(Boolean);
  for (const c of cand) { try { if (fs.existsSync(c)) return c; } catch(_){} }
  return undefined;
}
function log(t, s){ console.log('[waweb]', s, 't='+t); }

/* ---------- state ---------- */
/** tenants[tenant] = { client, webhook, qrSvg, qrText, ready, lastTs, lastEvent } */
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
    tenants[tenant].qrSvg = await QRCode.toString(qr, { type: 'svg' });
    tenants[tenant].qrText = qr;
    tenants[tenant].ready = false;
    tenants[tenant].lastEvent = 'qr';
    tenants[tenant].lastTs = now();
    log(tenant, 'qr');
    triggerTenantSync(tenant);
  });
  c.on('authenticated', () => {
    tenants[tenant].lastEvent = 'authenticated';
    tenants[tenant].lastTs = now();
    log(tenant, 'authenticated');
    triggerTenantSync(tenant);
  });
  c.on('auth_failure', (m) => {
    tenants[tenant].ready = false;
    tenants[tenant].qrSvg = null;
    tenants[tenant].lastEvent = 'auth_failure';
    tenants[tenant].lastTs = now();
    log(tenant, 'auth_failure ' + (m||''));
  });
  c.on('ready', () => {
    tenants[tenant].ready = true;
    tenants[tenant].qrSvg = null;
    tenants[tenant].lastEvent = 'ready';
    tenants[tenant].lastTs = now();
    log(tenant, 'ready');
    triggerTenantSync(tenant);
  });
  c.on('disconnected', (reason) => {
    tenants[tenant].ready = false;
    tenants[tenant].lastEvent = 'disconnected';
    tenants[tenant].lastTs = now();
    log(tenant, 'disconnected ' + reason);
    setTimeout(() => { try { c.initialize(); } catch(_){} }, 1500);
  });
  c.on('message', (msg) => {
    tenants[tenant].lastTs = now();
    const payload = {
      source: { type: 'whatsapp', tenant: Number(tenant) },
      message: { id: msg.id? msg.id._serialized:undefined, from: msg.from, author: msg.author, body: msg.body, text: msg.body },
      ts: Date.now(), leadId: Date.now()
    };
    const hook = tenants[tenant].webhook;
    if (hook) postJson(hook, payload);
  });
  return c;
}

function ensureSession(tenant, webhookUrl) {
  tenant = String(tenant);
  if (!tenants[tenant]) {
    ensureDir(STATE_DIR);
    ensureDir(path.join(STATE_DIR, `session-tenant-${tenant}`));
    tenants[tenant] = { client: null, webhook: webhookUrl || '', qrSvg: null, qrText: null, ready: false, lastTs: now(), lastEvent: 'init' };
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
  if (!s || !s.qrText) {
    try { console.log('[waweb]', 'qr_route_png_404', 't='+t, 'ready='+(!!s&&!!s.ready)); } catch(_){}
    return res.status(404).type('image/png').send('');
  }
  try {
    const buf = await QRCode.toBuffer(s.qrText, { type: 'png' });
    res.setHeader('Cache-Control','no-store');
    try { console.log('[waweb]', 'qr_route_png_200', 't='+t, 'len='+(buf?buf.length:0)); } catch(_){}
    return res.type('image/png').send(buf);
  } catch(_) {
    return res.status(500).type('application/json').json({ ok:false, error:'qr_png_failed' });
  }
});

app.post('/session/:tenant/send', async (req, res) => {
  if (!authorized(req)) return res.status(401).json({ ok:false, error:'unauthorized' });
  const t = String(req.params.tenant||'');
  const s = tenants[t];
  if (!s) return res.status(404).json({ ok:false, error:'no_session' });
  const to = (req.body?.to||'').replace(/\D/g,'');
  const text = (req.body?.text||'').toString();
  const attachment = req.body?.attachment && typeof req.body.attachment === 'object' ? req.body.attachment : null;
  if (!to || (!text && !attachment)) return res.status(400).json({ ok:false, error:'bad_params' });
  try {
    const jid = to.endsWith('@c.us') ? to : `${to}@c.us`;
    if (attachment && attachment.url) {
      const opts = {};
      if (attachment.filename) opts.filename = attachment.filename;
      if (text) opts.caption = text;
      const media = await MessageMedia.fromUrl(String(attachment.url), { unsafeMime: true });
      if (attachment.mime_type && typeof attachment.mime_type === 'string') {
        media.mimetype = attachment.mime_type;
      }
      await s.client.sendMessage(jid, media, opts);
    } else {
      await s.client.sendMessage(jid, text);
    }
    return res.json({ ok:true });
  } catch(e) {
    return res.status(500).json({ ok:false, error:String(e) });
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
