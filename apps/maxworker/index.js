'use strict';

const express = require('express');

const { cfg } = require('./config');
const { MaxPersonalSessionManager } = require('./session-manager');

const app = express();
app.use(express.json({ limit: '1mb' }));

const manager = new MaxPersonalSessionManager();

function authOk(req) {
  if (!cfg.authToken) return true;
  const token =
    String(req.headers['x-auth-token'] || '').trim() ||
    String(req.query.token || '').trim() ||
    (() => {
      const auth = String(req.headers.authorization || '').trim();
      if (!auth.toLowerCase().startsWith('bearer ')) return '';
      return auth.slice(7).trim();
    })();
  return token === cfg.authToken;
}

function requireAuth(req, res, next) {
  if (authOk(req)) return next();
  return res.status(401).json({ ok: false, error: 'unauthorized' });
}

function parseTenant(req) {
  const raw = req.body.tenant ?? req.body.tenant_id ?? req.query.tenant ?? req.query.tenant_id;
  const tenant = Number.parseInt(String(raw ?? ''), 10);
  return Number.isFinite(tenant) && tenant > 0 ? tenant : 0;
}

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    service: 'maxworker',
    kill_switch: cfg.killSwitch,
    mock_mode: cfg.mockMode,
  });
});

app.get('/metrics', (_req, res) => {
  res.type('text/plain; version=0.0.4').send(manager.metrics.render());
});

app.post('/session/start', requireAuth, async (req, res) => {
  const tenant = parseTenant(req);
  if (!tenant) return res.status(400).json({ ok: false, error: 'tenant_required' });
  const callbackUrl = String(req.body.callback_url || req.body.webhook || '').trim();
  if (!callbackUrl) return res.status(400).json({ ok: false, error: 'callback_url_required' });
  const force = Boolean(req.body.force);
  const webhookToken = String(req.body.webhook_token || '').trim();
  const payload = await manager.startSession({
    tenant,
    callbackUrl,
    webhookToken,
    force,
  });
  if (!payload.ok) return res.status(503).json(payload);
  return res.json(payload);
});

app.get('/session/qr', requireAuth, async (req, res) => {
  const tenant = parseTenant(req);
  if (!tenant) return res.status(400).json({ ok: false, error: 'tenant_required' });
  const payload = await manager.getQr(tenant);
  if (!payload.ok) {
    if (payload.error === 'qr_not_available') return res.status(404).json(payload);
    if (payload.error === 'qr_expired') return res.status(410).json(payload);
    return res.status(400).json(payload);
  }
  return res.json(payload);
});

app.get('/session/status', requireAuth, async (req, res) => {
  const tenant = parseTenant(req);
  if (!tenant) return res.status(400).json({ ok: false, error: 'tenant_required' });
  const payload = await manager.getStatus(tenant);
  return res.json(payload);
});

app.post('/session/logout', requireAuth, async (req, res) => {
  const tenant = parseTenant(req);
  if (!tenant) return res.status(400).json({ ok: false, error: 'tenant_required' });
  const payload = await manager.logout(tenant);
  return res.json(payload);
});

app.post('/send', requireAuth, async (req, res) => {
  const tenant = parseTenant(req);
  if (!tenant) return res.status(400).json({ ok: false, error: 'tenant_required' });
  const toValue = req.body.to ?? req.body.chat_id ?? req.body.peer;
  if (!toValue) return res.status(400).json({ ok: false, error: 'to_required' });
  const text = String(req.body.text || '').trim();
  const attachments = Array.isArray(req.body.attachments)
    ? req.body.attachments.filter((item) => item && typeof item === 'object')
    : [];
  if (!text && !attachments.length) {
    return res.status(400).json({ ok: false, error: 'text_required' });
  }
  const result = await manager.send({
    tenant,
    to: toValue,
    text,
    attachments,
    dedupe_key: req.body.dedupe_key || null,
    idempotency_key: req.body.idempotency_key || null,
  });
  return res.status(result.status).json(result.body);
});

app.post('/events/inbound', requireAuth, async (req, res) => {
  const result = await manager.ingestInbound(req.body || {});
  return res.status(result.status).json(result.body);
});

async function main() {
  const server = app.listen(cfg.port, cfg.host, () => {
    console.log(`[maxworker] listening on ${cfg.host}:${cfg.port} mock=${cfg.mockMode ? 1 : 0}`);
  });

  manager.restoreSessions().catch((err) => {
    console.error(`[maxworker] restore failed: ${err && err.message ? err.message : err}`);
  });
  manager.startWatchdog();

  process.on('SIGTERM', async () => {
    await manager.shutdown().catch(() => undefined);
    server.close(() => process.exit(0));
  });

  process.on('SIGINT', async () => {
    await manager.shutdown().catch(() => undefined);
    server.close(() => process.exit(0));
  });
}

main().catch((err) => {
  console.error(`[maxworker] fatal: ${err && err.stack ? err.stack : err}`);
  process.exit(1);
});
