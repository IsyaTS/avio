const express = require('express');
const config = require('./config');
const logger = require('./logger');
const { SessionManager } = require('./session-manager');
const { HttpError } = require('./errors');

const app = express();
const sessionManager = new SessionManager({ stateDir: config.stateDir, logger });

app.use(express.json({ limit: '10mb' }));

const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

const parseTenant = (value) => {
  if (value === undefined || value === null) {
    throw new HttpError(400, 'tenant_required', 'tenant is required');
  }
  const text = String(value).trim();
  if (!text) {
    throw new HttpError(400, 'tenant_required', 'tenant is required');
  }
  if (!/^[0-9]+$/.test(text)) {
    throw new HttpError(400, 'invalid_tenant', 'tenant must be numeric');
  }
  return text;
};

const parseBool = (value) => {
  if (typeof value === 'boolean') {
    return value;
  }
  if (value === undefined || value === null) {
    return false;
  }
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
};

const isPlainObject = (value) =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

const normalizeSendBody = (body) => {
  const normalizedTo =
    typeof body.to === 'string' && body.to.trim()
      ? body.to.trim()
      : body.to && typeof body.to === 'number'
        ? String(body.to)
        : '';
  const attachments = Array.isArray(body.attachments)
    ? body.attachments.filter(isPlainObject)
    : [];
  const singleAttachment = isPlainObject(body.attachment) ? body.attachment : null;
  const payload = isPlainObject(body.payload) ? { ...body.payload } : {};
  let type = typeof body.type === 'string' ? body.type.toLowerCase() : '';
  if (!type) {
    const inferred =
      (payload.type && String(payload.type).toLowerCase()) ||
      (attachments[0]?.type && String(attachments[0].type).toLowerCase()) ||
      (singleAttachment?.type && String(singleAttachment.type).toLowerCase()) ||
      '';
    if (inferred) {
      type = inferred;
    } else if (attachments.length || singleAttachment) {
      type = 'document';
    } else {
      type = 'text';
    }
  }
  const textValue = typeof body.text === 'string' ? body.text : '';
  const resultPayload = { ...payload };
  if (type === 'text') {
    resultPayload.text = resultPayload.text || textValue;
  } else if (!Object.keys(resultPayload).length) {
    const attachment = attachments[0] || singleAttachment;
    if (attachment) {
      Object.assign(resultPayload, attachment);
    }
  }
  return {
    to: normalizedTo,
    text: textValue,
    type,
    payload: resultPayload,
  };
};

app.get('/health', (req, res) => {
  res.json({ ok: true });
});

app.post(
  '/sessions/start',
  asyncHandler(async (req, res) => {
    const tenant = parseTenant(req.body?.tenant);
    let webhookUrl;
    if (typeof req.body?.webhookUrl === 'string') {
      webhookUrl = req.body.webhookUrl.trim();
    } else if (typeof req.body?.webhook_url === 'string') {
      webhookUrl = req.body.webhook_url.trim();
    }
    const forceReset = parseBool(req.body?.force);
    const options = { webhookUrl };
    if (forceReset) {
      options.forceReset = true;
    }
    await sessionManager.ensureSession(tenant, options);
    const summary = sessionManager.getSummary(tenant);
    res.json({ ok: true, session: summary });
  })
);

app.get(
  '/sessions/status',
  asyncHandler(async (req, res) => {
    const tenant = parseTenant(req.query?.tenant ?? req.query?.id);
    const summary = sessionManager.getSummary(tenant);
    if (!summary) {
      throw new HttpError(404, 'session_not_found', 'Session not initialized');
    }
    res.json({ ok: true, session: summary });
  })
);

app.post(
  '/messages/send',
  asyncHandler(async (req, res) => {
    const body = req.body || {};
    const tenant = parseTenant(body.tenant);
    if (!body.to) {
      throw new HttpError(400, 'invalid_recipient', 'Recipient "to" is required');
    }
    const normalized = normalizeSendBody(body);
    const response = await sessionManager.sendMessage(tenant, {
      tenant,
      to: normalized.to,
      type: normalized.type,
      payload: normalized.payload,
      text: normalized.text,
    });
    try {
      console.log(
        '[BAILEYS SEND]',
        `tenant=${tenant}`,
        `raw_to=${body.to || '-'}`,
        `normalized_jid=${normalized.to || '-'}`
      );
    } catch (_) {}
    res.json({
      ok: true,
      tenant,
      status: 'sent',
      messageId: response?.key?.id || null,
    });
  })
);

app.use((err, req, res, next) => {
  if (err instanceof HttpError) {
    logger.warn({ err: err.message, code: err.code, status: err.statusCode }, 'http_error');
    return res.status(err.statusCode).json({ ok: false, error: err.code, detail: err.message });
  }
  logger.error({ err }, 'unhandled_error');
  return res.status(500).json({ ok: false, error: 'internal_error' });
});

async function start() {
  await sessionManager.init();
  app.listen(config.port, config.host, () => {
    logger.info({ host: config.host, port: config.port }, 'wabaileys server started');
  });
}

start().catch((err) => {
  logger.error({ err }, 'fatal_startup_error');
  process.exit(1);
});
