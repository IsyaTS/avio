const path = require('path');
const fs = require('fs/promises');
const EventEmitter = require('events');
const QRCode = require('qrcode');
const baileys = require('@whiskeysockets/baileys');
const { createAuthStore } = require('./auth-store');
const { HttpError } = require('./errors');
const config = require('./config');
const baseLogger = require('./logger');
const { WebhookClient } = require('./webhook-client');

const {
  default: makeWASocket,
  fetchLatestBaileysVersion,
  DisconnectReason,
  Browsers,
  BufferJSON,
  jidNormalizedUser,
} = baileys;
const fallbackBaileysVersion = require('@whiskeysockets/baileys/lib/Defaults/baileys-version.json');

const DEFAULT_BROWSER = Browsers.macOS('Avio WABA');

const tenantKey = (tenant) => String(tenant || '').trim();

class SessionManager extends EventEmitter {
  constructor(options = {}) {
    super();
    this.stateDir = options.stateDir || config.stateDir;
    this.logger = (options.logger || baseLogger).child({ module: 'session-manager' });
    this.sessions = new Map();
    this.creating = new Map();
    this.baileysVersion = null;
    this.qrTtlMs = options.qrTtlMs || config.qrTtlMs;
    this.webhookClient =
      options.webhookClient ||
      new WebhookClient({
        baseUrl: options.appWebhookUrl || config.appWebhookUrl,
        appBaseUrl: options.appBaseUrl || config.appBaseUrl,
        logger: this.logger,
        refreshMs: config.providerTokenRefreshMs,
        internalAuthToken: config.internalAuthToken,
      });
  }

  async init() {
    await fs.mkdir(this.stateDir, { recursive: true });
    await this._selectBaileysVersion();
    await this._bootstrapExistingSessions();
  }

  async _selectBaileysVersion() {
    if (this.baileysVersion) {
      return this.baileysVersion;
    }
    let versionInfo;
    try {
      versionInfo = await fetchLatestBaileysVersion();
    } catch (err) {
      this.logger.warn({ err }, 'baileys_version_fetch_failed');
      versionInfo = { version: fallbackBaileysVersion.version, isLatest: false };
    }
    const version = versionInfo?.version || fallbackBaileysVersion.version;
    this.logger.info(
      { version: Array.isArray(version) ? version.join('.') : version, isLatest: versionInfo?.isLatest ?? false },
      'baileys_version_selected'
    );
    this.baileysVersion = version;
    return this.baileysVersion;
  }

  async _bootstrapExistingSessions() {
    const entries = await fs.readdir(this.stateDir, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isDirectory()) {
        continue;
      }
      const match = entry.name.match(/^tenant-(.+)$/);
      if (!match) {
        continue;
      }
      const tenant = match[1];
      if (!tenant) {
        continue;
      }
      this.logger.info({ tenant }, 'bootstrap_session');
      this.ensureSession(tenant).catch((err) => {
        this.logger.error({ tenant, err }, 'bootstrap_session_failed');
      });
    }
  }

  async ensureSession(tenant, options = {}) {
    const key = tenantKey(tenant);
    if (!key) {
      throw new HttpError(400, 'invalid_tenant', 'Tenant is required');
    }
    const current = this.sessions.get(key);
    if (current) {
      return current;
    }

    if (this.creating.has(key)) {
      return this.creating.get(key);
    }

    if (options.forceReset) {
      await this._disposeSession(key, { clearAuth: true });
    }
    const creationOptions = { ...options };
    delete creationOptions.forceReset;

    const creation = this._createSession(key, creationOptions).catch((err) => {
      this.logger.error({ tenant: key, err }, 'session_create_failed');
      this.creating.delete(key);
      throw err;
    });
    this.creating.set(key, creation);
    const session = await creation;
    this.creating.delete(key);
    return session;
  }

  async _createSession(tenant, options = {}) {
    const tenantDir = path.join(this.stateDir, `tenant-${tenant}`);
    const authDir = path.join(tenantDir, 'auth');
    await fs.mkdir(tenantDir, { recursive: true });
    const auth = await createAuthStore(authDir, this.logger);
    const version = await this._selectBaileysVersion();

    const socket = makeWASocket({
      auth: auth.state,
      version,
      printQRInTerminal: false,
      browser: DEFAULT_BROWSER,
      markOnlineOnConnect: false,
      syncFullHistory: false,
      getMessage: async () => undefined,
    });

    const session = {
      tenantId: tenant,
      socket,
      auth,
      status: 'connecting',
      lastError: null,
      qr: null,
      lastSeen: null,
      selfJid: resolveSelfJid({ socket: null, auth }),
      paths: { tenantDir, authDir },
      webhookUrl: options.webhookUrl || null,
    };

    this.sessions.set(tenant, session);
    this.logger.info({ tenant }, 'session_created');

    socket.ev.on('creds.update', (creds) => {
      auth.saveCreds(creds);
      const derived = resolveSelfJid({ socket, auth });
      if (derived) {
        session.selfJid = derived;
      }
    });
    socket.ev.on('connection.update', (update) => {
      this._handleConnectionUpdate(tenant, session, update).catch((err) => {
        this.logger.error({ tenant, err }, 'connection_update_error');
      });
    });
    socket.ev.on('messages.upsert', (packet) => {
      this._handleMessages(tenant, session, packet).catch((err) => {
        this.logger.error({ tenant, err }, 'messages_upsert_error');
      });
    });

    return session;
  }

  async _handleConnectionUpdate(tenant, session, update) {
    const { connection, lastDisconnect, qr } = update;
    if (qr) {
      await this._storeQr(session, qr);
      session.status = 'qr';
      session.lastError = null;
      this.logger.info({ tenant }, 'session_qr_generated');
      await this._emitQrEvent(tenant, session);
    }

    if (connection === 'open') {
      const derived = resolveSelfJid({ socket: session.socket, auth: session.auth });
      if (derived) {
        session.selfJid = derived;
      }
      session.status = 'connected';
      session.qr = null;
      session.lastError = null;
      session.lastSeen = Date.now();
      this.logger.info({ tenant }, 'session_connected');
      await this._emitReadyEvent(tenant, session);
      return;
    }

    if (connection === 'connecting') {
      session.status = 'connecting';
      return;
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const reason = lastDisconnect?.error?.message || update.reason || '';
      session.lastError = reason || `close_${statusCode || 'unknown'}`;
      session.status = statusCode === DisconnectReason.loggedOut ? 'error' : 'disconnected';

      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
      if (statusCode === DisconnectReason.loggedOut) {
        this.logger.warn({ tenant }, 'session_logged_out_clear_state');
        await this._disposeSession(tenant, { clearAuth: true });
        return;
      }

      if (shouldReconnect) {
        this.logger.warn({ tenant, statusCode }, 'session_reconnect_scheduled');
        await this._disposeSession(tenant, { clearAuth: false });
        setTimeout(() => {
          this.ensureSession(tenant).catch((err) => {
            this.logger.error({ tenant, err }, 'session_reconnect_failed');
          });
        }, 1000);
      }
    }
  }

  async _handleMessages(tenant, session, packet) {
    if (!packet || packet.type !== 'notify') {
      return;
    }
    const messages = packet.messages || [];
    for (const msg of messages) {
      if (!msg || msg.broadcast) {
        continue;
      }
      if (msg.key?.fromMe) {
        const outgoing = this._normalizeOutgoingMessage(tenant, msg, session);
        if (!outgoing) {
          continue;
        }
        outgoing.manager = true;
        this.logger.info(
          { tenant, to: outgoing.from_jid, messageId: outgoing.message_id },
          'handoff_outgoing_message'
        );
        await this._sendWebhookEvent(tenant, outgoing, session);
        continue;
      }
      const payload = this._normalizeIncomingMessage(tenant, msg, session);
      if (!payload) {
        continue;
      }
      this.logger.info(
        { tenant, from: payload.from_jid, to: payload.to, messageId: payload.message_id },
        'incoming_message'
      );
      try {
        console.log(
          '[BAILEYS INBOUND]',
          `tenant=${tenant}`,
          `remoteJid=${msg.key?.remoteJid || '-'}`,
          `from_jid=${payload.from_jid || '-'}`,
          `from_digits=${payload.from || '-'}`,
          `msg_id=${payload.message_id || '-'}`
        );
      } catch (_) {}
      await this._sendWebhookEvent(tenant, payload, session);
    }
  }

  _normalizeOutgoingMessage(tenant, msg, session) {
    const remoteJid = normalizeUserJid(msg.key?.remoteJid);
    const isGroup = remoteJid.endsWith('@g.us');
    if (!remoteJid || isGroup) {
      return null;
    }
    const messageId = msg.key?.id || `msg-${Date.now()}`;
    const body = unwrapMessage(msg.message);
    const text_value = extractText(body);
    const media = buildIncomingMediaEntries(tenant, messageId, body);

    const payload = {
      event: 'messages.outgoing',
      tenant: Number(tenant),
      channel: 'whatsapp',
      provider: 'whatsapp',
      message_id: messageId,
      from: remoteJid.replace(/\D/g, ''),
      from_jid: remoteJid,
      from_raw: remoteJid,
      to: session?.selfJid || remoteJid,
      text: text_value || '',
      ts: Number(msg.messageTimestamp || Math.floor(Date.now() / 1000)),
    };

    if (media.length) {
      payload.media = media;
    }
    try {
      payload.provider_raw = JSON.parse(JSON.stringify(msg, BufferJSON.replacer));
    } catch (_) {
      payload.provider_raw = null;
    }
    return payload;
  }

  _normalizeIncomingMessage(tenant, msg, session) {
    const remoteJid = normalizeUserJid(msg.key?.remoteJid);
    const isGroup = remoteJid.endsWith('@g.us');
    const participantJid = normalizeUserJid(msg.key?.participant || msg.participant);
    const resolvedSelf = resolveSelfJid(session);
    if (resolvedSelf && resolvedSelf !== session?.selfJid) {
      session.selfJid = resolvedSelf;
    }
    let senderJid = remoteJid;
    if (isGroup) {
      senderJid = participantJid || remoteJid;
    } else if (participantJid && participantJid !== remoteJid) {
      senderJid = participantJid;
    }
    if (!senderJid && participantJid) {
      senderJid = participantJid;
    }
    if (!senderJid) {
      senderJid = remoteJid;
    }
    const fromDigits = senderJid.replace(/\D/g, '');
    if (!senderJid || !fromDigits) {
      return null;
    }
    const messageId = msg.key?.id || `msg-${Date.now()}`;
    const body = unwrapMessage(msg.message);
    const text_value = extractText(body);
    const media = buildIncomingMediaEntries(tenant, messageId, body);
    const recipientJid = isGroup ? remoteJid : session?.selfJid || resolvedSelf || remoteJid;

    const payload = {
      event: 'messages.incoming',
      tenant: Number(tenant),
      channel: 'whatsapp',
      provider: 'whatsapp',
      message_id: messageId,
      from: fromDigits,
      from_jid: senderJid,
      from_raw: senderJid,
      to: recipientJid || remoteJid,
      text: text_value || '',
      ts: Number(msg.messageTimestamp || Math.floor(Date.now() / 1000)),
    };

    if (media.length) {
      payload.media = media;
    }
    if (isGroup) {
      payload.conversation_id = remoteJid;
    }
    try {
      payload.provider_raw = JSON.parse(JSON.stringify(msg, BufferJSON.replacer));
    } catch (_) {
      payload.provider_raw = null;
    }
    return payload;
  }

  async _storeQr(session, qrText) {
    try {
      const [dataUrl, svg] = await Promise.all([
        QRCode.toDataURL(qrText, { type: 'image/png', margin: 1, scale: 6 }),
        QRCode.toString(qrText, { type: 'svg', margin: 1 }),
      ]);
      const [, base64Part] = dataUrl.split(',');
      const generatedAt = Date.now();
      session.qr = {
        id: `qr-${generatedAt}`,
        raw: qrText,
        png: base64Part || dataUrl,
        svg,
        generatedAt,
      };
    } catch (err) {
      this.logger.error({ err }, 'qr_encode_failed');
      session.qr = {
        id: `qr-${Date.now()}`,
        raw: qrText,
        png: null,
        svg: null,
        generatedAt: Date.now(),
      };
    }
  }

  getSummary(tenant) {
    const key = tenantKey(tenant);
    const session = this.sessions.get(key);
    if (!session) {
      return null;
    }
    let qr = null;
    if (session.qr && Date.now() - session.qr.generatedAt <= this.qrTtlMs) {
      qr = {
        raw: session.qr.raw,
        png: session.qr.png,
        svg: session.qr.svg,
        generatedAt: session.qr.generatedAt,
        expiresAt: session.qr.generatedAt + this.qrTtlMs,
      };
    }
    return {
      tenant: session.tenantId,
      status: session.status,
      connected: session.status === 'connected',
      lastError: session.lastError,
      lastSeen: session.lastSeen,
      qr,
    };
  }

  async sendMessage(tenant, request) {
    const key = tenantKey(tenant);
    const session = await this.ensureSession(key);
    if (!session) {
      throw new HttpError(404, 'session_not_found', 'Session not initialized');
    }
    if (session.status !== 'connected') {
      throw new HttpError(409, 'session_not_ready', 'Session is not connected');
    }

    const jid = normalizeJid(request.to);
    const type = String(request.type || 'text').toLowerCase();
    const payload = request.payload || {};
    let content;

    if (type === 'text') {
      const text = payload.text || request.text || '';
      if (!text.trim()) {
        throw new HttpError(400, 'text_required', 'Text payload is empty');
      }
      content = { text };
    } else if (type === 'image') {
      const media = await resolveMedia(payload);
      content = {
        image: media.source,
        caption: payload.caption || '',
      };
    } else if (type === 'document') {
      const media = await resolveMedia(payload);
      content = {
        document: media.source,
        mimetype: media.mime || payload.mimetype || 'application/octet-stream',
        fileName: media.fileName || payload.fileName || payload.filename || 'document',
        caption: payload.caption || '',
      };
    } else {
      throw new HttpError(400, 'unsupported_type', `Unsupported type: ${type}`);
    }

    try {
      console.log('[BAILEYS SOCKET SEND]', `tenant=${tenant}`, `jid=${jid}`, `type=${type}`);
    } catch (_) {}
    const response = await session.socket.sendMessage(jid, content);
    session.lastSeen = Date.now();
    this.logger.info({ tenant: key, type, to: jid, messageId: response?.key?.id }, 'message_sent');
    return response;
  }

  async _clearAuthState(authDir) {
    try {
      await fs.rm(authDir, { recursive: true, force: true });
    } catch (err) {
      this.logger.warn({ err }, 'auth_state_remove_failed');
    }
  }

  async _disposeSession(tenant, options = {}) {
    const key = tenantKey(tenant);
    const existing = this.sessions.get(key);
    if (existing) {
      this.sessions.delete(key);
      try {
        if (existing.socket?.ws) {
          existing.socket.ws.close();
        } else if (existing.socket?.end) {
          existing.socket.end();
        }
      } catch (err) {
        this.logger.warn({ tenant: key, err }, 'session_socket_close_failed');
      }
      if (options.clearAuth) {
        await this._clearAuthState(existing.paths.authDir);
      }
    } else if (options.clearAuth) {
      const authDir = path.join(this.stateDir, `tenant-${key}`, 'auth');
      await this._clearAuthState(authDir);
    }
  }

  async _emitQrEvent(tenant, session) {
    if (!session.qr || !session.qr.svg) {
      return;
    }
    const payload = {
      event: 'qr',
      tenant: Number(tenant),
      channel: 'whatsapp',
      provider: 'whatsapp',
      qr_id: session.qr.id,
      svg: session.qr.svg,
    };
    await this._sendWebhookEvent(tenant, payload, session);
  }

  async _emitReadyEvent(tenant, session) {
    const payload = {
      event: 'ready',
      tenant: Number(tenant),
      channel: 'whatsapp',
      provider: 'whatsapp',
      state: 'ready',
      ts: Date.now(),
    };
    await this._sendWebhookEvent(tenant, payload, session);
  }

  async _sendWebhookEvent(tenant, payload, session) {
    if (!this.webhookClient) {
      return;
    }
    try {
      await this.webhookClient.postEvent(tenant, payload, {
        webhookUrl: session?.webhookUrl,
      });
    } catch (err) {
      this.logger.error({ tenant, event: payload?.event, err }, 'webhook_event_failed');
    }
  }
}

const normalizeUserJid = (jid) => {
  if (!jid) {
    return '';
  }
  try {
    const normalized = jidNormalizedUser(jid);
    return (normalized || '').toLowerCase();
  } catch (err) {
    return String(jid || '').trim().toLowerCase();
  }
};

const resolveSelfJid = (session) => {
  const fromSocket = normalizeUserJid(
    session?.socket?.user?.id || session?.socket?.user?.jid || session?.socket?.user?.wid
  );
  if (fromSocket) {
    return fromSocket;
  }

  const creds = session?.auth?.state?.creds;
  if (creds) {
    const candidates = [
      creds.me?.id,
      creds.me?.jid,
      creds.me?.wid,
      // Some Baileys builds expose wid on account
      creds.account?.wid,
      creds.account?.details,
    ];
    for (const candidate of candidates) {
      const normalized = normalizeUserJid(candidate);
      if (normalized) {
        return normalized;
      }
    }
  }
  return '';
};

const normalizeJid = (input) => {
  const raw = String(input || '').trim();
  if (!raw) {
    throw new HttpError(400, 'invalid_recipient', 'Recipient is required');
  }
  if (raw.endsWith('@s.whatsapp.net') || raw.endsWith('@c.us') || raw.endsWith('@g.us')) {
    return raw.toLowerCase();
  }
  const digits = raw.replace(/\D/g, '');
  if (!digits) {
    throw new HttpError(400, 'invalid_recipient', 'Recipient must contain digits');
  }
  return `${digits}@s.whatsapp.net`;
};

const resolveMedia = async (payload = {}) => {
  const url = (payload.url || payload.href || '').trim();
  const b64 = (payload.base64 || payload.b64 || payload.data || '').trim();
  if (url) {
    return { source: { url }, mime: payload.mimetype, fileName: payload.fileName || payload.filename };
  }
  if (b64) {
    const cleaned = b64.includes(',') ? b64.split(',').pop() : b64;
    return {
      source: Buffer.from(cleaned, 'base64'),
      mime: payload.mimetype,
      fileName: payload.fileName || payload.filename,
    };
  }
  throw new HttpError(400, 'invalid_media', 'Media payload must include url or base64');
};

const unwrapMessage = (message) => {
  if (!message) {
    return {};
  }
  if (message.ephemeralMessage && message.ephemeralMessage.message) {
    return unwrapMessage(message.ephemeralMessage.message);
  }
  if (message.viewOnceMessageV2 && message.viewOnceMessageV2.message) {
    return unwrapMessage(message.viewOnceMessageV2.message);
  }
  if (message.viewOnceMessage && message.viewOnceMessage.message) {
    return unwrapMessage(message.viewOnceMessage.message);
  }
  return message;
};

const extractText = (message = {}) => {
  if (!message) {
    return '';
  }
  if (message.conversation) {
    return message.conversation;
  }
  if (message.extendedTextMessage?.text) {
    return message.extendedTextMessage.text;
  }
  if (message.imageMessage?.caption) {
    return message.imageMessage.caption;
  }
  if (message.videoMessage?.caption) {
    return message.videoMessage.caption;
  }
  if (message.buttonsResponseMessage?.selectedButtonId) {
    return message.buttonsResponseMessage.selectedButtonId;
  }
  return '';
};

const buildIncomingMediaEntries = (tenant, messageId, message = {}) => {
  const entries = [];
  const push = (type, data) => {
    if (!data) {
      return;
    }
    entries.push({
      type,
      mime: data.mimetype || null,
      caption: data.caption || undefined,
      size: Number(data.fileLength || data.mediaKeyTimestamp || 0) || undefined,
      url: `baileys://${tenant}/${messageId}/${type}`,
    });
  };

  push('image', message.imageMessage);
  push('video', message.videoMessage);
  push('document', message.documentMessage);
  push('audio', message.audioMessage);
  push('sticker', message.stickerMessage);
  return entries;
};

module.exports = { SessionManager };
