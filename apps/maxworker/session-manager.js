'use strict';

const fs = require('fs');
const QRCode = require('qrcode');
const {
  listStoredTenants,
  persistSessionMetadata,
  readSessionMetadata,
} = require('./persistence');

const { cfg } = require('./config');
const { MetricsRegistry } = require('./metrics');
const {
  STATUS,
  createSessionState,
  markSeen,
  pruneExpired,
  publicState,
  transition,
} = require('./state');
const {
  buildInboundTextKey,
  buildMessageKey,
  buildTextEchoKey,
  classifyMessage,
  normalizeInboundPayload,
  extractInboundCandidates,
} = require('./events');
const {
  launchBrowserSession,
  openWebApp,
  probeAuthorized,
  readAccountIdentity,
  readQrSnapshot,
  attachInboundTap,
  closeBrowserSession,
  tenantSessionDir,
} = require('./browser');
const { selectors } = require('./selectors');
const { sendText } = require('./send');

class MaxPersonalSessionManager {
  constructor() {
    this.cfg = cfg;
    this.selectors = selectors;
    this.metrics = new MetricsRegistry();
    this.sessions = new Map();
    this.watchdogTimer = null;
  }

  startWatchdog() {
    if (this.watchdogTimer) return;
    this.watchdogTimer = setInterval(() => {
      this._watchdogTick().catch(() => undefined);
    }, Math.max(1000, this.cfg.heartbeatIntervalMs));
  }

  stopWatchdog() {
    if (!this.watchdogTimer) return;
    clearInterval(this.watchdogTimer);
    this.watchdogTimer = null;
  }

  async shutdown() {
    this.stopWatchdog();
    for (const state of this.sessions.values()) {
      await this._disconnectInternal(state, 'shutdown');
    }
    this._updateGaugeMetrics();
  }

  _getOrCreate(tenant) {
    const id = Number(tenant);
    let state = this.sessions.get(id);
    if (!state) {
      state = createSessionState(id);
      state.sessionDir = tenantSessionDir(this.cfg.sessionsDir, id);
      this.sessions.set(id, state);
    }
    return state;
  }

  _tenantAllowed(tenant) {
    const allowed = Array.isArray(this.cfg.allowedTenants) ? this.cfg.allowedTenants : [];
    if (!allowed.length) return true;
    return allowed.includes(Number(tenant));
  }

  _tenantDisabledPayload(tenant) {
    this.metrics.inc('max_personal_tenant_blocked_total');
    return {
      ok: false,
      error: 'tenant_not_allowed',
      tenant: Number(tenant || 0),
    };
  }

  _touch(state) {
    state.lastHeartbeatAt = Date.now();
  }

  _touchActivity(state) {
    const now = Date.now();
    state.lastActivityAt = now;
    state.lastHeartbeatAt = now;
  }

  _browserSessionCount(excludeTenant = null) {
    let count = 0;
    const excluded = excludeTenant === null ? null : Number(excludeTenant);
    for (const state of this.sessions.values()) {
      if (excluded !== null && Number(state.tenant) === excluded) continue;
      if (state.browserRef?.page) count += 1;
    }
    return count;
  }

  _canLaunchBrowserFor(state) {
    const currentLimit = Math.max(1, Number(this.cfg.maxBrowserSessions || 1));
    return this._browserSessionCount(state?.tenant) < currentLimit;
  }

  _markLaunchLimited(state, reason) {
    const limit = Math.max(1, Number(this.cfg.maxBrowserSessions || 1));
    state.lastError = `${reason || 'browser_session_limit'}:${limit}`;
    this.metrics.inc('max_personal_browser_launch_limited_total', { reason: reason || 'limit' });
  }

  _setError(state, reason) {
    state.lastError = String(reason || 'error');
    transition(state, STATUS.ERROR, state.lastError);
    this.metrics.inc('max_personal_session_error_total', { reason: state.lastError });
  }

  async _buildQrPngDataUrl(rawValue) {
    const value = String(rawValue || '').trim();
    if (!value) return null;
    if (value.startsWith('data:image/')) return value;
    try {
      return await QRCode.toDataURL(value, {
        type: 'image/png',
        margin: 1,
        width: 320,
      });
    } catch (_err) {
      return null;
    }
  }

  _nextQr(state) {
    const now = Date.now();
    state.qrId = `${state.tenant}-${now}`;
    state.qrSvg = null;
    state.qrPngDataUrl = null;
    state.qrExpiresAt = now + 90 * 1000;
    this.metrics.inc('max_personal_qr_generation_total');
  }

  async _syncQrSnapshot(state) {
    if (this.cfg.mockMode || !state.browserRef) return;
    const snapshot = await readQrSnapshot(state.browserRef, this.selectors || {}, 1200).catch(
      () => null
    );
    if (!snapshot) return;
    if (snapshot.qrPngDataUrl) {
      state.qrPngDataUrl = snapshot.qrPngDataUrl;
      state.qrSvg = snapshot.qrSvg || null;
      return;
    }
    if (snapshot.qrText) {
      const encoded = await this._buildQrPngDataUrl(snapshot.qrText);
      if (encoded) {
        state.qrPngDataUrl = encoded;
        state.qrSvg = null;
      }
    }
  }

  async _syncAccountIdentity(state) {
    if (!state.browserRef) return;
    const account = await readAccountIdentity(state.browserRef, this.selectors || {}, 1200).catch(
      () => ({})
    );
    if (!account || typeof account !== 'object') return;
    const merged = {
      ...(state.account || {}),
      ...account,
    };
    if (Object.keys(merged).length) {
      state.account = merged;
      persistSessionMetadata(state);
    }
  }

  _hydrateStateMetadata(state) {
    const meta = readSessionMetadata(state.sessionDir);
    if (!meta || typeof meta !== 'object') return;
    if (meta.callback_url) state.callbackUrl = String(meta.callback_url).trim();
    if (meta.webhook_token) state.webhookToken = String(meta.webhook_token).trim();
    state.lastPersistedStatus = String(meta.last_status || '').trim();
    state.authProbeFailures = Number(meta.auth_probe_failures || 0);
    state.restoreGraceUntil = Number(meta.restore_grace_until || 0);
    if (meta.account && typeof meta.account === 'object') {
      state.account = {
        ...(state.account || {}),
        ...meta.account,
      };
    }
    state.lastActivityAt = Math.max(
      Number(state.lastActivityAt || 0),
      Number(meta.last_inbound_at || 0),
      Number(meta.last_outbound_at || 0),
      Number(meta.updated_at || 0)
    );
  }

  _wasPersistedAuthorized(state) {
    const persisted = String(state.lastPersistedStatus || '').trim();
    return persisted === STATUS.AUTHORIZED || persisted === STATUS.STALE;
  }

  _clearAuthUncertainty(state) {
    state.authProbeFailures = 0;
    state.restoreGraceUntil = 0;
    state.lastError = null;
  }

  _markAuthUncertain(state, reason) {
    state.authProbeFailures = Number(state.authProbeFailures || 0) + 1;
    state.lastError = String(reason || 'auth_probe_uncertain');
    if (!Number(state.restoreGraceUntil || 0)) {
      state.restoreGraceUntil =
        Date.now() + Math.max(30, Number(this.cfg.authorizedRestoreGraceSeconds || 1800)) * 1000;
    }
    transition(state, STATUS.STALE, state.lastError);
    this.metrics.inc('max_personal_auth_probe_uncertain_total', { reason: state.lastError });
  }

  _shouldRequireReauthAfterProbe(state) {
    const failures = Number(state.authProbeFailures || 0);
    const maxFailures = Math.max(1, Number(this.cfg.authProbeFailuresBeforeReauth || 3));
    const graceUntil = Number(state.restoreGraceUntil || 0);
    return failures >= maxFailures && (!graceUntil || graceUntil <= Date.now());
  }

  _setReauthRequired(state, reason) {
    transition(state, STATUS.REAUTH_REQUIRED, reason || 'reauth_required');
    state.lastError = String(reason || 'reauth_required');
    state.restoreGraceUntil = 0;
    persistSessionMetadata(state);
  }

  async _restoreTenantSession(tenant) {
    if (!this._tenantAllowed(tenant)) {
      this.metrics.inc('max_personal_restore_skipped_total', { reason: 'tenant_not_allowed' });
      return null;
    }
    const state = this._getOrCreate(tenant);
    this._hydrateStateMetadata(state);
    if (state.browserRef || !fs.existsSync(state.sessionDir)) {
      return state;
    }
    if (this.cfg.mockMode) {
      transition(state, STATUS.AUTHORIZED, 'mock_restore');
      persistSessionMetadata(state);
      return state;
    }
    if (!this._canLaunchBrowserFor(state)) {
      this._markLaunchLimited(state, 'restore_limit');
      this._markAuthUncertain(state, 'restore_browser_session_limit');
      persistSessionMetadata(state);
      return state;
    }
    try {
      state.browserRef = await launchBrowserSession({
        tenant: state.tenant,
        sessionDir: state.sessionDir,
        headless: this.cfg.browserHeadless,
        timeoutMs: this.cfg.browserTimeoutMs,
      });
      await openWebApp(state.browserRef, this.cfg.maxWebUrl, this.cfg.browserTimeoutMs);
      await this._ensureInboundTap(state);
      const authorized = await probeAuthorized(
        state.browserRef,
        this.selectors || {},
        Math.min(5000, this.cfg.browserTimeoutMs)
      ).catch(() => false);
      if (authorized) {
        transition(state, STATUS.AUTHORIZED, 'boot_restore_authorized');
        state.qrId = null;
        state.qrPngDataUrl = null;
        state.qrSvg = null;
        state.qrExpiresAt = null;
        this._clearAuthUncertainty(state);
        state.lastHeartbeatAt = Date.now();
        await this._syncAccountIdentity(state);
      } else {
        await this._syncQrSnapshot(state);
        if (this._wasPersistedAuthorized(state)) {
          this._markAuthUncertain(state, 'boot_restore_probe_uncertain');
          state.lastHeartbeatAt =
            Date.now() - Math.max(1, Number(this.cfg.sessionStaleSeconds || 75)) * 1000 - 1000;
        } else if (state.qrPngDataUrl) {
          transition(state, STATUS.WAITING_QR, 'boot_restore_waiting_qr');
          state.qrId = state.qrId || `${state.tenant}-${Date.now()}`;
          state.qrExpiresAt = Date.now() + 90 * 1000;
        } else {
          this._setReauthRequired(state, 'boot_restore_reauth_required');
        }
      }
    } catch (err) {
      this._setError(state, err && err.message ? err.message : 'restore_failed');
    }
    persistSessionMetadata(state);
    return state;
  }

  async restoreSessions() {
    const tenants = listStoredTenants(this.cfg.sessionsDir);
    for (const tenant of tenants) {
      if (!this._tenantAllowed(tenant)) {
        this.metrics.inc('max_personal_restore_skipped_total', { reason: 'tenant_not_allowed' });
        continue;
      }
      await this._restoreTenantSession(tenant);
    }
    this._updateGaugeMetrics();
  }

  async _ensureInboundTap(state) {
    if (this.cfg.mockMode || !state.browserRef?.page) return;
    if (typeof state.stopInboundTap === 'function') return;
    state.stopInboundTap = await attachInboundTap(state.browserRef, async (payload, _meta) => {
      this.metrics.inc('max_personal_tap_payload_total');
      const candidates = extractInboundCandidates(payload);
      if (candidates.length) {
        this.metrics.inc('max_personal_tap_candidate_total', {}, candidates.length);
      }
      for (const candidate of candidates) {
        try {
          await this.ingestInbound({
            tenant: state.tenant,
            chat_id: candidate.chatId,
            message_id: candidate.messageId,
            text: candidate.text,
            from_self: candidate.fromSelf,
            user_id: candidate.userId,
            username: candidate.username,
            display_name: candidate.displayName,
            ts: candidate.ts,
            type: 'message',
            tap_source: _meta && _meta.source ? String(_meta.source) : '',
            source_debug: candidate.sourceDebug || null,
          });
        } catch (_err) {
          // ignore single-event failure
        }
      }
    });
  }

  async startSession({ tenant, callbackUrl, webhookToken = '', force = false }) {
    if (!this._tenantAllowed(tenant)) {
      return this._tenantDisabledPayload(tenant);
    }
    if (this.cfg.killSwitch) {
      return { ok: false, error: 'kill_switch', status: STATUS.DISCONNECTED };
    }
    const state = this._getOrCreate(tenant);
    this._hydrateStateMetadata(state);
    if (
      !force &&
      (state.status === STATUS.AUTHORIZED ||
        state.status === STATUS.STALE ||
        this._wasPersistedAuthorized(state))
    ) {
      if (!state.browserRef && fs.existsSync(state.sessionDir)) {
        await this._restoreTenantSession(tenant);
      }
      return { ok: true, ...publicState(state), reused: true };
    }
    if (
      force ||
      state.browserRef ||
      state.status === STATUS.REAUTH_REQUIRED ||
      state.status === STATUS.ERROR ||
      state.status === STATUS.DISCONNECTED ||
      state.status === STATUS.AUTHORIZING ||
      state.status === STATUS.WAITING_QR
    ) {
      await this._disconnectInternal(state, force ? 'forced_restart' : 'start_session_restart');
    }
    state.callbackUrl = String(callbackUrl || '').trim();
    state.webhookToken = String(webhookToken || '').trim();
    state.lastError = null;
    this._touch(state);
    transition(state, STATUS.WAITING_QR, 'start_session');
    this._nextQr(state);
    if (this.cfg.mockMode) {
      state.qrPngDataUrl = await this._buildQrPngDataUrl(`MAX-MOCK:${state.qrId}`);
    }

    fs.mkdirSync(state.sessionDir, { recursive: true });
    persistSessionMetadata(state);

    if (!this.cfg.mockMode) {
      if (!this._canLaunchBrowserFor(state)) {
        this._markLaunchLimited(state, 'start_session_limit');
        transition(state, STATUS.STALE, 'browser_session_limit');
        persistSessionMetadata(state);
        this._updateGaugeMetrics();
        return { ok: false, error: 'browser_session_limit', ...publicState(state) };
      }
      try {
        state.browserRef = await launchBrowserSession({
          tenant: state.tenant,
          sessionDir: state.sessionDir,
          headless: this.cfg.browserHeadless,
          timeoutMs: this.cfg.browserTimeoutMs,
        });
        await openWebApp(state.browserRef, this.cfg.maxWebUrl, this.cfg.browserTimeoutMs);
        await this._ensureInboundTap(state);
        const authorized = await probeAuthorized(
          state.browserRef,
          this.selectors || {},
          Math.min(5000, this.cfg.browserTimeoutMs)
        ).catch(() => false);
        if (authorized) {
          transition(state, STATUS.AUTHORIZED, 'browser_authorized');
          state.qrId = null;
          state.qrPngDataUrl = null;
          state.qrSvg = null;
          state.qrExpiresAt = null;
          this._clearAuthUncertainty(state);
          this.metrics.inc('max_personal_qr_success_total');
          await this._syncAccountIdentity(state);
        } else {
          transition(state, STATUS.AUTHORIZING, 'browser_waiting_auth');
          await this._syncQrSnapshot(state);
        }
        persistSessionMetadata(state);
      } catch (err) {
        this._setError(state, err && err.message ? err.message : 'browser_launch_failed');
        persistSessionMetadata(state);
      }
    }

    if (this.cfg.mockMode && this.cfg.mockAuthorizeSeconds > 0) {
      const delay = this.cfg.mockAuthorizeSeconds * 1000;
      setTimeout(() => {
        const current = this._getOrCreate(state.tenant);
        if (current.status === STATUS.WAITING_QR || current.status === STATUS.AUTHORIZING) {
          transition(current, STATUS.AUTHORIZED, 'mock_authorized');
          current.account = {
            account_id: `mock-${current.tenant}`,
            display_name: `MAX account ${current.tenant}`,
            username: `tenant_${current.tenant}`,
          };
          current.qrId = null;
          current.qrPngDataUrl = null;
          current.qrSvg = null;
          current.qrExpiresAt = null;
          this.metrics.inc('max_personal_qr_success_total');
          persistSessionMetadata(current);
        }
      }, delay);
    }

    this._updateGaugeMetrics();
    return { ok: true, ...publicState(state) };
  }

  async getQr(tenant) {
    if (!this._tenantAllowed(tenant)) {
      return this._tenantDisabledPayload(tenant);
    }
    const state = this._getOrCreate(tenant);
    this._touch(state);
    if (!this.cfg.mockMode && (state.status === STATUS.WAITING_QR || state.status === STATUS.AUTHORIZING)) {
      await this._syncQrSnapshot(state);
    }
    if (!state.qrId || !state.qrPngDataUrl) {
      return { ok: false, error: 'qr_not_available', status: state.status };
    }
    if (state.qrExpiresAt && state.qrExpiresAt <= Date.now()) {
      if (this._wasPersistedAuthorized(state) || state.status === STATUS.STALE) {
        this._markAuthUncertain(state, 'qr_expired_after_authorized_session');
        persistSessionMetadata(state);
        return { ok: false, error: 'session_restore_pending', status: state.status };
      }
      this._setReauthRequired(state, 'qr_expired');
      return { ok: false, error: 'qr_expired', status: state.status };
    }
    return {
      ok: true,
      status: state.status,
      qr_id: state.qrId,
      qr_png_data_url: state.qrPngDataUrl,
      qr_svg: state.qrSvg,
      qr_expires_at: state.qrExpiresAt,
    };
  }

  async getStatus(tenant) {
    if (!this._tenantAllowed(tenant)) {
      return this._tenantDisabledPayload(tenant);
    }
    const state = this._getOrCreate(tenant);
    this._touch(state);
    if (!this.cfg.mockMode && state.status === STATUS.AUTHORIZED) {
      await this._syncAccountIdentity(state);
    }
    if (
      (state.status === STATUS.WAITING_QR || state.status === STATUS.AUTHORIZING) &&
      state.qrExpiresAt &&
      state.qrExpiresAt <= Date.now()
    ) {
      if (this._wasPersistedAuthorized(state)) {
        this._markAuthUncertain(state, 'qr_expired_after_authorized_session');
      } else {
        this._setReauthRequired(state, 'qr_expired');
      }
      persistSessionMetadata(state);
    }
    return { ok: true, ...publicState(state) };
  }

  async logout(tenant) {
    if (!this._tenantAllowed(tenant)) {
      return this._tenantDisabledPayload(tenant);
    }
    const state = this._getOrCreate(tenant);
    await this._disconnectInternal(state, 'logout');
    transition(state, STATUS.DISCONNECTED, 'logout');
    state.qrId = null;
    state.qrPngDataUrl = null;
    state.qrSvg = null;
    state.qrExpiresAt = null;
    persistSessionMetadata(state);
    this._updateGaugeMetrics();
    return { ok: true, ...publicState(state) };
  }

  async send(payload) {
    const tenant = Number(payload.tenant);
    if (!this._tenantAllowed(tenant)) {
      return { status: 403, body: this._tenantDisabledPayload(tenant) };
    }
    const state = this._getOrCreate(tenant);
    this._touchActivity(state);
    const dedupeKey = String(payload.idempotency_key || payload.dedupe_key || '').trim();
    if (dedupeKey) {
      const dedupe = markSeen(
        state.outboundDedup,
        `out:${tenant}:${dedupeKey}`,
        this.cfg.dedupeTtlSeconds
      );
      if (dedupe.duplicate) {
        this.metrics.inc('max_personal_outbound_duplicate_total');
        return {
          status: 200,
          body: {
            ok: true,
            duplicate: true,
            message_id: `duplicate:${dedupeKey}`,
            chat_id: String(payload.to || ''),
          },
        };
      }
    }
    const result = await sendText(state, payload, this.cfg);
    if (!result.ok) {
      this.metrics.inc('max_personal_outbound_fail_total', { reason: result.error || 'send_failed' });
      return { status: 502, body: { ok: false, ...result } };
    }

    const key = buildMessageKey(tenant, payload.to, result.message_id);
    state.lastOutboundAt = Date.now();
    state.lastChatId = String(payload.to || state.lastChatId || '').trim();
    if (key) {
      markSeen(state.sentEcho, key, this.cfg.dedupeTtlSeconds);
    }
    const textEchoKey = buildTextEchoKey(tenant, payload.to, payload.text || '');
    if (textEchoKey) {
      state.sentEchoText.set(textEchoKey, {
        sentAt: Date.now(),
        expiresAt: Date.now() + 30_000,
      });
    }
    this.metrics.inc('max_personal_outbound_success_total');
    state.lastActivityAt = Date.now();
    return { status: 200, body: { ok: true, ...result } };
  }

  _mergeAccountFromPayload(state, rawPayload, msg) {
    const accountRaw =
      (rawPayload && typeof rawPayload.account === 'object' && rawPayload.account) ||
      (rawPayload && typeof rawPayload.self === 'object' && rawPayload.self) ||
      {};
    const candidate = {
      account_id: String(
        accountRaw.account_id || accountRaw.id || accountRaw.user_id || state.account?.account_id || ''
      ).trim(),
      display_name: String(
        accountRaw.display_name ||
          accountRaw.name ||
          msg.displayName ||
          state.account?.display_name ||
          ''
      ).trim(),
      username: String(
        accountRaw.username || accountRaw.login || msg.username || state.account?.username || ''
      ).trim(),
      phone: String(accountRaw.phone || state.account?.phone || '').trim(),
    };
    const compact = Object.fromEntries(
      Object.entries(candidate).filter(([, value]) => String(value || '').trim())
    );
    if (Object.keys(compact).length) {
      state.account = {
        ...(state.account || {}),
        ...compact,
      };
      persistSessionMetadata(state);
    }
  }

  _messageLooksLikeSelfSender(state, msg) {
    const account = state.account || {};
    const userId = String(msg.userId || '').trim();
    const username = String(msg.username || '').trim().toLowerCase();
    const displayName = String(msg.displayName || '').trim().toLowerCase();
    const accountId = String(account.account_id || '').trim();
    const accountUsername = String(account.username || '').trim().toLowerCase();
    const accountDisplayName = String(account.display_name || '').trim().toLowerCase();

    if (userId && accountId && userId === accountId) return true;
    if (username && accountUsername && username === accountUsername) return true;
    if (displayName && accountDisplayName && displayName === accountDisplayName) return true;
    return false;
  }

  _promoteAuthorizedFromInbound(state, msg) {
    if (!msg || !msg.chatId) return;
    if (state.status === STATUS.AUTHORIZED) return;
    if (
      state.status !== STATUS.WAITING_QR &&
      state.status !== STATUS.AUTHORIZING &&
      state.status !== STATUS.REAUTH_REQUIRED &&
      state.status !== STATUS.STALE
    ) {
      return;
    }
      transition(state, STATUS.AUTHORIZED, 'inbound_activity_authorized');
    state.qrId = null;
    state.qrPngDataUrl = null;
    state.qrSvg = null;
    state.qrExpiresAt = null;
    this._clearAuthUncertainty(state);
    this.metrics.inc('max_personal_qr_success_total');
    persistSessionMetadata(state);
  }

  async ingestInbound(rawPayload = {}) {
    const tenant = Number(rawPayload.tenant || rawPayload.tenant_id || 0);
    if (!tenant) {
      return { status: 400, body: { ok: false, error: 'tenant_required' } };
    }
    if (!this._tenantAllowed(tenant)) {
      return { status: 403, body: this._tenantDisabledPayload(tenant) };
    }
    const state = this._getOrCreate(tenant);
    this._touchActivity(state);

    const msg = normalizeInboundPayload(rawPayload);
    msg.tenant = tenant;
    if (!msg.fromSelf && this._messageLooksLikeSelfSender(state, msg)) {
      msg.fromSelf = true;
    }
    const eventType = String(rawPayload.event || rawPayload.type || '')
      .trim()
      .toLowerCase();

    this._mergeAccountFromPayload(state, rawPayload, msg);

    if (eventType === 'session.authorized' || eventType === 'authorized') {
      transition(state, STATUS.AUTHORIZED, 'event_authorized');
      state.qrId = null;
      state.qrPngDataUrl = null;
      state.qrSvg = null;
      state.qrExpiresAt = null;
      this._clearAuthUncertainty(state);
      this.metrics.inc('max_personal_qr_success_total');
      persistSessionMetadata(state);
      return { status: 200, body: { ok: true, kind: 'session_authorized' } };
    }

    if (eventType === 'session.disconnected' || eventType === 'disconnected') {
      transition(state, STATUS.DISCONNECTED, 'event_disconnected');
      state.lastError = String(rawPayload.reason || 'disconnected');
      this.metrics.inc('max_personal_session_disconnect_total', {
        reason: state.lastError || 'disconnected',
      });
      persistSessionMetadata(state);
      return { status: 200, body: { ok: true, kind: 'session_disconnected' } };
    }

    if (eventType === 'session.qr' || eventType === 'qr') {
      transition(state, STATUS.WAITING_QR, 'event_qr_refresh');
      const qrRaw = String(
        rawPayload.qr_png_data_url ||
          rawPayload.qr_data_url ||
          rawPayload.qr ||
          rawPayload.qr_code ||
          ''
      ).trim();
      state.qrPngDataUrl = await this._buildQrPngDataUrl(qrRaw);
      state.qrSvg = String(rawPayload.qr_svg || '').trim() || null;
      state.qrId = String(rawPayload.qr_id || state.qrId || `${tenant}-${Date.now()}`).trim();
      state.qrExpiresAt = Date.now() + 90 * 1000;
      this.metrics.inc('max_personal_qr_generation_total');
      persistSessionMetadata(state);
      return { status: 200, body: { ok: true, kind: 'session_qr' } };
    }

    this._promoteAuthorizedFromInbound(state, msg);

    const dedupeKey = buildMessageKey(tenant, msg.chatId, msg.messageId);
    if (dedupeKey) {
      const dedupe = markSeen(state.seenInbound, dedupeKey, this.cfg.dedupeTtlSeconds);
      if (dedupe.duplicate) {
        this.metrics.inc('max_personal_duplicate_events_total');
        return { status: 200, body: { ok: true, duplicate: true } };
      }
    }
    const textDedupeKey = buildInboundTextKey(tenant, msg.chatId, msg.text);
    if (textDedupeKey) {
      const textDedupe = markSeen(state.seenInboundText, textDedupeKey, 8);
      if (textDedupe.duplicate) {
        this.metrics.inc('max_personal_duplicate_events_total', { kind: 'text_window' });
        return { status: 200, body: { ok: true, duplicate: true, reason: 'text_window' } };
      }
    }

    const kind = classifyMessage(
      msg,
      state.sentEcho,
      state.sentEchoText,
      this.cfg.dedupeTtlSeconds
    );
    console.log(
      JSON.stringify({
        event: 'max_personal_inbound_classified',
        tenant,
        chat_id: msg.chatId,
        message_id: msg.messageId,
        from_self: Boolean(msg.fromSelf),
        kind,
        text_preview: String(msg.text || '').slice(0, 80),
        tap_source: rawPayload.tap_source || null,
        dom_debug: rawPayload.dom_debug || null,
        source_debug: rawPayload.source_debug || null,
      })
    );
    if (kind === 'self_echo') {
      this.metrics.inc('max_personal_self_echo_suppressed_total');
      return { status: 200, body: { ok: true, suppressed: 'self_echo' } };
    }

    const webhookPayload = {
      tenant,
      channel: 'max_personal',
      message: {
        message_id: msg.messageId || `evt-${Date.now()}`,
        chat_id: msg.chatId,
        peer: msg.chatId,
        text: msg.text,
        max_user_id: msg.userId || null,
        max_username: msg.username || null,
        display_name: msg.displayName || null,
        ts: msg.ts,
      },
      manager: kind === 'manager_outgoing',
      out: kind === 'manager_outgoing',
      origin: kind === 'manager_outgoing' ? 'max_personal:manager' : 'max_personal:inbound',
    };

    const pushed = await this._pushWebhook(state, webhookPayload);
    if (!pushed) {
      this.metrics.inc('max_personal_webhook_fail_total');
      return { status: 502, body: { ok: false, error: 'webhook_push_failed' } };
    }
    this.metrics.inc('max_personal_inbound_events_total', { kind });
    if (kind === 'manager_outgoing') {
      state.lastOutboundAt = Date.now();
    } else {
      state.lastInboundAt = Date.now();
    }
    state.lastChatId = msg.chatId || state.lastChatId || '';
    state.lastActivityAt = Date.now();
    return { status: 200, body: { ok: true, kind } };
  }

  async _pushWebhook(state, payload) {
    if (!state.callbackUrl) return false;
    const headers = { 'Content-Type': 'application/json' };
    if (state.webhookToken) headers['X-Webhook-Token'] = state.webhookToken;
    if (this.cfg.authToken) headers['X-Auth-Token'] = this.cfg.authToken;
    try {
      const response = await fetch(state.callbackUrl, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
      });
      return response.status >= 200 && response.status < 300;
    } catch (_err) {
      return false;
    }
  }

  async _disconnectInternal(state, reason) {
    state.lastError = reason || null;
    state.reconnectAttempts = 0;
    state.nextReconnectAt = 0;
    if (typeof state.stopInboundTap === 'function') {
      try {
        state.stopInboundTap();
      } catch (_err) {
        // ignore
      }
    }
    state.stopInboundTap = null;
    try {
      await closeBrowserSession(state.browserRef);
    } catch (_err) {
      // ignore
    }
    state.browserRef = null;
    this.metrics.inc('max_personal_session_disconnect_total', { reason: state.lastError || 'disconnect' });
    persistSessionMetadata(state);
  }

  async _attemptReconnect(state) {
    try {
      await closeBrowserSession(state.browserRef);
    } catch (_err) {
      // ignore
    }
    state.browserRef = null;
    try {
      if (!this._canLaunchBrowserFor(state)) {
        this._markLaunchLimited(state, 'reconnect_limit');
        this._markAuthUncertain(state, 'reconnect_browser_session_limit');
        persistSessionMetadata(state);
        return false;
      }
      state.browserRef = await launchBrowserSession({
        tenant: state.tenant,
        sessionDir: state.sessionDir,
        headless: this.cfg.browserHeadless,
        timeoutMs: this.cfg.browserTimeoutMs,
      });
      await openWebApp(state.browserRef, this.cfg.maxWebUrl, this.cfg.browserTimeoutMs);
      await this._ensureInboundTap(state);
      const authorized = await probeAuthorized(
        state.browserRef,
        this.selectors || {},
        Math.min(5000, this.cfg.browserTimeoutMs)
      ).catch(() => false);
      if (authorized) {
        transition(state, STATUS.AUTHORIZED, 'reconnect_ok');
        state.lastHeartbeatAt = Date.now();
        state.reconnectAttempts = 0;
        state.nextReconnectAt = 0;
        this._clearAuthUncertainty(state);
        await this._syncAccountIdentity(state);
        persistSessionMetadata(state);
        return true;
      }
      await this._syncQrSnapshot(state);
      this._markAuthUncertain(state, 'reconnect_probe_uncertain');
      if (this._shouldRequireReauthAfterProbe(state)) {
        this._setReauthRequired(state, 'reconnect_requires_qr');
      }
      persistSessionMetadata(state);
      return false;
    } catch (err) {
      state.lastError = err && err.message ? String(err.message) : 'reconnect_failed';
      persistSessionMetadata(state);
      return false;
    }
  }

  async _watchdogTick() {
    const now = Date.now();
    for (const state of this.sessions.values()) {
      pruneExpired(state.sentEcho, now);
      pruneExpired(state.sentEchoText, now);
      pruneExpired(state.seenInbound, now);
      pruneExpired(state.seenInboundText, now);
      pruneExpired(state.outboundDedup, now);

      await this._cleanupSessionIfNeeded(state, now);

      if (
        !this.cfg.mockMode &&
        state.browserRef?.page &&
        (state.status === STATUS.WAITING_QR || state.status === STATUS.AUTHORIZING)
      ) {
        try {
          await this._ensureInboundTap(state);
          const authorized = await probeAuthorized(
            state.browserRef,
            this.selectors || {},
            Math.min(5000, this.cfg.browserTimeoutMs)
          );
          if (authorized) {
            transition(state, STATUS.AUTHORIZED, 'watchdog_authorized');
            state.qrId = null;
            state.qrPngDataUrl = null;
            state.qrSvg = null;
            state.qrExpiresAt = null;
            this._clearAuthUncertainty(state);
            state.lastHeartbeatAt = now;
            state.reconnectAttempts = 0;
            state.nextReconnectAt = 0;
            this.metrics.inc('max_personal_qr_success_total');
            await this._syncAccountIdentity(state);
            persistSessionMetadata(state);
            continue;
          }
          await this._syncQrSnapshot(state);
          state.lastHeartbeatAt = now;
          persistSessionMetadata(state);
        } catch (_err) {
          // keep current status until explicit error/reconnect path
        }
      }

      if (!this.cfg.mockMode && state.status === STATUS.AUTHORIZED && state.browserRef?.page) {
        try {
          await this._ensureInboundTap(state);
          await state.browserRef.page.title();
          state.lastHeartbeatAt = now;
          state.reconnectAttempts = 0;
        } catch (_err) {
          // continue into stale flow
        }
      }

      const stale = now - Number(state.lastHeartbeatAt || 0) > this.cfg.sessionStaleSeconds * 1000;
      if (!stale) continue;
      if (state.status === STATUS.AUTHORIZED) {
        transition(state, STATUS.STALE, 'heartbeat_timeout');
        this.metrics.inc('max_personal_stale_sessions_total');
        persistSessionMetadata(state);
      }
      if (state.status !== STATUS.STALE) continue;

      if (Number(state.nextReconnectAt || 0) > now) {
        continue;
      }

      state.reconnectAttempts = Number(state.reconnectAttempts || 0) + 1;
      this.metrics.inc('max_personal_reconnect_attempt_total');
      if (state.reconnectAttempts > this.cfg.maxReconnectAttempts) {
        transition(state, STATUS.REAUTH_REQUIRED, 'reconnect_exhausted');
        state.lastError = 'reconnect_exhausted';
        persistSessionMetadata(state);
        continue;
      }
      if (this.cfg.mockMode) {
        transition(state, STATUS.AUTHORIZED, 'mock_reconnect_ok');
        state.lastHeartbeatAt = now;
        state.reconnectAttempts = 0;
        persistSessionMetadata(state);
      } else {
        const reconnected = await this._attemptReconnect(state);
        if (!reconnected) {
          state.nextReconnectAt =
            now +
            Math.max(250, Number(this.cfg.reconnectBackoffMs || 1000)) *
              Math.max(1, state.reconnectAttempts);
        }
      }
    }
    this._updateGaugeMetrics();
  }

  async _cleanupSessionIfNeeded(state, now) {
    if (!state || this.cfg.mockMode || !state.browserRef?.page) return;

    if (
      (state.status === STATUS.WAITING_QR || state.status === STATUS.AUTHORIZING) &&
      state.qrExpiresAt &&
      now >
        Number(state.qrExpiresAt) +
          Math.max(0, Number(this.cfg.expiredQrCleanupGraceSeconds || 0)) * 1000
    ) {
      await this._disconnectInternal(state, 'expired_qr_cleanup');
      this._setReauthRequired(state, 'expired_qr_cleanup');
      this.metrics.inc('max_personal_browser_cleanup_total', { reason: 'expired_qr' });
      return;
    }

    if ([STATUS.REAUTH_REQUIRED, STATUS.DISCONNECTED, STATUS.ERROR].includes(state.status)) {
      await this._disconnectInternal(state, `cleanup_${state.status}`);
      this.metrics.inc('max_personal_browser_cleanup_total', { reason: state.status });
      return;
    }

    const ttlMs = Math.max(60, Number(this.cfg.idleBrowserTtlSeconds || 1800)) * 1000;
    const lastActivity = Math.max(
      Number(state.lastActivityAt || 0),
      Number(state.lastTransitionAt || 0)
    );
    if (lastActivity && now - lastActivity > ttlMs) {
      await this._disconnectInternal(state, 'idle_browser_ttl');
      if (state.status === STATUS.AUTHORIZED) {
        transition(state, STATUS.STALE, 'idle_browser_ttl');
      }
      this.metrics.inc('max_personal_browser_cleanup_total', { reason: 'idle_ttl' });
      persistSessionMetadata(state);
    }
  }

  _updateGaugeMetrics() {
    let active = 0;
    let waitingQr = 0;
    let stale = 0;
    let browsers = 0;
    for (const state of this.sessions.values()) {
      if (state.status === STATUS.AUTHORIZED) active += 1;
      if (state.status === STATUS.WAITING_QR || state.status === STATUS.AUTHORIZING) waitingQr += 1;
      if (state.status === STATUS.STALE) stale += 1;
      if (state.browserRef?.page) browsers += 1;
    }
    this.metrics.set('max_personal_active_sessions', {}, active);
    this.metrics.set('max_personal_waiting_qr_sessions', {}, waitingQr);
    this.metrics.set('max_personal_stale_sessions', {}, stale);
    this.metrics.set('max_personal_total_sessions', {}, this.sessions.size);
    this.metrics.set('max_personal_browser_sessions', {}, browsers);
    this.metrics.set('max_personal_browser_session_limit', {}, this.cfg.maxBrowserSessions);
    this.metrics.set('max_personal_allowed_tenants', {}, (this.cfg.allowedTenants || []).length);
    const memory = process.memoryUsage ? process.memoryUsage() : {};
    this.metrics.set('maxworker_process_rss_bytes', {}, Number(memory.rss || 0));
    this.metrics.set('maxworker_process_heap_used_bytes', {}, Number(memory.heapUsed || 0));
  }
}

module.exports = {
  MaxPersonalSessionManager,
  STATUS,
};
