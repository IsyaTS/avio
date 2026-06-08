'use strict';

const STATUS = Object.freeze({
  IDLE: 'idle',
  WAITING_QR: 'waiting_qr',
  AUTHORIZING: 'authorizing',
  AUTHORIZED: 'authorized',
  STALE: 'stale',
  REAUTH_REQUIRED: 'reauth_required',
  DISCONNECTED: 'disconnected',
  ERROR: 'error',
});

const ALLOWED_TRANSITIONS = Object.freeze({
  [STATUS.IDLE]: new Set([
    STATUS.WAITING_QR,
    STATUS.AUTHORIZING,
    STATUS.AUTHORIZED,
    STATUS.REAUTH_REQUIRED,
    STATUS.DISCONNECTED,
    STATUS.ERROR,
  ]),
  [STATUS.WAITING_QR]: new Set([
    STATUS.AUTHORIZING,
    STATUS.AUTHORIZED,
    STATUS.STALE,
    STATUS.REAUTH_REQUIRED,
    STATUS.DISCONNECTED,
    STATUS.ERROR,
  ]),
  [STATUS.AUTHORIZING]: new Set([
    STATUS.AUTHORIZED,
    STATUS.WAITING_QR,
    STATUS.STALE,
    STATUS.REAUTH_REQUIRED,
    STATUS.DISCONNECTED,
    STATUS.ERROR,
  ]),
  [STATUS.AUTHORIZED]: new Set([
    STATUS.STALE,
    STATUS.DISCONNECTED,
    STATUS.REAUTH_REQUIRED,
    STATUS.ERROR,
  ]),
  [STATUS.STALE]: new Set([
    STATUS.AUTHORIZED,
    STATUS.REAUTH_REQUIRED,
    STATUS.DISCONNECTED,
    STATUS.ERROR,
  ]),
  [STATUS.REAUTH_REQUIRED]: new Set([
    STATUS.WAITING_QR,
    STATUS.AUTHORIZING,
    STATUS.AUTHORIZED,
    STATUS.STALE,
    STATUS.DISCONNECTED,
    STATUS.ERROR,
  ]),
  [STATUS.DISCONNECTED]: new Set([
    STATUS.WAITING_QR,
    STATUS.AUTHORIZING,
    STATUS.AUTHORIZED,
    STATUS.IDLE,
    STATUS.ERROR,
  ]),
  [STATUS.ERROR]: new Set([
    STATUS.WAITING_QR,
    STATUS.AUTHORIZING,
    STATUS.AUTHORIZED,
    STATUS.REAUTH_REQUIRED,
    STATUS.DISCONNECTED,
    STATUS.IDLE,
  ]),
});

function nowMs() {
  return Date.now();
}

function createSessionState(tenant) {
  return {
    tenant: Number(tenant),
    status: STATUS.IDLE,
    qrId: null,
    qrPngDataUrl: null,
    qrSvg: null,
    qrExpiresAt: null,
    callbackUrl: '',
    webhookToken: '',
    lastHeartbeatAt: nowMs(),
    reconnectAttempts: 0,
    nextReconnectAt: 0,
    lastError: null,
    lastPersistedStatus: '',
    authProbeFailures: 0,
    restoreGraceUntil: 0,
    account: {},
    sessionDir: '',
    browserRef: null,
    sentEcho: new Map(),
    sentEchoText: new Map(),
    seenInbound: new Map(),
    seenInboundText: new Map(),
    outboundDedup: new Map(),
    dedupeCounter: 0,
  };
}

function canTransition(fromStatus, toStatus) {
  if (fromStatus === toStatus) return true;
  const allowed = ALLOWED_TRANSITIONS[fromStatus];
  if (!allowed) return false;
  return allowed.has(toStatus);
}

function transition(state, nextStatus, reason = '') {
  const current = state.status || STATUS.IDLE;
  if (!canTransition(current, nextStatus)) {
    return false;
  }
  state.status = nextStatus;
  state.lastTransitionReason = reason || '';
  state.lastTransitionAt = nowMs();
  return true;
}

function markSeen(mapRef, key, ttlSeconds, tsNow = nowMs()) {
  if (!key) return { duplicate: false, size: mapRef.size };
  const expiresAt = tsNow + Math.max(1, Number(ttlSeconds || 1)) * 1000;
  const current = mapRef.get(key);
  if (current && current > tsNow) {
    return { duplicate: true, size: mapRef.size };
  }
  mapRef.set(key, expiresAt);
  pruneExpired(mapRef, tsNow);
  return { duplicate: false, size: mapRef.size };
}

function pruneExpired(mapRef, tsNow = nowMs()) {
  for (const [key, rawValue] of mapRef.entries()) {
    const expiresAt =
      rawValue && typeof rawValue === 'object' ? Number(rawValue.expiresAt || 0) : Number(rawValue || 0);
    if (!expiresAt || expiresAt <= tsNow) {
      mapRef.delete(key);
    }
  }
}

function publicState(state) {
  return {
    tenant: state.tenant,
    status: state.status,
    qr_id: state.qrId,
    qr_expires_at: state.qrExpiresAt,
    account: state.account || {},
    last_heartbeat: state.lastHeartbeatAt || null,
    reconnect_attempts: state.reconnectAttempts || 0,
    last_error: state.lastError || null,
    auth_probe_failures: state.authProbeFailures || 0,
    restore_grace_until: state.restoreGraceUntil || null,
  };
}

module.exports = {
  STATUS,
  createSessionState,
  canTransition,
  transition,
  markSeen,
  pruneExpired,
  publicState,
};
