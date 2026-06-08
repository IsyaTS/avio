'use strict';

const path = require('path');

function parseBool(value, fallback = false) {
  if (value === undefined || value === null || value === '') return !!fallback;
  if (typeof value === 'boolean') return value;
  const lowered = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(lowered)) return true;
  if (['0', 'false', 'no', 'off'].includes(lowered)) return false;
  return !!fallback;
}

function parseIntSafe(value, fallback, min) {
  const num = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(num)) return fallback;
  if (Number.isFinite(min) && num < min) return min;
  return num;
}

function parseIntList(value) {
  return String(value ?? '')
    .split(',')
    .map((item) => Number.parseInt(item.trim(), 10))
    .filter((item) => Number.isFinite(item) && item > 0);
}

const cfg = {
  host: process.env.MAXWORKER_HOST || '0.0.0.0',
  port: parseIntSafe(process.env.MAXWORKER_PORT, 9010, 1),
  authToken:
    (process.env.MAX_PERSONAL_WORKER_TOKEN || process.env.MAXWORKER_TOKEN || process.env.WEBHOOK_SECRET || '').trim(),
  killSwitch: parseBool(process.env.MAX_PERSONAL_KILL_SWITCH, false),
  sessionsDir:
    process.env.MAX_PERSONAL_SESSIONS_DIR || path.join('/data', 'max-personal-sessions'),
  sessionStaleSeconds: parseIntSafe(process.env.MAX_PERSONAL_STALE_SECONDS, 75, 10),
  maxReconnectAttempts: parseIntSafe(process.env.MAX_PERSONAL_RECONNECT_ATTEMPTS, 5, 1),
  reconnectBackoffMs: parseIntSafe(process.env.MAX_PERSONAL_RECONNECT_BACKOFF_MS, 3000, 100),
  authorizedRestoreGraceSeconds: parseIntSafe(
    process.env.MAX_PERSONAL_AUTHORIZED_RESTORE_GRACE_SECONDS,
    1800,
    30
  ),
  authProbeFailuresBeforeReauth: parseIntSafe(
    process.env.MAX_PERSONAL_AUTH_PROBE_FAILURES_BEFORE_REAUTH,
    3,
    1
  ),
  mockMode: parseBool(process.env.MAX_PERSONAL_MOCK, false),
  mockAuthorizeSeconds: parseIntSafe(process.env.MAX_PERSONAL_MOCK_AUTHORIZE_SECONDS, 7, 0),
  browserHeadless: parseBool(process.env.MAX_PERSONAL_BROWSER_HEADLESS, true),
  browserTimeoutMs: parseIntSafe(process.env.MAX_PERSONAL_BROWSER_TIMEOUT_MS, 30000, 1000),
  maxWebUrl: (process.env.MAX_PERSONAL_WEB_URL || 'https://web.max.ru/').trim(),
  outboundEnabled: !parseBool(process.env.MAX_PERSONAL_OUTBOUND_DISABLED, false),
  heartbeatIntervalMs: parseIntSafe(process.env.MAX_PERSONAL_HEARTBEAT_MS, 5000, 500),
  dedupeTtlSeconds: parseIntSafe(process.env.MAX_PERSONAL_DEDUPE_TTL_SECONDS, 900, 60),
  maxBrowserSessions: parseIntSafe(process.env.MAX_PERSONAL_MAX_BROWSER_SESSIONS, 6, 1),
  allowedTenants: parseIntList(process.env.MAX_PERSONAL_ALLOWED_TENANTS),
  idleBrowserTtlSeconds: parseIntSafe(process.env.MAX_PERSONAL_IDLE_BROWSER_TTL_SECONDS, 1800, 60),
  expiredQrCleanupGraceSeconds: parseIntSafe(
    process.env.MAX_PERSONAL_EXPIRED_QR_CLEANUP_GRACE_SECONDS,
    120,
    0
  ),
  sendTextWithAttachments: parseBool(process.env.MAX_PERSONAL_SEND_TEXT_WITH_ATTACHMENTS, true),
  fetchRemoteAttachments: parseBool(process.env.MAX_PERSONAL_FETCH_REMOTE_ATTACHMENTS, true),
  remoteAttachmentTimeoutMs: parseIntSafe(
    process.env.MAX_PERSONAL_REMOTE_ATTACHMENT_TIMEOUT_MS,
    15000,
    1000
  ),
  remoteAttachmentMaxBytes: parseIntSafe(
    process.env.MAX_PERSONAL_REMOTE_ATTACHMENT_MAX_BYTES,
    15 * 1024 * 1024,
    1024 * 1024
  ),
};

module.exports = {
  cfg,
  parseBool,
  parseIntList,
  parseIntSafe,
};
