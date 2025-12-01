const path = require('path');

const numberFromEnv = (value, fallback) => {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const stringFromEnv = (value, fallback = '') => {
  if (value === undefined || value === null) {
    return fallback;
  }
  return String(value).trim();
};

const appBaseUrl = stringFromEnv(process.env.APP_BASE_URL, 'http://app:8000').replace(/\/$/, '');
const webhookSecret = stringFromEnv(process.env.WEBHOOK_SECRET);
const adminToken = stringFromEnv(process.env.ADMIN_TOKEN);
const waInternalToken = stringFromEnv(process.env.WA_WEB_TOKEN);
const internalToken = stringFromEnv(process.env.WA_INTERNAL_TOKEN) || waInternalToken || webhookSecret || adminToken;

const config = {
  host: process.env.HOST || '0.0.0.0',
  port: numberFromEnv(process.env.PORT, 9002),
  logLevel: (process.env.LOG_LEVEL || 'info').toLowerCase(),
  stateDir: path.resolve(
    process.env.STATE_DIR || path.join(__dirname, '..', 'data', 'wa-state')
  ),
  qrTtlMs: numberFromEnv(process.env.QR_TTL_MS, 5 * 60 * 1000),
  providerTokenRefreshMs: numberFromEnv(process.env.PROVIDER_TOKEN_REFRESH_MS, 5 * 60 * 1000),
  appBaseUrl,
  appWebhookUrl: stringFromEnv(process.env.APP_WEBHOOK_URL, `${appBaseUrl}/webhook`),
  adminToken,
  internalAuthToken: internalToken,
  webhookSecret,
};

module.exports = config;
