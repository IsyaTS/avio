const config = require('./config');
const baseLogger = require('./logger');

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class WebhookClient {
  constructor(options = {}) {
    this.baseUrl = (options.baseUrl || config.appWebhookUrl || '').trim();
    this.internalBaseUrl = (options.appBaseUrl || config.appBaseUrl || '').trim();
    this.logger = (options.logger || baseLogger).child({ module: 'webhook-client' });
    this.refreshMs = options.refreshMs || config.providerTokenRefreshMs || 300000;
    this.internalAuthToken = options.internalAuthToken || config.internalAuthToken || '';
    this.tokenCache = new Map();
    if (!this.baseUrl) {
      throw new Error('Webhook base URL is not configured');
    }
    if (!this.internalBaseUrl) {
      throw new Error('App base URL is not configured');
    }
  }

  async postEvent(tenant, payload, { webhookUrl } = {}) {
    const tenantKey = String(tenant);
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      try {
        const token = await this._getProviderToken(tenantKey, attempt > 1);
        const target = this._composeWebhookUrl(webhookUrl || this.baseUrl, token);
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 5000);
        const response = await fetch(target, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          signal: controller.signal,
        });
        clearTimeout(timer);
        if (response.status === 401 && attempt < 3) {
          this.logger.warn({ tenant: tenantKey }, 'webhook_unauthorized_retry');
          await this._getProviderToken(tenantKey, true);
          await delay(attempt * 400);
          continue;
        }
        if (!response.ok) {
          const text = await response.text().catch(() => '');
          this.logger.warn({ tenant: tenantKey, status: response.status, body: text }, 'webhook_non_200');
          if (response.status >= 500 && attempt < 3) {
            await delay(attempt * 400);
            continue;
          }
          return false;
        }
        return true;
      } catch (err) {
        if (err.name === 'AbortError' && attempt < 3) {
          this.logger.warn({ tenant: tenantKey }, 'webhook_timeout_retry');
          await delay(attempt * 400);
          continue;
        }
        this.logger.error({ tenant: tenantKey, err }, 'webhook_request_failed');
        if (attempt >= 3) {
          throw err;
        }
        await delay(attempt * 400);
      }
    }
    return false;
  }

  async _getProviderToken(tenant, forceRefresh = false) {
    const cached = this.tokenCache.get(tenant);
    const now = Date.now();
    if (!forceRefresh && cached && now - cached.fetchedAt < this.refreshMs) {
      return cached.token;
    }
    const token = await this._requestProviderToken(tenant);
    this.tokenCache.set(tenant, { token, fetchedAt: now });
    return token;
  }

  async _requestProviderToken(tenant) {
    if (!this.internalAuthToken) {
      throw new Error('Internal auth token is not configured');
    }
    const url = `${this.internalBaseUrl}/internal/tenant/${tenant}/ensure`;
    const headers = { 'Content-Type': 'application/json', 'X-Auth-Token': this.internalAuthToken };
    const response = await fetch(url, { method: 'POST', headers });
    if (!response.ok) {
      const text = await response.text().catch(() => '');
      throw new Error(`provider_token_fetch_failed status=${response.status} body=${text}`);
    }
    const body = await response.json().catch(() => ({}));
    const token = body.provider_token;
    if (!token) {
      throw new Error('provider_token_missing');
    }
    this.logger.info({ tenant }, 'provider_token_refreshed');
    return token;
  }

  _composeWebhookUrl(base, token) {
    const target = new URL(base);
    if (token) {
      target.searchParams.set('token', token);
    }
    return target.toString();
  }
}

module.exports = { WebhookClient };
