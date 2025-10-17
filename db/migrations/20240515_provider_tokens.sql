CREATE TABLE IF NOT EXISTS provider_tokens (
    tenant INTEGER PRIMARY KEY,
    token TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE provider_tokens IS 'Provider tokens for authenticating inbound provider webhooks.';
COMMENT ON COLUMN provider_tokens.tenant IS 'Tenant identifier matching waweb sessions.';
COMMENT ON COLUMN provider_tokens.token IS 'Secret token issued to waweb for authenticating provider callbacks.';
COMMENT ON COLUMN provider_tokens.created_at IS 'Creation timestamp.';
