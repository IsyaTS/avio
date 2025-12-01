CREATE TABLE IF NOT EXISTS provider_tokens (
    tenant INTEGER PRIMARY KEY,
    token TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'provider_tokens'
          AND column_name = 'tenant_id'
    ) THEN
        EXECUTE 'ALTER TABLE provider_tokens RENAME COLUMN tenant_id TO tenant';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'provider_tokens'::regclass
          AND conname = 'provider_tokens_token_key'
    ) THEN
        EXECUTE 'ALTER TABLE provider_tokens ADD CONSTRAINT provider_tokens_token_key UNIQUE (token)';
    END IF;
END $$;

ALTER TABLE provider_tokens
    ALTER COLUMN created_at SET DEFAULT now();

COMMENT ON TABLE provider_tokens IS 'Provider tokens for authenticating inbound provider webhooks.';
COMMENT ON COLUMN provider_tokens.tenant IS 'Tenant identifier matching waweb sessions.';
COMMENT ON COLUMN provider_tokens.token IS 'Secret token issued to waweb for authenticating provider callbacks.';
COMMENT ON COLUMN provider_tokens.created_at IS 'Creation timestamp.';
