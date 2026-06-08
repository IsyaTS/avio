CREATE TABLE IF NOT EXISTS tenant_avito_accounts (
    id BIGSERIAL PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    account_id BIGINT NOT NULL,
    account_login TEXT,
    access_token TEXT,
    refresh_token TEXT,
    expires_at BIGINT,
    obtained_at BIGINT,
    scope TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    last_webhook_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tenant_avito_accounts_status_check
        CHECK (status IN ('active', 'disconnected', 'error')),
    CONSTRAINT tenant_avito_accounts_tenant_account_key
        UNIQUE (tenant_id, account_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_avito_accounts_active_account
    ON tenant_avito_accounts(account_id)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_tenant_avito_accounts_tenant_status
    ON tenant_avito_accounts(tenant_id, status);

CREATE INDEX IF NOT EXISTS idx_tenant_avito_accounts_account_id
    ON tenant_avito_accounts(account_id);

ALTER TABLE tenant_avito_accounts
    ADD COLUMN IF NOT EXISTS display_name TEXT;
