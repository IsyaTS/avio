CREATE TABLE IF NOT EXISTS avito_item_contexts (
    id BIGSERIAL PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    account_id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    city TEXT,
    address TEXT,
    url TEXT,
    source TEXT NOT NULL DEFAULT 'unknown',
    status TEXT NOT NULL DEFAULT 'unknown',
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT avito_item_contexts_unique_item
        UNIQUE (tenant_id, account_id, item_id),
    CONSTRAINT avito_item_contexts_status_check
        CHECK (status IN ('resolved', 'unknown', 'error'))
);

CREATE INDEX IF NOT EXISTS idx_avito_item_contexts_tenant_account_item
    ON avito_item_contexts(tenant_id, account_id, item_id);

CREATE INDEX IF NOT EXISTS idx_avito_item_contexts_tenant_city
    ON avito_item_contexts(tenant_id, city);

CREATE TABLE IF NOT EXISTS avito_lead_item_contexts (
    id BIGSERIAL PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    lead_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT avito_lead_item_contexts_unique_lead
        UNIQUE (tenant_id, lead_id)
);

CREATE INDEX IF NOT EXISTS idx_avito_lead_item_contexts_tenant_lead
    ON avito_lead_item_contexts(tenant_id, lead_id);

CREATE INDEX IF NOT EXISTS idx_avito_lead_item_contexts_tenant_account_item
    ON avito_lead_item_contexts(tenant_id, account_id, item_id);
