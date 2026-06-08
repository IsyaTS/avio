CREATE TABLE IF NOT EXISTS tenant_assets (
    id BIGSERIAL PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    asset_id TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    original_filename TEXT,
    mime TEXT,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    relative_path TEXT,
    public_url TEXT,
    checksum TEXT,
    status TEXT NOT NULL DEFAULT 'draft',
    source TEXT NOT NULL DEFAULT 'manual_upload',
    legacy_photo_id TEXT,
    ai_metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tenant_assets_key UNIQUE (tenant_id, asset_id),
    CONSTRAINT tenant_assets_status_check CHECK (status IN ('draft', 'active', 'needs_review', 'disabled', 'deleted'))
);

CREATE INDEX IF NOT EXISTS idx_tenant_assets_tenant_status
    ON tenant_assets(tenant_id, status);

CREATE INDEX IF NOT EXISTS idx_tenant_assets_tenant_type
    ON tenant_assets(tenant_id, asset_type);

CREATE TABLE IF NOT EXISTS tenant_asset_rules (
    id BIGSERIAL PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    rule_id TEXT NOT NULL,
    asset_id TEXT,
    source TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'needs_review',
    priority INTEGER NOT NULL DEFAULT 0,
    trigger JSONB NOT NULL DEFAULT '{}',
    conditions JSONB NOT NULL DEFAULT '{}',
    action JSONB NOT NULL DEFAULT '{}',
    guards JSONB NOT NULL DEFAULT '{}',
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
    needs_review BOOLEAN NOT NULL DEFAULT TRUE,
    compiler_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tenant_asset_rules_key UNIQUE (tenant_id, rule_id),
    CONSTRAINT tenant_asset_rules_status_check CHECK (status IN ('active', 'needs_review', 'disabled', 'deleted'))
);

CREATE INDEX IF NOT EXISTS idx_tenant_asset_rules_tenant_status
    ON tenant_asset_rules(tenant_id, status);

CREATE INDEX IF NOT EXISTS idx_tenant_asset_rules_tenant_asset
    ON tenant_asset_rules(tenant_id, asset_id);

CREATE TABLE IF NOT EXISTS tenant_asset_usage_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    lead_id INTEGER NOT NULL,
    channel TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    rule_id TEXT,
    event_type TEXT NOT NULL DEFAULT 'sent',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tenant_asset_usage_lead_asset
    ON tenant_asset_usage_events(tenant_id, lead_id, asset_id);

CREATE INDEX IF NOT EXISTS idx_tenant_asset_usage_created_at
    ON tenant_asset_usage_events(tenant_id, created_at);
