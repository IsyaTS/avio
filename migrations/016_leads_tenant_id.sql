ALTER TABLE leads
    ADD COLUMN IF NOT EXISTS tenant_id INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_leads_tenant_updated
    ON leads(tenant_id, updated_at DESC);
