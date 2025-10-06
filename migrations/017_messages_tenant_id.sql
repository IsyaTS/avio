ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS tenant_id INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_messages_tenant_created
    ON messages(tenant_id, created_at DESC);
