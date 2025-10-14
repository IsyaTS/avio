ALTER TABLE leads DROP CONSTRAINT IF EXISTS leads_id_key;
ALTER TABLE leads DROP COLUMN IF EXISTS id;
ALTER TABLE leads RENAME COLUMN lead_id TO id;
ALTER TABLE leads ALTER COLUMN id SET NOT NULL;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS telegram_user_id BIGINT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS telegram_username TEXT;
ALTER TABLE leads ALTER COLUMN telegram_user_id DROP DEFAULT;
ALTER TABLE leads ALTER COLUMN telegram_user_id DROP NOT NULL;
DROP INDEX IF EXISTS idx_leads_updated_at;
DROP INDEX IF EXISTS ux_leads_id;
DROP INDEX IF EXISTS ux_leads_tenant_telegram;
DROP INDEX IF EXISTS ux_leads_tenant_telegram_user;
CREATE INDEX IF NOT EXISTS idx_leads_tenant_updated_at
    ON leads(tenant_id, updated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_tenant_telegram_user
    ON leads(tenant_id, telegram_user_id)
    WHERE telegram_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_tenant_username
    ON leads(tenant_id, telegram_username);
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_lead_id_fkey;
ALTER TABLE messages
    ADD CONSTRAINT messages_lead_id_fkey
        FOREIGN KEY (lead_id)
        REFERENCES leads(id)
        ON DELETE CASCADE;
ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_lead_id_fkey;
ALTER TABLE outbox
    ADD CONSTRAINT outbox_lead_id_fkey
        FOREIGN KEY (lead_id)
        REFERENCES leads(id)
        ON DELETE CASCADE;
