ALTER TABLE leads
    ADD COLUMN IF NOT EXISTS telegram_user_id BIGINT,
    ADD COLUMN IF NOT EXISTS telegram_username TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_tenant_telegram
    ON leads(tenant_id, telegram_user_id)
    WHERE telegram_user_id IS NOT NULL;

ALTER TABLE contacts
    ADD COLUMN IF NOT EXISTS telegram_user_id BIGINT,
    ADD COLUMN IF NOT EXISTS telegram_username TEXT;

CREATE INDEX IF NOT EXISTS idx_contacts_telegram_user
    ON contacts(telegram_user_id);
