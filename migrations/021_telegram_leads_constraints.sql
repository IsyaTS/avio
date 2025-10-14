ALTER TABLE leads
    ADD COLUMN IF NOT EXISTS id BIGINT GENERATED ALWAYS AS (lead_id) STORED;

ALTER TABLE leads
    ADD CONSTRAINT IF NOT EXISTS leads_id_key UNIQUE (id);

UPDATE leads SET telegram_user_id = 0 WHERE telegram_user_id IS NULL;

ALTER TABLE leads
    ALTER COLUMN telegram_user_id SET DEFAULT 0,
    ALTER COLUMN telegram_user_id SET NOT NULL;

DROP INDEX IF EXISTS ux_leads_tenant_telegram;

CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_tenant_telegram
    ON leads(tenant_id, telegram_user_id)
    WHERE telegram_user_id > 0;

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS telegram_user_id BIGINT NOT NULL DEFAULT 0;

ALTER TABLE messages
    DROP CONSTRAINT IF EXISTS messages_lead_id_fkey;

ALTER TABLE messages
    ADD CONSTRAINT messages_lead_id_fkey
        FOREIGN KEY (lead_id)
        REFERENCES leads(id)
        ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_messages_tenant_telegram_user
    ON messages(tenant_id, telegram_user_id);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_created_at
    ON messages(tenant_id, created_at);
