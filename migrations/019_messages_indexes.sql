-- Add indexes to speed up WhatsApp export queries

CREATE INDEX IF NOT EXISTS idx_messages_tenant_channel_created_at
    ON messages (tenant_id, channel, created_at);

CREATE INDEX IF NOT EXISTS idx_messages_lead_created_at
    ON messages (lead_id, created_at);

CREATE INDEX IF NOT EXISTS idx_leads_tenant_channel
    ON leads (tenant_id, channel);
