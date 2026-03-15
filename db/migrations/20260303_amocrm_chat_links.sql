CREATE TABLE IF NOT EXISTS crm_chat_links (
  id BIGSERIAL PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  lead_id BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
  provider TEXT NOT NULL,
  external_chat_id TEXT,
  external_conversation_id TEXT,
  external_contact_id BIGINT,
  external_lead_id BIGINT,
  chat_scope_id TEXT,
  source_id TEXT,
  last_inbound_message_id TEXT,
  last_outbound_message_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crm_chat_links_tenant_lead_provider
  ON crm_chat_links(tenant_id, lead_id, provider);

CREATE INDEX IF NOT EXISTS idx_crm_chat_links_external_chat
  ON crm_chat_links(provider, external_chat_id);

CREATE INDEX IF NOT EXISTS idx_crm_chat_links_external_conversation
  ON crm_chat_links(provider, external_conversation_id);

CREATE INDEX IF NOT EXISTS idx_crm_chat_links_tenant_updated
  ON crm_chat_links(tenant_id, updated_at DESC);
