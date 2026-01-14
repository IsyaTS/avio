CREATE TABLE IF NOT EXISTS amocrm_tokens (
  tenant_id        INTEGER PRIMARY KEY,
  access_token_enc TEXT,
  refresh_token_enc TEXT,
  expires_at       TIMESTAMPTZ,
  obtained_at      TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error       TEXT,
  raw_payload      JSONB
);

CREATE INDEX IF NOT EXISTS idx_amocrm_tokens_updated
  ON amocrm_tokens(updated_at DESC);

CREATE TABLE IF NOT EXISTS crm_links (
  id               BIGSERIAL PRIMARY KEY,
  tenant_id        INTEGER NOT NULL,
  lead_id          BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
  provider         TEXT NOT NULL,
  provider_lead_id BIGINT,
  pipeline_id      BIGINT,
  stage_index      INTEGER NOT NULL DEFAULT 0,
  inbound_count    INTEGER NOT NULL DEFAULT 0,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crm_links_tenant_lead_provider
  ON crm_links(tenant_id, lead_id, provider);
CREATE INDEX IF NOT EXISTS idx_crm_links_provider_lead
  ON crm_links(provider, provider_lead_id);
CREATE INDEX IF NOT EXISTS idx_crm_links_tenant_updated
  ON crm_links(tenant_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS crm_extracted_fields (
  id          BIGSERIAL PRIMARY KEY,
  tenant_id   INTEGER NOT NULL,
  lead_id     BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
  provider    TEXT NOT NULL,
  field_key   TEXT NOT NULL,
  field_value TEXT NOT NULL,
  amo_field_id BIGINT,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crm_extracted_fields
  ON crm_extracted_fields(tenant_id, lead_id, provider, field_key);
CREATE INDEX IF NOT EXISTS idx_crm_extracted_fields_tenant
  ON crm_extracted_fields(tenant_id, provider);

CREATE TABLE IF NOT EXISTS crm_outbox (
  id           BIGSERIAL PRIMARY KEY,
  tenant_id    INTEGER NOT NULL,
  provider     TEXT NOT NULL,
  lead_id      BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
  event_type   TEXT NOT NULL,
  payload      JSONB NOT NULL,
  attempts     INTEGER NOT NULL DEFAULT 0,
  next_retry_at TIMESTAMPTZ,
  last_error   TEXT,
  status       TEXT NOT NULL DEFAULT 'pending',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crm_outbox_status_retry
  ON crm_outbox(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_crm_outbox_tenant_created
  ON crm_outbox(tenant_id, created_at DESC);
