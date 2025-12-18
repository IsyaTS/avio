-- Avito Analytics OAuth tokens (admin-only)

CREATE TABLE IF NOT EXISTS avito_analytics_tokens (
  account_id         BIGINT PRIMARY KEY,
  display_name       TEXT,
  scopes             TEXT,
  token_type         TEXT,
  access_token_enc   TEXT,
  refresh_token_enc  TEXT,
  expires_at         TIMESTAMPTZ,
  obtained_at        TIMESTAMPTZ,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error         TEXT,
  raw_payload        JSONB
);

CREATE INDEX IF NOT EXISTS idx_avito_analytics_tokens_updated
  ON avito_analytics_tokens(updated_at DESC);
