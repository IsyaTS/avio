-- Provider tokens for waweb authentication
CREATE TABLE IF NOT EXISTS provider_tokens (
  tenant_id   INTEGER PRIMARY KEY,
  token       TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
