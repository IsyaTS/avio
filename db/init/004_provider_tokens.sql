CREATE TABLE IF NOT EXISTS provider_tokens (
  tenant      INTEGER PRIMARY KEY,
  token       TEXT UNIQUE NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
