-- Store Avito job applications (IDs + raw payloads)

CREATE TABLE IF NOT EXISTS avito_job_application_events (
  id                BIGSERIAL PRIMARY KEY,
  avito_account_id  BIGINT NOT NULL,
  application_id    TEXT NOT NULL,
  source            TEXT,
  payload_json      JSONB,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_avito_job_application_events_account_app
  ON avito_job_application_events(avito_account_id, application_id);

CREATE INDEX IF NOT EXISTS idx_avito_job_application_events_account_created
  ON avito_job_application_events(avito_account_id, created_at DESC);
