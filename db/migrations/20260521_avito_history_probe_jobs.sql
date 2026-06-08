CREATE TABLE IF NOT EXISTS avito_history_probe_jobs (
  job_id              TEXT PRIMARY KEY,
  tenant_id           INTEGER NOT NULL,
  status              TEXT NOT NULL,
  period_from         TIMESTAMPTZ NOT NULL,
  period_to           TIMESTAMPTZ NOT NULL,
  chat_limit          INTEGER NOT NULL DEFAULT 0,
  chats_seen          INTEGER NOT NULL DEFAULT 0,
  chats_with_messages INTEGER NOT NULL DEFAULT 0,
  messages_seen       INTEGER NOT NULL DEFAULT 0,
  messages_in_period  INTEGER NOT NULL DEFAULT 0,
  oldest_message_at   TIMESTAMPTZ,
  newest_message_at   TIMESTAMPTZ,
  api_errors_summary  JSONB,
  error_code          TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at         TIMESTAMPTZ,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_avito_history_probe_jobs_tenant_created
  ON avito_history_probe_jobs(tenant_id, created_at DESC);
