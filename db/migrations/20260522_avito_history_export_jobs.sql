CREATE TABLE IF NOT EXISTS avito_history_export_jobs (
  job_id              TEXT PRIMARY KEY,
  tenant_id           INTEGER NOT NULL,
  status              TEXT NOT NULL,
  target_dialogs      INTEGER NOT NULL DEFAULT 0,
  candidates_seen     INTEGER NOT NULL DEFAULT 0,
  dialogs_accepted    INTEGER NOT NULL DEFAULT 0,
  dialogs_rejected    INTEGER NOT NULL DEFAULT 0,
  reject_reasons      JSONB,
  file_path           TEXT,
  file_size           INTEGER NOT NULL DEFAULT 0,
  contextual_file_path TEXT,
  contextual_file_size INTEGER NOT NULL DEFAULT 0,
  contextual_cases_count INTEGER NOT NULL DEFAULT 0,
  review_cases_file_path TEXT,
  review_cases_file_size INTEGER NOT NULL DEFAULT 0,
  review_cases_count INTEGER NOT NULL DEFAULT 0,
  rejected_cases_summary_path TEXT,
  rejected_cases_summary_size INTEGER NOT NULL DEFAULT 0,
  contextual_quality_summary JSONB,
  contextual_mode     TEXT,
  ai_extracted_count  INTEGER NOT NULL DEFAULT 0,
  rule_fallback_count INTEGER NOT NULL DEFAULT 0,
  context_bound_count INTEGER NOT NULL DEFAULT 0,
  direct_example_count INTEGER NOT NULL DEFAULT 0,
  clarify_first_count INTEGER NOT NULL DEFAULT 0,
  style_only_count    INTEGER NOT NULL DEFAULT 0,
  review_count        INTEGER NOT NULL DEFAULT 0,
  reject_count        INTEGER NOT NULL DEFAULT 0,
  training_file_path  TEXT,
  training_file_size  INTEGER NOT NULL DEFAULT 0,
  training_examples_count INTEGER NOT NULL DEFAULT 0,
  review_file_path    TEXT,
  review_file_size    INTEGER NOT NULL DEFAULT 0,
  review_examples_count INTEGER NOT NULL DEFAULT 0,
  summary_file_path   TEXT,
  summary_file_size   INTEGER NOT NULL DEFAULT 0,
  rejected_examples_count INTEGER NOT NULL DEFAULT 0,
  hard_rejected_count INTEGER NOT NULL DEFAULT 0,
  ai_rejected_count   INTEGER NOT NULL DEFAULT 0,
  ai_reviewed_count   INTEGER NOT NULL DEFAULT 0,
  ai_failed_count     INTEGER NOT NULL DEFAULT 0,
  quality_summary     JSONB,
  quality_mode        TEXT,
  api_errors_summary  JSONB,
  error_code          TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at         TIMESTAMPTZ,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_avito_history_export_jobs_tenant_created
  ON avito_history_export_jobs(tenant_id, created_at DESC);

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS contextual_file_path TEXT;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS contextual_file_size INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS contextual_cases_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS review_cases_file_path TEXT;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS review_cases_file_size INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS review_cases_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS rejected_cases_summary_path TEXT;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS rejected_cases_summary_size INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS contextual_quality_summary JSONB;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS contextual_mode TEXT;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS ai_extracted_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS rule_fallback_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS context_bound_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS direct_example_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS clarify_first_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS style_only_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS review_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS reject_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS training_file_path TEXT;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS training_file_size INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS training_examples_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS review_file_path TEXT;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS review_file_size INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS review_examples_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS summary_file_path TEXT;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS summary_file_size INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS rejected_examples_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS hard_rejected_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS ai_rejected_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS ai_reviewed_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS ai_failed_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS quality_summary JSONB;

ALTER TABLE avito_history_export_jobs
  ADD COLUMN IF NOT EXISTS quality_mode TEXT;
