CREATE TABLE IF NOT EXISTS contextual_case_sets (
  id BIGSERIAL PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  set_id TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'avito',
  source_export_job_id TEXT,
  domain_schema_id TEXT,
  domain_schema JSONB NOT NULL DEFAULT '{}',
  business_rules_draft JSONB NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'imported',
  cases_count INTEGER NOT NULL DEFAULT 0,
  active_cases_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  activated_at TIMESTAMPTZ,
  UNIQUE(tenant_id, set_id)
);

CREATE TABLE IF NOT EXISTS contextual_cases (
  id BIGSERIAL PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  set_id TEXT NOT NULL,
  case_id TEXT NOT NULL,
  domain_schema_id TEXT,
  domain TEXT,
  intent TEXT,
  mode TEXT,
  context JSONB NOT NULL DEFAULT '{}',
  dialog JSONB NOT NULL DEFAULT '{}',
  reply_facts JSONB NOT NULL DEFAULT '{}',
  applicability JSONB NOT NULL DEFAULT '{}',
  quality JSONB NOT NULL DEFAULT '{}',
  search_text TEXT NOT NULL,
  fingerprint CHAR(40),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  embedding DOUBLE PRECISION[],
  embedding_model TEXT,
  embedding_status TEXT NOT NULL DEFAULT 'pending',
  embedding_error TEXT,
  times_used INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(tenant_id, case_id),
  CONSTRAINT contextual_cases_embedding_status_check
    CHECK (embedding_status IN ('pending', 'ready', 'failed')),
  CONSTRAINT contextual_cases_mode_check
    CHECK (mode IS NULL OR mode IN ('direct_example', 'context_bound', 'clarify_first', 'style_only', 'review', 'reject'))
);

CREATE INDEX IF NOT EXISTS idx_contextual_case_sets_tenant_created
  ON contextual_case_sets(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_contextual_cases_tenant_active_domain_intent
  ON contextual_cases(tenant_id, is_active, domain, intent);
CREATE INDEX IF NOT EXISTS idx_contextual_cases_tenant_set
  ON contextual_cases(tenant_id, set_id);
CREATE INDEX IF NOT EXISTS idx_contextual_cases_tenant_embedding_status
  ON contextual_cases(tenant_id, embedding_status);
CREATE INDEX IF NOT EXISTS idx_contextual_cases_tenant_fingerprint
  ON contextual_cases(tenant_id, fingerprint);
