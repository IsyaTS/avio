-- Learning v2: intervention-based policy learning
CREATE TABLE IF NOT EXISTS dialogue_state_snapshots (
  id                BIGSERIAL PRIMARY KEY,
  tenant_id         INTEGER NOT NULL,
  lead_id           BIGINT NOT NULL DEFAULT 0,
  contact_id        BIGINT NOT NULL DEFAULT 0,
  channel           TEXT NOT NULL DEFAULT '',
  feature_version   TEXT NOT NULL,
  fingerprint       CHAR(40) NOT NULL,
  snapshot_json     JSONB NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dialogue_state_snapshots_tenant_lead_created
  ON dialogue_state_snapshots(tenant_id, lead_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dialogue_state_snapshots_tenant_fingerprint
  ON dialogue_state_snapshots(tenant_id, fingerprint, created_at DESC);

CREATE TABLE IF NOT EXISTS intervention_episodes (
  id                     BIGSERIAL PRIMARY KEY,
  tenant_id              INTEGER NOT NULL,
  lead_id                BIGINT NOT NULL,
  channel                TEXT NOT NULL DEFAULT '',
  source_event           TEXT NOT NULL,
  trigger_user_text      TEXT,
  pre_bot_snapshot_id    BIGINT REFERENCES dialogue_state_snapshots(id) ON DELETE SET NULL,
  pre_manager_snapshot_id BIGINT REFERENCES dialogue_state_snapshots(id) ON DELETE SET NULL,
  bot_message_id         BIGINT REFERENCES messages(id) ON DELETE SET NULL,
  manager_message_id     BIGINT REFERENCES messages(id) ON DELETE SET NULL,
  bot_reply_text         TEXT,
  manager_reply_text     TEXT,
  bot_action             JSONB NOT NULL DEFAULT '{}'::jsonb,
  manager_action         JSONB NOT NULL DEFAULT '{}'::jsonb,
  stitched_dialogue      JSONB NOT NULL DEFAULT '[]'::jsonb,
  policy_key             CHAR(40),
  status                 TEXT NOT NULL DEFAULT 'captured',
  outcome_payload        JSONB NOT NULL DEFAULT '{}'::jsonb,
  reward                 DOUBLE PRECISION NOT NULL DEFAULT 0,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_intervention_episodes_tenant_lead_created
  ON intervention_episodes(tenant_id, lead_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_intervention_episodes_tenant_status
  ON intervention_episodes(tenant_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_intervention_episodes_tenant_policy_key
  ON intervention_episodes(tenant_id, policy_key, created_at DESC);

CREATE TABLE IF NOT EXISTS episode_labels (
  id             BIGSERIAL PRIMARY KEY,
  episode_id      BIGINT NOT NULL REFERENCES intervention_episodes(id) ON DELETE CASCADE,
  tenant_id       INTEGER NOT NULL,
  label_type      TEXT NOT NULL,
  label_key       TEXT NOT NULL,
  label_value     JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence      DOUBLE PRECISION NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_episode_labels_episode
  ON episode_labels(episode_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_episode_labels_tenant_type
  ON episode_labels(tenant_id, label_type, created_at DESC);

CREATE TABLE IF NOT EXISTS policy_candidates (
  id                   BIGSERIAL PRIMARY KEY,
  tenant_id            INTEGER NOT NULL,
  policy_key           CHAR(40) NOT NULL,
  fingerprint_payload  JSONB NOT NULL,
  recommended_action   TEXT NOT NULL,
  avoid_action         TEXT,
  discouraged_actions  JSONB NOT NULL DEFAULT '[]'::jsonb,
  style_hints          JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence_count       INTEGER NOT NULL DEFAULT 0,
  distinct_leads_count INTEGER NOT NULL DEFAULT 0,
  reward_delta         DOUBLE PRECISION NOT NULL DEFAULT 0,
  confidence           DOUBLE PRECISION NOT NULL DEFAULT 0,
  freshness            DOUBLE PRECISION NOT NULL DEFAULT 0,
  negative_evidence    INTEGER NOT NULL DEFAULT 0,
  active               BOOLEAN NOT NULL DEFAULT FALSE,
  last_episode_id      BIGINT REFERENCES intervention_episodes(id) ON DELETE SET NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ux_policy_candidates_tenant_key UNIQUE (tenant_id, policy_key)
);
CREATE INDEX IF NOT EXISTS idx_policy_candidates_tenant_active
  ON policy_candidates(tenant_id, active, updated_at DESC);

CREATE TABLE IF NOT EXISTS policy_candidate_evidence (
  id             BIGSERIAL PRIMARY KEY,
  candidate_id   BIGINT NOT NULL REFERENCES policy_candidates(id) ON DELETE CASCADE,
  episode_id     BIGINT NOT NULL REFERENCES intervention_episodes(id) ON DELETE CASCADE,
  tenant_id      INTEGER NOT NULL,
  lead_id        BIGINT NOT NULL,
  reward         DOUBLE PRECISION NOT NULL DEFAULT 0,
  positive       BOOLEAN NOT NULL DEFAULT FALSE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ux_policy_candidate_evidence UNIQUE (candidate_id, episode_id)
);
CREATE INDEX IF NOT EXISTS idx_policy_candidate_evidence_candidate
  ON policy_candidate_evidence(candidate_id, created_at DESC);

CREATE TABLE IF NOT EXISTS tenant_policy_rules (
  id                  BIGSERIAL PRIMARY KEY,
  tenant_id           INTEGER NOT NULL,
  candidate_id        BIGINT REFERENCES policy_candidates(id) ON DELETE SET NULL,
  rule_key            CHAR(40) NOT NULL,
  fingerprint_payload JSONB NOT NULL,
  recommended_action  TEXT NOT NULL,
  avoid_action        TEXT,
  style_hints         JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence          DOUBLE PRECISION NOT NULL DEFAULT 0,
  evidence_count      INTEGER NOT NULL DEFAULT 0,
  status              TEXT NOT NULL DEFAULT 'shadow',
  active              BOOLEAN NOT NULL DEFAULT TRUE,
  shadow_only         BOOLEAN NOT NULL DEFAULT TRUE,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  promoted_at         TIMESTAMPTZ,
  demoted_at          TIMESTAMPTZ,
  CONSTRAINT ux_tenant_policy_rules_tenant_key UNIQUE (tenant_id, rule_key)
);
CREATE INDEX IF NOT EXISTS idx_tenant_policy_rules_tenant_active
  ON tenant_policy_rules(tenant_id, active, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS policy_decisions (
  id                  BIGSERIAL PRIMARY KEY,
  tenant_id           INTEGER NOT NULL,
  lead_id             BIGINT NOT NULL,
  channel             TEXT NOT NULL DEFAULT '',
  snapshot_id         BIGINT REFERENCES dialogue_state_snapshots(id) ON DELETE SET NULL,
  rule_id             BIGINT REFERENCES tenant_policy_rules(id) ON DELETE SET NULL,
  mode                TEXT NOT NULL,
  status              TEXT NOT NULL,
  reason              TEXT NOT NULL DEFAULT '',
  similarity          DOUBLE PRECISION NOT NULL DEFAULT 0,
  confidence          DOUBLE PRECISION NOT NULL DEFAULT 0,
  recommended_action  TEXT,
  avoid_action        TEXT,
  style_hints         JSONB NOT NULL DEFAULT '{}'::jsonb,
  applied             BOOLEAN NOT NULL DEFAULT FALSE,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_policy_decisions_tenant_lead_created
  ON policy_decisions(tenant_id, lead_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_policy_decisions_tenant_status_created
  ON policy_decisions(tenant_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS policy_outcomes (
  id                 BIGSERIAL PRIMARY KEY,
  tenant_id          INTEGER NOT NULL,
  lead_id            BIGINT NOT NULL,
  episode_id         BIGINT REFERENCES intervention_episodes(id) ON DELETE CASCADE,
  decision_id        BIGINT REFERENCES policy_decisions(id) ON DELETE SET NULL,
  reward             DOUBLE PRECISION NOT NULL DEFAULT 0,
  manager_agreement  BOOLEAN,
  manager_action     TEXT,
  outcome_payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_policy_outcomes_tenant_lead_created
  ON policy_outcomes(tenant_id, lead_id, created_at DESC);
