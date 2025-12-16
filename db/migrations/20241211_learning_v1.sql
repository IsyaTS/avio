-- Learning v1: per-tenant feedback, training examples, fine-tune flags

DO $$
BEGIN
  -- Optional pgvector; skip gracefully if extension is absent in the image.
  CREATE EXTENSION IF NOT EXISTS vector;
EXCEPTION
  WHEN undefined_file THEN
    RAISE NOTICE 'pgvector extension is not installed, skipping';
END $$;

-- Message feedback: store lead, expected answer for dislikes, author hint
ALTER TABLE message_feedback ADD COLUMN IF NOT EXISTS lead_id BIGINT;
ALTER TABLE message_feedback ADD COLUMN IF NOT EXISTS expected_answer TEXT;
ALTER TABLE message_feedback ADD COLUMN IF NOT EXISTS created_by TEXT;

-- Backfill lead_id from messages
UPDATE message_feedback mf
SET lead_id = m.lead_id
FROM messages m
WHERE mf.message_id = m.id
  AND mf.lead_id IS NULL;

-- Backfill expected_answer for existing dislikes (prefer trimmed comment)
UPDATE message_feedback
SET expected_answer = COALESCE(
  NULLIF(btrim(expected_answer), ''),
  NULLIF(btrim(comment), ''),
  '[missing]'
)
WHERE rating = 'dislike'
  AND (expected_answer IS NULL OR btrim(expected_answer) = '');

DO $$
BEGIN
  ALTER TABLE message_feedback
    ADD CONSTRAINT chk_message_feedback_expected_answer
    CHECK (rating <> 'dislike' OR (expected_answer IS NOT NULL AND length(btrim(expected_answer)) > 0));
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_message_feedback_tenant_lead
  ON message_feedback(tenant_id, lead_id);

-- Training examples derived from feedback (per-tenant, per-lead)
CREATE TABLE IF NOT EXISTS training_examples (
  id                  BIGSERIAL PRIMARY KEY,
  tenant_id           INTEGER NOT NULL,
  lead_id             BIGINT,
  message_id          BIGINT REFERENCES messages(id) ON DELETE SET NULL,
  source              TEXT NOT NULL,
  source_feedback_id  BIGINT REFERENCES message_feedback(id) ON DELETE SET NULL,
  q_text              TEXT NOT NULL,
  a_text              TEXT NOT NULL,
  fingerprint         CHAR(40),
  is_bad              BOOLEAN NOT NULL DEFAULT FALSE,
  is_active           BOOLEAN NOT NULL DEFAULT TRUE,
  embedding           DOUBLE PRECISION[],
  embedding_model     TEXT,
  embedding_status    TEXT NOT NULL DEFAULT 'pending',
  embedding_error     TEXT,
  times_used          INTEGER NOT NULL DEFAULT 0,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
  ALTER TABLE training_examples
    ADD CONSTRAINT chk_training_examples_source CHECK (source IN ('like', 'correction', 'manual'));
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE training_examples
    ADD CONSTRAINT chk_training_examples_embedding_status CHECK (embedding_status IN ('pending', 'ready', 'failed'));
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_training_examples_tenant_active
  ON training_examples(tenant_id, is_active, is_bad);
CREATE INDEX IF NOT EXISTS idx_training_examples_tenant_status
  ON training_examples(tenant_id, embedding_status);
CREATE INDEX IF NOT EXISTS idx_training_examples_fingerprint
  ON training_examples(tenant_id, fingerprint)
  WHERE fingerprint IS NOT NULL;

-- Explicitly mark bad bot answers (exclude from retrieval)
CREATE TABLE IF NOT EXISTS bad_bot_messages (
  id           BIGSERIAL PRIMARY KEY,
  tenant_id    INTEGER NOT NULL,
  message_id   BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  feedback_id  BIGINT REFERENCES message_feedback(id) ON DELETE SET NULL,
  reason       TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_bad_bot_messages_tenant_message
  ON bad_bot_messages(tenant_id, message_id);

-- Per-tenant model selection (fine-tune disabled by default)
CREATE TABLE IF NOT EXISTS tenant_models (
  tenant_id       INTEGER PRIMARY KEY,
  base_model      TEXT,
  finetune_model  TEXT,
  use_finetune    BOOLEAN NOT NULL DEFAULT FALSE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
