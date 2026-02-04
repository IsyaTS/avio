-- Лиды
CREATE TABLE IF NOT EXISTS leads (
  id                BIGINT PRIMARY KEY,
  title             TEXT,
  channel           TEXT,
  source_real_id    INTEGER,
  tenant_id         INTEGER NOT NULL DEFAULT 0,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  telegram_user_id  BIGINT,
  telegram_username TEXT,
  peer              VARCHAR(255),
  contact           TEXT
);

CREATE INDEX IF NOT EXISTS idx_leads_tenant_updated_at
  ON leads(tenant_id, updated_at DESC);
DO $$
BEGIN
  ALTER TABLE leads
    ADD CONSTRAINT ux_leads_tenant_channel_peer
    UNIQUE (tenant_id, channel, peer);
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_leads_tenant_channel_peer
  ON leads(tenant_id, channel, peer);
CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_tenant_telegram_user
  ON leads(tenant_id, telegram_user_id)
  WHERE telegram_user_id IS NOT NULL;

-- Сообщения
CREATE TABLE IF NOT EXISTS messages (
  id               BIGSERIAL PRIMARY KEY,
  lead_id          BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
  direction        SMALLINT NOT NULL, -- 0=in, 1=out
  text             TEXT NOT NULL,
  provider_msg_id  TEXT,
  status           TEXT,              -- received/sent/failed
  tenant_id        INTEGER NOT NULL DEFAULT 0,
  telegram_user_id BIGINT NOT NULL DEFAULT 0,
  is_bot           BOOLEAN NOT NULL DEFAULT FALSE,
  attachments      JSONB,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messages_lead_created ON messages(lead_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_created_at ON messages(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_telegram_user ON messages(tenant_id, telegram_user_id);

CREATE TABLE IF NOT EXISTS message_feedback (
  id          BIGSERIAL PRIMARY KEY,
  tenant_id   INTEGER NOT NULL DEFAULT 0,
  lead_id     BIGINT,
  message_id  BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  rating      TEXT NOT NULL,
  comment     TEXT,
  expected_answer TEXT,
  created_by  TEXT,
  handled     BOOLEAN NOT NULL DEFAULT FALSE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
  ALTER TABLE message_feedback
    ADD CONSTRAINT chk_message_feedback_rating CHECK (rating IN ('like', 'dislike'));
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;
DO $$
BEGIN
  ALTER TABLE message_feedback
    ADD CONSTRAINT chk_message_feedback_expected_answer CHECK (rating <> 'dislike' OR (expected_answer IS NOT NULL AND length(btrim(expected_answer)) > 0));
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_message_feedback_tenant_created
  ON message_feedback(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_message_feedback_message
  ON message_feedback(message_id);
CREATE INDEX IF NOT EXISTS idx_message_feedback_tenant_lead
  ON message_feedback(tenant_id, lead_id);

-- User-corrected examples for retrieval/future fine-tune
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

-- Explicitly mark bad bot answers (from dislikes)
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

-- Per-tenant model selection (fine-tune path, disabled by default)
CREATE TABLE IF NOT EXISTS tenant_models (
  tenant_id       INTEGER PRIMARY KEY,
  base_model      TEXT,
  finetune_model  TEXT,
  use_finetune    BOOLEAN NOT NULL DEFAULT FALSE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Outbox (для отправок и идемпотентности)
CREATE TABLE IF NOT EXISTS outbox (
  id             BIGSERIAL PRIMARY KEY,
  lead_id        BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
  text           TEXT NOT NULL,
  dedup_hash     CHAR(40) NOT NULL, -- sha1(text)
  status         TEXT NOT NULL DEFAULT 'queued', -- queued/sent/failed/retry
  attempts       INTEGER NOT NULL DEFAULT 0,
  last_error     TEXT,
  scheduled_at   TIMESTAMPTZ,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at        TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_outbox_lead_dedup ON outbox(lead_id, dedup_hash);
CREATE INDEX IF NOT EXISTS idx_outbox_status_created
  ON outbox(status, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_status_updated
  ON outbox(status, updated_at DESC);

-- Кэш источников (realId) поверх Redis
CREATE TABLE IF NOT EXISTS source_cache (
  lead_id    BIGINT PRIMARY KEY,
  real_id    INTEGER NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Сырой лог вебхуков (для разбора инцидентов)
CREATE TABLE IF NOT EXISTS webhook_events (
  id          BIGSERIAL PRIMARY KEY,
  provider    TEXT NOT NULL,          -- umnico
  event_type  TEXT NOT NULL,
  lead_id     BIGINT,
  payload     JSONB NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Простой KV для конфигов/счетчиков
CREATE TABLE IF NOT EXISTS kv (
  key        TEXT PRIMARY KEY,
  value      TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
