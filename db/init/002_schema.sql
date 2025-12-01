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
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messages_lead_created ON messages(lead_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_created_at ON messages(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_telegram_user ON messages(tenant_id, telegram_user_id);

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
