-- Лиды
CREATE TABLE IF NOT EXISTS leads (
  lead_id        BIGINT PRIMARY KEY,
  title          TEXT,
  channel        TEXT,              -- avito / whatsapp / etc
  source_real_id INTEGER,           -- кеш realId источника
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_leads_updated_at ON leads(updated_at);

-- Сообщения
CREATE TABLE IF NOT EXISTS messages (
  id             BIGSERIAL PRIMARY KEY,
  lead_id        BIGINT NOT NULL REFERENCES leads(lead_id) ON DELETE CASCADE,
  direction      SMALLINT NOT NULL, -- 0=in, 1=out
  text           TEXT NOT NULL,
  provider_msg_id TEXT,
  status         TEXT,              -- received/sent/failed
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messages_lead_created ON messages(lead_id, created_at DESC);

-- Outbox (для отправок и идемпотентности)
CREATE TABLE IF NOT EXISTS outbox (
  id             BIGSERIAL PRIMARY KEY,
  lead_id        BIGINT NOT NULL REFERENCES leads(lead_id) ON DELETE CASCADE,
  text           TEXT NOT NULL,
  dedup_hash     CHAR(40) NOT NULL, -- sha1(text)
  status         TEXT NOT NULL DEFAULT 'queued', -- queued/sent/failed/retry
  attempts       INTEGER NOT NULL DEFAULT 0,
  last_error     TEXT,
  scheduled_at   TIMESTAMPTZ,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at        TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_outbox_lead_dedup ON outbox(lead_id, dedup_hash);
CREATE INDEX IF NOT EXISTS idx_outbox_status_created ON outbox(status, created_at);

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
