-- Контакты клиента (одна сущность для всех каналов)
CREATE TABLE IF NOT EXISTS contacts (
  id              BIGSERIAL PRIMARY KEY,
  whatsapp_phone  TEXT UNIQUE,
  avito_user_id   BIGINT UNIQUE,
  avito_login     TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Связка лидов из любых каналов с одним контактом
CREATE TABLE IF NOT EXISTS lead_contacts (
  lead_id     BIGINT PRIMARY KEY,
  contact_id  BIGINT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
  linked_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Индексы на сообщения по контакту
CREATE INDEX IF NOT EXISTS idx_messages_by_contact_time ON messages(lead_id, created_at);
