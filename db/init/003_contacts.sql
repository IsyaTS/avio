-- Контакты клиента (одна сущность для всех каналов)
CREATE TABLE IF NOT EXISTS contacts (
  id              BIGSERIAL PRIMARY KEY,
  tenant_id       INTEGER NOT NULL DEFAULT 0,
  phone           TEXT,
  whatsapp_phone  TEXT,
  avito_user_id   BIGINT,
  avito_login     TEXT,
  telegram_user_id BIGINT,
  telegram_username TEXT,
  max_user_id     BIGINT,
  max_username    TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contacts_tenant ON contacts(tenant_id);
CREATE INDEX IF NOT EXISTS idx_contacts_telegram_user ON contacts(telegram_user_id);
CREATE INDEX IF NOT EXISTS idx_contacts_max_user ON contacts(max_user_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_contacts_tenant_phone
  ON contacts(tenant_id, phone)
  WHERE tenant_id > 0 AND phone IS NOT NULL AND btrim(phone) <> '';
CREATE UNIQUE INDEX IF NOT EXISTS ux_contacts_tenant_whatsapp_phone
  ON contacts(tenant_id, whatsapp_phone)
  WHERE tenant_id > 0 AND whatsapp_phone IS NOT NULL AND btrim(whatsapp_phone) <> '';
CREATE UNIQUE INDEX IF NOT EXISTS ux_contacts_tenant_avito_user_id
  ON contacts(tenant_id, avito_user_id)
  WHERE tenant_id > 0 AND avito_user_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_contacts_tenant_max_user_id
  ON contacts(tenant_id, max_user_id)
  WHERE tenant_id > 0 AND max_user_id IS NOT NULL;

-- Связка лидов из любых каналов с одним контактом
CREATE TABLE IF NOT EXISTS lead_contacts (
  lead_id     BIGINT PRIMARY KEY REFERENCES leads(id) ON DELETE CASCADE,
  contact_id  BIGINT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
  channel     TEXT,
  peer        TEXT,
  linked_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Индексы на сообщения по контакту
CREATE INDEX IF NOT EXISTS idx_messages_by_contact_time ON messages(lead_id, created_at);
