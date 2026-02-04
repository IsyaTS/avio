ALTER TABLE contacts
  ADD COLUMN IF NOT EXISTS max_user_id BIGINT UNIQUE;

ALTER TABLE contacts
  ADD COLUMN IF NOT EXISTS max_username TEXT;

CREATE INDEX IF NOT EXISTS idx_contacts_max_user ON contacts(max_user_id);
