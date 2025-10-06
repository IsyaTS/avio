ALTER TABLE contacts
    ADD COLUMN IF NOT EXISTS is_group BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE contacts
SET is_group = TRUE
WHERE whatsapp_phone IS NOT NULL
  AND whatsapp_phone LIKE '%@g.us';
