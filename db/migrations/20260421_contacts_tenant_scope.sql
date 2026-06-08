-- Tenant-scoped contacts to prevent cross-tenant identity mixing.

ALTER TABLE contacts
  ADD COLUMN IF NOT EXISTS tenant_id INTEGER NOT NULL DEFAULT 0;

-- Drop legacy global uniqueness; we'll recreate tenant-scoped uniqueness.
ALTER TABLE contacts DROP CONSTRAINT IF EXISTS contacts_phone_key;
ALTER TABLE contacts DROP CONSTRAINT IF EXISTS contacts_whatsapp_phone_key;
ALTER TABLE contacts DROP CONSTRAINT IF EXISTS contacts_avito_user_id_key;
ALTER TABLE contacts DROP CONSTRAINT IF EXISTS contacts_max_user_id_key;

DROP INDEX IF EXISTS ux_contacts_phone;
DROP INDEX IF EXISTS ux_contacts_whatsapp_phone;
DROP INDEX IF EXISTS ux_contacts_avito_user_id;
DROP INDEX IF EXISTS ux_contacts_max_user_id;

-- Split contacts referenced by multiple tenants into tenant-local clones.
DO $$
DECLARE
    rec RECORD;
    new_contact_id BIGINT;
BEGIN
    FOR rec IN
        SELECT c.id AS contact_id,
               t.tenant_id,
               ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY t.tenant_id) AS rn
        FROM contacts c
        JOIN (
            SELECT lc.contact_id, l.tenant_id
            FROM lead_contacts lc
            JOIN leads l ON l.id = lc.lead_id
            GROUP BY lc.contact_id, l.tenant_id
        ) t ON t.contact_id = c.id
    LOOP
        IF rec.rn = 1 THEN
            UPDATE contacts
            SET tenant_id = rec.tenant_id
            WHERE id = rec.contact_id
              AND tenant_id = 0;
        ELSE
            INSERT INTO contacts(
                tenant_id,
                phone,
                whatsapp_phone,
                avito_user_id,
                avito_login,
                telegram_user_id,
                telegram_username,
                max_user_id,
                max_username,
                created_at,
                updated_at
            )
            SELECT rec.tenant_id,
                   c.phone,
                   c.whatsapp_phone,
                   c.avito_user_id,
                   c.avito_login,
                   c.telegram_user_id,
                   c.telegram_username,
                   c.max_user_id,
                   c.max_username,
                   now(),
                   now()
            FROM contacts c
            WHERE c.id = rec.contact_id
            RETURNING id INTO new_contact_id;

            UPDATE lead_contacts lc
            SET contact_id = new_contact_id,
                linked_at = now()
            FROM leads l
            WHERE lc.lead_id = l.id
              AND lc.contact_id = rec.contact_id
              AND l.tenant_id = rec.tenant_id;
        END IF;
    END LOOP;
END $$;

-- Backfill single-tenant rows that are still tenant_id=0.
UPDATE contacts c
SET tenant_id = src.tenant_id
FROM (
    SELECT lc.contact_id, MIN(l.tenant_id) AS tenant_id
    FROM lead_contacts lc
    JOIN leads l ON l.id = lc.lead_id
    GROUP BY lc.contact_id
    HAVING COUNT(DISTINCT l.tenant_id) = 1
) src
WHERE c.id = src.contact_id
  AND c.tenant_id = 0;

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
