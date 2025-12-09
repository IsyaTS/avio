-- Align contacts schema with application expectations
ALTER TABLE IF EXISTS contacts
  ADD COLUMN IF NOT EXISTS phone TEXT;

-- Keep phone unique when provided, but allow multiple NULLs
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname = 'ux_contacts_phone'
  ) THEN
    CREATE UNIQUE INDEX ux_contacts_phone ON contacts(phone) WHERE phone IS NOT NULL;
  END IF;
EXCEPTION
  WHEN duplicate_table THEN NULL;
END $$;
