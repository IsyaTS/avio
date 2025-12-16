CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;
DO $$
BEGIN
  -- Optional: pgvector for embeddings. If not available, skip without failing init.
  CREATE EXTENSION IF NOT EXISTS vector;
EXCEPTION
  WHEN undefined_file THEN
    RAISE NOTICE 'pgvector extension is not installed, skipping';
END $$;
