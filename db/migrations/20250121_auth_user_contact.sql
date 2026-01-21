-- Auth: add contact fields to users

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS contact TEXT,
  ADD COLUMN IF NOT EXISTS preferred_messenger TEXT;
