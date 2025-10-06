-- lead states
CREATE TABLE IF NOT EXISTS lead_state(
  lead_id BIGINT PRIMARY KEY,
  state TEXT NOT NULL DEFAULT 'new',          -- new, engaged, qualified, proposal, booked, lost, won
  updated_at TIMESTAMPTZ DEFAULT now(),
  extra JSONB DEFAULT '{}'::jsonb
);
CREATE TABLE IF NOT EXISTS lead_state_history(
  id BIGSERIAL PRIMARY KEY,
  lead_id BIGINT NOT NULL,
  from_state TEXT,
  to_state TEXT NOT NULL,
  reason TEXT,
  payload JSONB DEFAULT '{}'::jsonb,
  ts TIMESTAMPTZ DEFAULT now()
);
-- tags
CREATE TABLE IF NOT EXISTS lead_tags(
  id BIGSERIAL PRIMARY KEY,
  lead_id BIGINT NOT NULL,
  tag TEXT NOT NULL,
  value TEXT,
  ts TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_lead_tags_lead ON lead_tags(lead_id);
CREATE INDEX IF NOT EXISTS idx_lead_tags_tag ON lead_tags(tag);

-- dialog events (на случай отсутствия)
CREATE TABLE IF NOT EXISTS dialog_events(
  id BIGSERIAL PRIMARY KEY,
  lead_id BIGINT,
  event TEXT NOT NULL,
  payload JSONB DEFAULT '{}'::jsonb,
  ts TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dialog_events_lead_ts ON dialog_events(lead_id, ts);

-- outbox retry columns (добавляются, если их нет)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='outbox' AND column_name='retry_count') THEN
    ALTER TABLE outbox ADD COLUMN retry_count INT NOT NULL DEFAULT 0;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='outbox' AND column_name='next_retry_at') THEN
    ALTER TABLE outbox ADD COLUMN next_retry_at TIMESTAMPTZ;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='outbox' AND column_name='last_error') THEN
    ALTER TABLE outbox ADD COLUMN last_error TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='outbox' AND column_name='status') THEN
    ALTER TABLE outbox ADD COLUMN status TEXT; -- sent, queued, failed
  END IF;
END$$;
CREATE INDEX IF NOT EXISTS idx_outbox_next_retry ON outbox(next_retry_at);
