-- события диалога
CREATE TABLE IF NOT EXISTS dialog_events(
  id BIGSERIAL PRIMARY KEY,
  lead_id BIGINT,
  event TEXT NOT NULL,
  payload JSONB DEFAULT '{}'::jsonb,
  ts TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dialog_events_lead_ts ON dialog_events(lead_id, ts);

-- фоллоу-ап состояния
CREATE TABLE IF NOT EXISTS followup_state(
  lead_id BIGINT PRIMARY KEY,
  cadence TEXT NOT NULL DEFAULT 'default',
  last_outbox_ts TIMESTAMPTZ,
  step INT NOT NULL DEFAULT 0,
  stopped BOOLEAN NOT NULL DEFAULT FALSE,
  extra JSONB DEFAULT '{}'::jsonb
);

-- A/B / бандит: решения и исходы
CREATE TABLE IF NOT EXISTS ab_decisions(
  id BIGSERIAL PRIMARY KEY,
  lead_id BIGINT,
  arm TEXT NOT NULL,
  decided_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ab_decisions_lead ON ab_decisions(lead_id);

CREATE TABLE IF NOT EXISTS ab_outcomes(
  id BIGSERIAL PRIMARY KEY,
  lead_id BIGINT,
  arm TEXT NOT NULL,
  outcome TEXT NOT NULL,
  ts TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ab_outcomes_arm_ts ON ab_outcomes(arm, ts);
