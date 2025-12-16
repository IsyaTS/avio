ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS is_bot BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS message_feedback (
    id BIGSERIAL PRIMARY KEY,
    tenant_id INTEGER NOT NULL DEFAULT 0,
    message_id BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    rating TEXT NOT NULL,
    comment TEXT,
    handled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
    ALTER TABLE message_feedback
        ADD CONSTRAINT chk_message_feedback_rating CHECK (rating IN ('like', 'dislike'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_message_feedback_tenant_created
    ON message_feedback(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_message_feedback_message
    ON message_feedback(message_id);
