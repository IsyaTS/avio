"""Ensure telegram_user_id column is non-null with explicit values."""

from __future__ import annotations

from alembic import op


revision = "0006_messages_telegram_enforce"
down_revision = "0005_schema_alignment"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS telegram_user_id BIGINT
        """
    )
    op.execute(
        """
        ALTER TABLE messages
        ALTER COLUMN telegram_user_id SET DEFAULT 0
        """
    )
    op.execute(
        """
        UPDATE messages
        SET telegram_user_id = 0
        WHERE telegram_user_id IS NULL
        """
    )
    op.execute(
        """
        ALTER TABLE messages
        ALTER COLUMN telegram_user_id SET NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_messages_tenant_telegram_user
        ON messages(tenant_id, telegram_user_id)
        """
    )
    op.execute(
        """
        ALTER TABLE messages
        ALTER COLUMN telegram_user_id DROP DEFAULT
        """
    )
    op.execute(
        """
        UPDATE outbox
        SET status = 'skipped_due_to_migration',
            last_error = 'skipped after telegram schema migration'
        WHERE status NOT IN ('queued', 'sent', 'failed', 'retry')
           OR status IS NULL
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE outbox
        SET status = 'queued',
            last_error = NULL
        WHERE status = 'skipped_due_to_migration'
        """
    )
    op.execute(
        """
        ALTER TABLE messages
        ALTER COLUMN telegram_user_id SET DEFAULT 0
        """
    )
    op.execute(
        """
        ALTER TABLE messages
        ALTER COLUMN telegram_user_id DROP NOT NULL
        """
    )
    op.execute(
        """
        DROP INDEX IF EXISTS idx_messages_tenant_telegram_user
        """
    )
