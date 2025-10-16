"""Add updated_at column and index for outbox."""

from __future__ import annotations

from alembic import op


revision = "9e4d1c2b3a6f"
down_revision = "8f2c1c3b4a5d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE outbox
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
        """
    )
    op.execute(
        """
        UPDATE outbox
        SET updated_at = created_at
        WHERE updated_at IS NULL OR updated_at < created_at;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_outbox_status_updated
        ON outbox(status, updated_at DESC);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_outbox_status_updated;")
    op.execute("ALTER TABLE outbox DROP COLUMN IF EXISTS updated_at;")
