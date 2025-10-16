"""Ensure messages defaults and tenant index ordering."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0004_messages_defaults_and_indexes"
down_revision = "3fd5fd74a3f9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "messages",
        "telegram_user_id",
        existing_type=sa.BigInteger(),
        server_default="0",
        existing_nullable=False,
    )

    op.execute(
        "DROP INDEX IF EXISTS idx_messages_tenant_created_at"
    )
    op.create_index(
        "idx_messages_tenant_created_at",
        "messages",
        ["tenant_id", "created_at"],
        postgresql_ops={"created_at": "DESC"},
    )


def downgrade() -> None:
    op.drop_index("idx_messages_tenant_created_at", table_name="messages")
    op.create_index(
        "idx_messages_tenant_created_at",
        "messages",
        ["tenant_id", "created_at"],
    )
    op.alter_column(
        "messages",
        "telegram_user_id",
        existing_type=sa.BigInteger(),
        server_default=None,
        existing_nullable=False,
    )
