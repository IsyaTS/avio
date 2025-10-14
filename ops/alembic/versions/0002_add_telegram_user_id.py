"""Add telegram_user_id column and index."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0002_add_telegram_user_id"
down_revision = "0001_base_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "leads",
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=False, server_default="0"),
    )
    op.execute("UPDATE leads SET telegram_user_id = 0 WHERE telegram_user_id IS NULL")
    op.alter_column(
        "leads",
        "telegram_user_id",
        existing_type=sa.BigInteger(),
        nullable=False,
        server_default="0",
    )
    op.create_index(
        "uniq_leads_tenant_telegram_user",
        "leads",
        ["tenant_id", "telegram_user_id"],
        unique=True,
        postgresql_where=sa.text("telegram_user_id > 0"),
    )


def downgrade() -> None:
    op.drop_index("uniq_leads_tenant_telegram_user", table_name="leads")
    op.drop_column("leads", "telegram_user_id")
