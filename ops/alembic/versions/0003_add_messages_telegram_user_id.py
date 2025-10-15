"""Add messages.telegram_user_id column and related indexes."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0003_add_messages_telegram_user_id"
down_revision = "0002_rename_lead_id_to_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive
        raise RuntimeError("Database connection is required for this migration")

    op.add_column(
        "messages",
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=False, server_default="0"),
    )
    op.create_index(
        "idx_messages_tenant_telegram_user",
        "messages",
        ["tenant_id", "telegram_user_id"],
    )
    op.alter_column(
        "messages",
        "telegram_user_id",
        server_default=None,
    )

    dialect = bind.dialect
    has_contact_column = dialect.has_column(bind, "contacts", "telegram_user_id")
    if not has_contact_column:
        op.add_column("contacts", sa.Column("telegram_user_id", sa.BigInteger(), nullable=True))
        op.create_index("idx_contacts_telegram_user", "contacts", ["telegram_user_id"])
    else:
        op.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes
                    WHERE schemaname = current_schema()
                      AND tablename = 'contacts'
                      AND indexname = 'idx_contacts_telegram_user'
                ) THEN
                    CREATE INDEX idx_contacts_telegram_user ON contacts(telegram_user_id);
                END IF;
            END;
            $$;
            """
        )


def downgrade() -> None:
    op.drop_index("idx_messages_tenant_telegram_user", table_name="messages")
    op.drop_column("messages", "telegram_user_id")

    conn = op.get_bind()
    if conn and conn.dialect.has_column(conn, "contacts", "telegram_user_id"):
        op.drop_index("idx_contacts_telegram_user", table_name="contacts")
        op.drop_column("contacts", "telegram_user_id")
