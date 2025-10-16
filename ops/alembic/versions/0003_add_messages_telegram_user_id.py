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

    inspector = sa.inspect(bind)

    if inspector.has_table("messages"):
        message_columns = {column["name"] for column in inspector.get_columns("messages")}
        added_message_column = False
        if "telegram_user_id" not in message_columns:
            op.add_column(
                "messages",
                sa.Column(
                    "telegram_user_id", sa.BigInteger(), nullable=False, server_default="0"
                ),
            )
            added_message_column = True
            message_columns.add("telegram_user_id")

        if "telegram_user_id" in message_columns:
            message_indexes = {index["name"] for index in inspector.get_indexes("messages")}
            if "idx_messages_tenant_telegram_user" not in message_indexes:
                op.create_index(
                    "idx_messages_tenant_telegram_user",
                    "messages",
                    ["tenant_id", "telegram_user_id"],
                )

            if added_message_column:
                op.alter_column(
                    "messages",
                    "telegram_user_id",
                    server_default=None,
                )

    if inspector.has_table("contacts"):
        contact_columns = {column["name"] for column in inspector.get_columns("contacts")}
        if "telegram_user_id" not in contact_columns:
            op.add_column(
                "contacts",
                sa.Column("telegram_user_id", sa.BigInteger(), nullable=True),
            )
            contact_columns.add("telegram_user_id")

        if "telegram_user_id" in contact_columns:
            contact_indexes = {index["name"] for index in inspector.get_indexes("contacts")}
            if "idx_contacts_telegram_user" not in contact_indexes:
                op.create_index(
                    "idx_contacts_telegram_user", "contacts", ["telegram_user_id"]
                )


def downgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive
        raise RuntimeError("Database connection is required for this migration")

    inspector = sa.inspect(bind)

    if inspector.has_table("messages"):
        message_indexes = {index["name"] for index in inspector.get_indexes("messages")}
        if "idx_messages_tenant_telegram_user" in message_indexes:
            op.drop_index("idx_messages_tenant_telegram_user", table_name="messages")

        message_columns = {column["name"] for column in inspector.get_columns("messages")}
        if "telegram_user_id" in message_columns:
            op.drop_column("messages", "telegram_user_id")

    if inspector.has_table("contacts"):
        contact_columns = {column["name"] for column in inspector.get_columns("contacts")}
        if "telegram_user_id" in contact_columns:
            contact_indexes = {
                index["name"] for index in inspector.get_indexes("contacts")
            }
            if "idx_contacts_telegram_user" in contact_indexes:
                op.drop_index("idx_contacts_telegram_user", table_name="contacts")

            op.drop_column("contacts", "telegram_user_id")
