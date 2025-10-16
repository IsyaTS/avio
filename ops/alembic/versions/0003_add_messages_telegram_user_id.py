"""Add messages.telegram_user_id column and related indexes."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "3fd5fd74a3f9"
down_revision = "2b9e3c2d41f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive
        raise RuntimeError("Database connection is required for this migration")

    def get_inspector():
        inspector = sa.inspect(bind)
        inspector.clear_cache()
        return inspector

    def table_exists(table_name: str) -> bool:
        return table_name in get_inspector().get_table_names()

    def column_exists(table_name: str, column_name: str) -> bool:
        if not table_exists(table_name):
            return False
        columns = get_inspector().get_columns(table_name)
        return any(column["name"] == column_name for column in columns)

    def index_exists(table_name: str, index_name: str) -> bool:
        if not table_exists(table_name):
            return False
        indexes = get_inspector().get_indexes(table_name)
        return any(index["name"] == index_name for index in indexes)

    def index_with_columns_exists(table_name: str, column_names: list[str]) -> bool:
        if not table_exists(table_name):
            return False
        indexes = get_inspector().get_indexes(table_name)
        for index in indexes:
            if index.get("column_names") == column_names:
                return True
        return False

    if table_exists("messages"):
        added_message_column = False
        if not column_exists("messages", "telegram_user_id"):
            op.add_column(
                "messages",
                sa.Column(
                    "telegram_user_id", sa.BigInteger(), nullable=False, server_default="0"
                ),
            )
            added_message_column = True

        if column_exists("messages", "telegram_user_id"):
            if not index_exists("messages", "idx_messages_tenant_telegram_user"):
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

    if table_exists("leads"):
        if not column_exists("leads", "telegram_user_id"):
            op.add_column(
                "leads",
                sa.Column("telegram_user_id", sa.BigInteger(), nullable=True),
            )

        if column_exists("leads", "telegram_user_id"):
            leads_index_name = "idx_leads_tenant_telegram_user"
            if not index_exists("leads", leads_index_name) and not index_with_columns_exists(
                "leads", ["tenant_id", "telegram_user_id"]
            ):
                op.create_index(
                    leads_index_name,
                    "leads",
                    ["tenant_id", "telegram_user_id"],
                )

    if table_exists("contacts"):
        if not column_exists("contacts", "telegram_user_id"):
            op.add_column(
                "contacts",
                sa.Column("telegram_user_id", sa.BigInteger(), nullable=True),
            )

        if column_exists("contacts", "telegram_user_id") and not index_exists(
            "contacts", "idx_contacts_telegram_user"
        ):
            op.create_index(
                "idx_contacts_telegram_user", "contacts", ["telegram_user_id"]
            )


def downgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive
        raise RuntimeError("Database connection is required for this migration")

    def get_inspector():
        inspector = sa.inspect(bind)
        inspector.clear_cache()
        return inspector

    def table_exists(table_name: str) -> bool:
        return table_name in get_inspector().get_table_names()

    def column_exists(table_name: str, column_name: str) -> bool:
        if not table_exists(table_name):
            return False
        columns = get_inspector().get_columns(table_name)
        return any(column["name"] == column_name for column in columns)

    def index_exists(table_name: str, index_name: str) -> bool:
        if not table_exists(table_name):
            return False
        indexes = get_inspector().get_indexes(table_name)
        return any(index["name"] == index_name for index in indexes)

    if table_exists("messages"):
        if index_exists("messages", "idx_messages_tenant_telegram_user"):
            op.drop_index("idx_messages_tenant_telegram_user", table_name="messages")

        if column_exists("messages", "telegram_user_id"):
            op.drop_column("messages", "telegram_user_id")

    if table_exists("leads"):
        if index_exists("leads", "idx_leads_tenant_telegram_user"):
            op.drop_index("idx_leads_tenant_telegram_user", table_name="leads")

    if table_exists("contacts") and column_exists("contacts", "telegram_user_id"):
        if index_exists("contacts", "idx_contacts_telegram_user"):
            op.drop_index("idx_contacts_telegram_user", table_name="contacts")

        if column_exists("contacts", "telegram_user_id"):
            op.drop_column("contacts", "telegram_user_id")
