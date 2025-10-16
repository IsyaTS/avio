"""Ensure messages defaults and tenant index ordering."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "2b9e3c2d41f0"
down_revision = "3fd5fd74a3f9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive guardrail
        raise RuntimeError("Database connection is required for this migration")

    inspector = sa.inspect(bind)
    inspector.clear_cache()

    table_names = inspector.get_table_names()
    if "messages" not in table_names:
        raise RuntimeError("messages table must exist for this migration")

    columns = {column["name"] for column in inspector.get_columns("messages")}
    if "telegram_user_id" not in columns:
        raise RuntimeError("messages.telegram_user_id column is required")

    op.alter_column(
        "messages",
        "telegram_user_id",
        existing_type=sa.BigInteger(),
        server_default="0",
        existing_nullable=False,
    )

    index_name = "idx_messages_tenant_created_at"
    indexes = {index["name"] for index in inspector.get_indexes("messages")}
    if index_name in indexes:
        op.drop_index(index_name, table_name="messages")
        inspector = sa.inspect(bind)
        inspector.clear_cache()

    required_index_columns = {"tenant_id", "created_at"}
    columns = {column["name"] for column in inspector.get_columns("messages")}
    if not required_index_columns.issubset(columns):
        raise RuntimeError(
            "messages table is missing columns required for tenant+created_at index"
        )

    indexes = {index["name"] for index in inspector.get_indexes("messages")}
    if index_name not in indexes:
        op.create_index(
            index_name,
            "messages",
            ["tenant_id", "created_at"],
            postgresql_ops={"created_at": "DESC"},
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive guardrail
        raise RuntimeError("Database connection is required for this migration")

    inspector = sa.inspect(bind)
    inspector.clear_cache()

    table_names = inspector.get_table_names()
    if "messages" not in table_names:
        raise RuntimeError("messages table must exist for this migration")

    index_name = "idx_messages_tenant_created_at"
    indexes = {index["name"] for index in inspector.get_indexes("messages")}
    if index_name in indexes:
        op.drop_index(index_name, table_name="messages")
        inspector = sa.inspect(bind)
        inspector.clear_cache()

    columns = {column["name"] for column in inspector.get_columns("messages")}
    if {"tenant_id", "created_at"}.issubset(columns):
        indexes = {index["name"] for index in inspector.get_indexes("messages")}
        if index_name not in indexes:
            op.create_index(
                index_name,
                "messages",
                ["tenant_id", "created_at"],
            )

    columns = {column["name"] for column in inspector.get_columns("messages")}
    if "telegram_user_id" not in columns:
        raise RuntimeError("messages.telegram_user_id column is required")

    op.alter_column(
        "messages",
        "telegram_user_id",
        existing_type=sa.BigInteger(),
        server_default=None,
        existing_nullable=False,
    )
