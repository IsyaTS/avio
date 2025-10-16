"""Add telegram_user_id column to leads and unique index."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "8f2c1c3b4a5d"
down_revision = "5f2b6a65d7f1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive guard
        raise RuntimeError("Database connection is required for this migration")

    def get_inspector() -> sa.Inspector:
        inspector = sa.inspect(bind)
        inspector.clear_cache()
        return inspector

    def table_exists(table_name: str) -> bool:
        return table_name in get_inspector().get_table_names()

    def column_exists(table_name: str, column_name: str) -> bool:
        if not table_exists(table_name):
            return False
        return any(
            column.get("name") == column_name
            for column in get_inspector().get_columns(table_name)
        )

    def index_exists(table_name: str, index_name: str) -> bool:
        if not table_exists(table_name):
            return False
        return any(
            index.get("name") == index_name
            for index in get_inspector().get_indexes(table_name)
        )

    if not table_exists("leads"):
        return

    if not column_exists("leads", "telegram_user_id"):
        op.add_column("leads", sa.Column("telegram_user_id", sa.BigInteger(), nullable=True))

    # Drop legacy indexes that rely on telegram_username or non-unique user id mapping
    for legacy_index in (
        "idx_leads_tenant_username",
        "ux_leads_tenant_telegram",
        "idx_leads_tenant_telegram_user",
    ):
        if index_exists("leads", legacy_index):
            op.drop_index(legacy_index, table_name="leads")

    if not index_exists("leads", "ux_leads_tenant_telegram_user"):
        op.create_index(
            "ux_leads_tenant_telegram_user",
            "leads",
            ["tenant_id", "telegram_user_id"],
            unique=True,
            postgresql_where=sa.text("telegram_user_id IS NOT NULL"),
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive guard
        raise RuntimeError("Database connection is required for this migration")

    def get_inspector() -> sa.Inspector:
        inspector = sa.inspect(bind)
        inspector.clear_cache()
        return inspector

    def table_exists(table_name: str) -> bool:
        return table_name in get_inspector().get_table_names()

    def column_exists(table_name: str, column_name: str) -> bool:
        if not table_exists(table_name):
            return False
        return any(
            column.get("name") == column_name
            for column in get_inspector().get_columns(table_name)
        )

    def index_exists(table_name: str, index_name: str) -> bool:
        if not table_exists(table_name):
            return False
        return any(
            index.get("name") == index_name
            for index in get_inspector().get_indexes(table_name)
        )

    if not table_exists("leads"):
        return

    if index_exists("leads", "ux_leads_tenant_telegram_user"):
        op.drop_index("ux_leads_tenant_telegram_user", table_name="leads")

    if not index_exists("leads", "idx_leads_tenant_username"):
        op.create_index(
            "idx_leads_tenant_username",
            "leads",
            ["tenant_id", "telegram_username"],
        )

    if column_exists("leads", "telegram_user_id"):
        op.drop_column("leads", "telegram_user_id")
