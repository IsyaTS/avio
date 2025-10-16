"""Rename leads.lead_id to id and refresh foreign keys."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "ad3f4c52b6e1"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive guardrail
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

    def fk_exists(table_name: str, constraint_name: str) -> bool:
        if not table_exists(table_name):
            return False
        foreign_keys = get_inspector().get_foreign_keys(table_name)
        return any(fk["name"] == constraint_name for fk in foreign_keys)

    def index_exists(table_name: str, index_name: str) -> bool:
        if not table_exists(table_name):
            return False
        indexes = get_inspector().get_indexes(table_name)
        return any(index["name"] == index_name for index in indexes)

    def drop_fk_if_exists(table_name: str, constraint_name: str) -> None:
        if fk_exists(table_name, constraint_name):
            op.drop_constraint(constraint_name, table_name=table_name, type_="foreignkey")

    def create_fk_if_missing(table_name: str, constraint_name: str) -> None:
        if not column_exists(table_name, "lead_id"):
            return
        if not fk_exists(table_name, constraint_name):
            op.create_foreign_key(
                constraint_name,
                source_table=table_name,
                referent_table="leads",
                local_cols=["lead_id"],
                remote_cols=["id"],
                ondelete="CASCADE",
            )

    for table_name, constraint_name in (
        ("messages", "messages_lead_id_fkey"),
        ("outbox", "outbox_lead_id_fkey"),
        ("lead_contacts", "lead_contacts_lead_id_fkey"),
    ):
        drop_fk_if_exists(table_name, constraint_name)

    if column_exists("leads", "lead_id") and not column_exists("leads", "id"):
        op.alter_column("leads", "lead_id", new_column_name="id")

    if column_exists("leads", "id"):
        op.alter_column(
            "leads",
            "id",
            existing_type=sa.BigInteger(),
            nullable=False,
        )

    if table_exists("leads") and table_exists("lead_contacts"):
        op.execute(
            """
            INSERT INTO leads (id, channel, created_at)
            SELECT DISTINCT lc.lead_id, 'whatsapp', NOW()
            FROM lead_contacts AS lc
            WHERE lc.lead_id IS NOT NULL
              AND lc.lead_id NOT IN (SELECT id FROM leads)
            """
        )

        if column_exists("lead_contacts", "lead_id"):
            op.execute(
                """
                DELETE FROM lead_contacts AS lc
                WHERE lc.lead_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM leads AS l WHERE l.id = lc.lead_id
                  )
                """
            )

    for table_name, constraint_name in (
        ("messages", "messages_lead_id_fkey"),
        ("outbox", "outbox_lead_id_fkey"),
        ("lead_contacts", "lead_contacts_lead_id_fkey"),
    ):
        create_fk_if_missing(table_name, constraint_name)

    if table_exists("leads") and index_exists("leads", "idx_leads_tenant_username"):
        op.drop_index("idx_leads_tenant_username", table_name="leads")

    if table_exists("leads") and not index_exists("leads", "idx_leads_tenant_updated_at"):
        op.create_index(
            "idx_leads_tenant_updated_at",
            "leads",
            ["tenant_id", "updated_at"],
            postgresql_ops={"updated_at": "DESC"},
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive guardrail
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

    def fk_exists(table_name: str, constraint_name: str) -> bool:
        if not table_exists(table_name):
            return False
        foreign_keys = get_inspector().get_foreign_keys(table_name)
        return any(fk["name"] == constraint_name for fk in foreign_keys)

    def index_exists(table_name: str, index_name: str) -> bool:
        if not table_exists(table_name):
            return False
        indexes = get_inspector().get_indexes(table_name)
        return any(index["name"] == index_name for index in indexes)

    for table_name, constraint in (
        ("messages", "messages_lead_id_fkey"),
        ("outbox", "outbox_lead_id_fkey"),
        ("lead_contacts", "lead_contacts_lead_id_fkey"),
    ):
        if fk_exists(table_name, constraint):
            op.drop_constraint(constraint, table_name=table_name, type_="foreignkey")

    if column_exists("leads", "id") and not column_exists("leads", "lead_id"):
        op.execute("ALTER TABLE leads RENAME COLUMN id TO lead_id")

    for table_name, constraint in (
        ("messages", "messages_lead_id_fkey"),
        ("outbox", "outbox_lead_id_fkey"),
        ("lead_contacts", "lead_contacts_lead_id_fkey"),
    ):
        if column_exists(table_name, "lead_id") and not fk_exists(table_name, constraint):
            op.create_foreign_key(
                constraint,
                source_table=table_name,
                referent_table="leads",
                local_cols=["lead_id"],
                remote_cols=["lead_id"],
                ondelete="CASCADE",
            )

    if table_exists("leads"):
        columns = {column["name"] for column in get_inspector().get_columns("leads")}
        if {"tenant_id", "telegram_username"}.issubset(columns):
            if not index_exists("leads", "idx_leads_tenant_username"):
                op.create_index(
                    "idx_leads_tenant_username",
                    "leads",
                    ["tenant_id", "telegram_username"],
                )
