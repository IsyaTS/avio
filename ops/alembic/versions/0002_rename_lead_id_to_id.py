"""Rename leads.lead_id to id and refresh foreign keys."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0002_rename_lead_id_to_id"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive guardrail
        raise RuntimeError("Database connection is required for this migration")

    def table_exists(table_name: str) -> bool:
        result = bind.execute(
            sa.text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_name = :table_name
                )
                """
            ),
            {"table_name": table_name},
        )
        return bool(result.scalar())

    def column_exists(table_name: str, column_name: str) -> bool:
        if not table_exists(table_name):
            return False
        result = bind.execute(
            sa.text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = :table_name
                      AND column_name = :column_name
                )
                """
            ),
            {"table_name": table_name, "column_name": column_name},
        )
        return bool(result.scalar())

    def constraint_exists(table_name: str, constraint_name: str) -> bool:
        if not table_exists(table_name):
            return False
        result = bind.execute(
            sa.text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE table_schema = current_schema()
                      AND table_name = :table_name
                      AND constraint_name = :constraint_name
                      AND constraint_type = 'FOREIGN KEY'
                )
                """
            ),
            {"table_name": table_name, "constraint_name": constraint_name},
        )
        return bool(result.scalar())

    def drop_fk_if_exists(table_name: str, constraint_name: str) -> None:
        if constraint_exists(table_name, constraint_name):
            op.drop_constraint(constraint_name, table_name=table_name, type_="foreignkey")

    def create_fk_if_missing(table_name: str, constraint_name: str) -> None:
        if not column_exists(table_name, "lead_id"):
            return
        if not constraint_exists(table_name, constraint_name):
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

    for table_name, constraint_name in (
        ("messages", "messages_lead_id_fkey"),
        ("outbox", "outbox_lead_id_fkey"),
        ("lead_contacts", "lead_contacts_lead_id_fkey"),
    ):
        create_fk_if_missing(table_name, constraint_name)

    if table_exists("leads"):
        op.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_tenant_updated_at
            ON leads(tenant_id, updated_at DESC)
            """
        )
        op.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_tenant_username
            ON leads(tenant_id, telegram_username)
            """
        )


def downgrade() -> None:
    for table_name, constraint in (
        ("messages", "messages_lead_id_fkey"),
        ("outbox", "outbox_lead_id_fkey"),
        ("lead_contacts", "lead_contacts_lead_id_fkey"),
    ):
        op.drop_constraint(constraint, table_name=table_name, type_="foreignkey")
        op.create_foreign_key(
            constraint,
            source_table=table_name,
            referent_table="leads",
            local_cols=["lead_id"],
            remote_cols=["lead_id"],
            ondelete="CASCADE",
        )

    op.execute("ALTER TABLE leads RENAME COLUMN id TO lead_id")
