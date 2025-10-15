"""Rename leads.lead_id to id and refresh foreign keys."""

from __future__ import annotations

from alembic import op

revision = "0002_rename_lead_id_to_id"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE leads RENAME COLUMN lead_id TO id")
    op.execute("ALTER TABLE leads ALTER COLUMN id SET NOT NULL")

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
            remote_cols=["id"],
            ondelete="CASCADE",
        )

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
