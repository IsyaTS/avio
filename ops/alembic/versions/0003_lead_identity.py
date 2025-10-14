"""Ensure lead id alias and telegram unique key."""

from __future__ import annotations

from alembic import op


revision = "0003_lead_identity"
down_revision = "0002_add_telegram_user_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE leads
        ADD COLUMN IF NOT EXISTS id BIGINT
        """
    )
    op.execute(
        """
        ALTER TABLE leads
        ADD COLUMN IF NOT EXISTS telegram_user_id BIGINT
        """
    )
    op.execute(
        """
        ALTER TABLE leads
        ADD COLUMN IF NOT EXISTS telegram_username TEXT
        """
    )
    op.execute(
        """
        UPDATE leads
        SET id = lead_id
        WHERE id IS NULL OR id <> lead_id
        """
    )
    op.execute("ALTER TABLE leads ALTER COLUMN id SET NOT NULL")
    op.execute(
        """
        ALTER TABLE leads
        ADD CONSTRAINT IF NOT EXISTS leads_id_key UNIQUE (id)
        """
    )
    op.execute("ALTER TABLE leads ALTER COLUMN telegram_user_id DROP DEFAULT")
    op.execute("ALTER TABLE leads ALTER COLUMN telegram_user_id DROP NOT NULL")
    op.execute(
        """
        UPDATE leads
        SET telegram_user_id = NULL
        WHERE telegram_user_id IS NOT NULL AND telegram_user_id <= 0
        """
    )
    op.execute("DROP INDEX IF EXISTS uniq_leads_tenant_telegram_user")
    op.execute("DROP INDEX IF EXISTS ux_leads_tenant_telegram")
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_tenant_telegram_user
        ON leads(tenant_id, telegram_user_id)
        WHERE telegram_user_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ux_leads_tenant_telegram_user")
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_leads_tenant_telegram_user
        ON leads(tenant_id, telegram_user_id)
        WHERE telegram_user_id > 0
        """
    )
    op.execute("ALTER TABLE leads DROP CONSTRAINT IF EXISTS leads_id_key")
    op.execute("ALTER TABLE leads DROP COLUMN IF EXISTS id")
    op.execute(
        """
        UPDATE leads
        SET telegram_user_id = 0
        WHERE telegram_user_id IS NULL
        """
    )
    op.execute("ALTER TABLE leads ALTER COLUMN telegram_user_id SET DEFAULT 0")
    op.execute("ALTER TABLE leads ALTER COLUMN telegram_user_id SET NOT NULL")
