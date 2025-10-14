"""Rename lead_id to id and refresh Telegram indexes."""

from __future__ import annotations

from alembic import op


revision = "0004_leads_primary_key"
down_revision = "0003_lead_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_leads_updated_at")
    op.execute("DROP INDEX IF EXISTS ux_leads_id")
    op.execute("DROP INDEX IF EXISTS ux_leads_tenant_telegram")
    op.execute("DROP INDEX IF EXISTS ux_leads_tenant_telegram_user")
    op.execute("ALTER TABLE leads DROP CONSTRAINT IF EXISTS leads_id_key")
    op.execute("ALTER TABLE leads DROP COLUMN IF EXISTS id")
    op.execute("ALTER TABLE leads RENAME COLUMN lead_id TO id")
    op.execute("ALTER TABLE leads ALTER COLUMN id SET NOT NULL")
    op.execute(
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS telegram_user_id BIGINT"
    )
    op.execute(
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS telegram_username TEXT"
    )
    op.execute("ALTER TABLE leads ALTER COLUMN telegram_user_id DROP DEFAULT")
    op.execute("ALTER TABLE leads ALTER COLUMN telegram_user_id DROP NOT NULL")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_leads_tenant_updated_at"
        " ON leads(tenant_id, updated_at DESC)"
    )
    op.execute("DROP INDEX IF EXISTS idx_leads_tenant_username")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_leads_tenant_username"
        " ON leads(tenant_id, telegram_username)"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_tenant_telegram_user"
        " ON leads(tenant_id, telegram_user_id)"
        " WHERE telegram_user_id IS NOT NULL"
    )
    op.execute("ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_lead_id_fkey")
    op.execute(
        "ALTER TABLE messages"
        " ADD CONSTRAINT messages_lead_id_fkey"
        " FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE"
    )
    op.execute("ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_lead_id_fkey")
    op.execute(
        "ALTER TABLE outbox"
        " ADD CONSTRAINT outbox_lead_id_fkey"
        " FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_lead_id_fkey")
    op.execute(
        "ALTER TABLE outbox"
        " ADD CONSTRAINT outbox_lead_id_fkey"
        " FOREIGN KEY (lead_id) REFERENCES leads(lead_id) ON DELETE CASCADE"
    )
    op.execute("ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_lead_id_fkey")
    op.execute(
        "ALTER TABLE messages"
        " ADD CONSTRAINT messages_lead_id_fkey"
        " FOREIGN KEY (lead_id) REFERENCES leads(lead_id) ON DELETE CASCADE"
    )
    op.execute("DROP INDEX IF EXISTS ux_leads_tenant_telegram_user")
    op.execute("DROP INDEX IF EXISTS idx_leads_tenant_username")
    op.execute("DROP INDEX IF EXISTS idx_leads_tenant_updated_at")
    op.execute("ALTER TABLE leads RENAME COLUMN id TO lead_id")
    op.execute(
        "ALTER TABLE leads"
        " ADD COLUMN IF NOT EXISTS id BIGINT GENERATED ALWAYS AS (lead_id) STORED"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_id ON leads(id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_leads_updated_at ON leads(updated_at)"
    )
    op.execute("ALTER TABLE leads ALTER COLUMN telegram_user_id SET DEFAULT 0")
    op.execute("ALTER TABLE leads ALTER COLUMN telegram_user_id SET NOT NULL")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_tenant_telegram"
        " ON leads(tenant_id, telegram_user_id)"
        " WHERE telegram_user_id > 0"
    )
