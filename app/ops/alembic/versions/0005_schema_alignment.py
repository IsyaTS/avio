"""Normalize lead identity and Telegram metadata for leads/messages."""

from __future__ import annotations

from alembic import op


revision = "0005_schema_alignment"
down_revision = "0004_leads_primary_key"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'leads' AND column_name = 'id'
            ) THEN
                ALTER TABLE leads ADD COLUMN id BIGINT;
            END IF;
        END;
        $$;
        """
    )
    op.execute("UPDATE leads SET id = lead_id WHERE id IS NULL")
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class WHERE relname = 'leads_id_seq'
            ) THEN
                CREATE SEQUENCE leads_id_seq OWNED BY leads.id;
            END IF;
        END;
        $$;
        """
    )
    op.execute(
        """
        SELECT setval(
            'leads_id_seq',
            COALESCE((SELECT MAX(id) FROM leads), 0) + 1,
            false
        )
        """
    )
    op.execute("ALTER TABLE leads ALTER COLUMN id SET DEFAULT nextval('leads_id_seq')")
    op.execute("ALTER TABLE leads ALTER COLUMN id SET NOT NULL")
    op.execute("ALTER TABLE leads DROP CONSTRAINT IF EXISTS leads_pkey")
    op.execute("ALTER TABLE leads DROP CONSTRAINT IF EXISTS leads_id_key")
    op.execute("ALTER TABLE leads ADD PRIMARY KEY (id)")

    op.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS telegram_user_id BIGINT")
    op.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS telegram_username TEXT")
    op.execute("DROP INDEX IF EXISTS ux_leads_tenant_telegram")
    op.execute("DROP INDEX IF EXISTS uniq_leads_tenant_telegram_user")
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_tenant_telegram_user
        ON leads(tenant_id, telegram_user_id)
        WHERE telegram_user_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_leads_tenant_username
        ON leads(tenant_id, telegram_username)
        """
    )
    op.execute(
        """
        ALTER TABLE leads
        ALTER COLUMN lead_id DROP NOT NULL
        """
    )

    op.execute(
        """
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS telegram_user_id BIGINT
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_messages_tenant_telegram_user
        ON messages(tenant_id, telegram_user_id)
        """
    )

    op.execute("ALTER TABLE IF EXISTS messages DROP CONSTRAINT IF EXISTS messages_lead_id_fkey")
    op.execute(
        """
        ALTER TABLE IF EXISTS messages
        ADD CONSTRAINT messages_lead_id_fkey
        FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
        """
    )
    op.execute("ALTER TABLE IF EXISTS outbox DROP CONSTRAINT IF EXISTS outbox_lead_id_fkey")
    op.execute(
        """
        ALTER TABLE IF EXISTS outbox
        ADD CONSTRAINT outbox_lead_id_fkey
        FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS contacts (
            id BIGSERIAL PRIMARY KEY,
            whatsapp_phone TEXT UNIQUE,
            avito_user_id BIGINT UNIQUE,
            avito_login TEXT,
            telegram_user_id BIGINT,
            telegram_username TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_contacts_telegram_user
        ON contacts(telegram_user_id)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_contacts_telegram_user")
    op.execute("DROP TABLE IF EXISTS contacts")
    op.execute("DROP INDEX IF EXISTS idx_messages_tenant_telegram_user")
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS telegram_user_id")
    op.execute("DROP INDEX IF EXISTS ux_leads_tenant_telegram_user")
    op.execute("DROP INDEX IF EXISTS idx_leads_tenant_username")
    op.execute("ALTER TABLE leads DROP COLUMN IF EXISTS telegram_username")
    op.execute("ALTER TABLE leads DROP COLUMN IF EXISTS telegram_user_id")
    op.execute("ALTER TABLE leads DROP CONSTRAINT IF EXISTS leads_pkey")
    op.execute("ALTER TABLE leads ADD CONSTRAINT leads_pkey PRIMARY KEY (lead_id)")
    op.execute("ALTER TABLE leads ALTER COLUMN lead_id SET NOT NULL")
    op.execute("ALTER TABLE leads ALTER COLUMN id DROP DEFAULT")
    op.execute("ALTER TABLE leads DROP COLUMN IF EXISTS id")
    op.execute("DROP SEQUENCE IF EXISTS leads_id_seq")
