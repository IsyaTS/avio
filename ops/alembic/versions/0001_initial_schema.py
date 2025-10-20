"""Initial schema with legacy lead_id primary key."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "leads",
        sa.Column("lead_id", sa.BigInteger(), primary_key=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("channel", sa.Text(), nullable=True),
        sa.Column("source_real_id", sa.Integer(), nullable=True),
        sa.Column("tenant_id", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=True),
        sa.Column("telegram_username", sa.Text(), nullable=True),
        sa.Column("peer", sa.Text(), nullable=True),
        sa.Column("contact", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_leads_tenant_updated_at",
        "leads",
        ["tenant_id", "updated_at"],
        postgresql_ops={"updated_at": "DESC"},
    )
    op.create_index(
        "ux_leads_tenant_telegram",
        "leads",
        ["tenant_id", "telegram_user_id"],
        unique=True,
        postgresql_where=sa.text("telegram_user_id IS NOT NULL"),
    )
    op.create_index(
        "idx_leads_tenant_username",
        "leads",
        ["tenant_id", "telegram_username"],
    )

    op.create_table(
        "messages",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "lead_id",
            sa.BigInteger(),
            sa.ForeignKey("leads.lead_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("direction", sa.SmallInteger(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("provider_msg_id", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("tenant_id", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "idx_messages_lead_created",
        "messages",
        ["lead_id", "created_at"],
        postgresql_ops={"created_at": "DESC"},
    )
    op.create_index(
        "idx_messages_tenant_created_at",
        "messages",
        ["tenant_id", "created_at"],
    )

    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "lead_id",
            sa.BigInteger(),
            sa.ForeignKey("leads.lead_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("dedup_hash", sa.String(length=40), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="queued"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ux_outbox_lead_dedup",
        "outbox",
        ["lead_id", "dedup_hash"],
        unique=True,
    )
    op.create_index(
        "idx_outbox_status_created",
        "outbox",
        ["status", "created_at"],
    )

    op.create_table(
        "source_cache",
        sa.Column("lead_id", sa.BigInteger(), primary_key=True),
        sa.Column("real_id", sa.Integer(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "webhook_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("lead_id", sa.BigInteger(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "kv",
        sa.Column("key", sa.Text(), primary_key=True),
        sa.Column("value", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "contacts",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("whatsapp_phone", sa.Text(), nullable=True, unique=True),
        sa.Column("avito_user_id", sa.BigInteger(), nullable=True, unique=True),
        sa.Column("avito_login", sa.Text(), nullable=True),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=True),
        sa.Column("telegram_username", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "idx_contacts_telegram_user",
        "contacts",
        ["telegram_user_id"],
    )

    op.create_table(
        "lead_contacts",
        sa.Column(
            "lead_id",
            sa.BigInteger(),
            sa.ForeignKey("leads.lead_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "contact_id",
            sa.BigInteger(),
            sa.ForeignKey("contacts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("channel", sa.Text(), nullable=True),
        sa.Column("peer", sa.Text(), nullable=True),
        sa.Column(
            "linked_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("lead_contacts")
    op.drop_index("idx_contacts_telegram_user", table_name="contacts")
    op.drop_table("contacts")
    op.drop_table("kv")
    op.drop_table("webhook_events")
    op.drop_table("source_cache")
    op.drop_index("idx_outbox_status_created", table_name="outbox")
    op.drop_index("ux_outbox_lead_dedup", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("idx_messages_tenant_created_at", table_name="messages")
    op.drop_index("idx_messages_lead_created", table_name="messages")
    op.drop_table("messages")
    op.drop_index("idx_leads_tenant_username", table_name="leads")
    op.drop_index("ux_leads_tenant_telegram", table_name="leads")
    op.drop_index("idx_leads_tenant_updated_at", table_name="leads")
    op.drop_table("leads")
