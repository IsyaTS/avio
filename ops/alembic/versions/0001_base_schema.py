"""Base schema for leads and messages tables."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0001_base_schema"
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
        sa.Column("telegram_username", sa.Text(), nullable=True),
    )
    op.create_unique_constraint(
        "uq_leads_tenant_lead",
        "leads",
        ["tenant_id", "lead_id"],
    )
    op.create_index(
        "idx_leads_updated_at",
        "leads",
        ["updated_at"],
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


def downgrade() -> None:
    op.drop_index("idx_messages_tenant_created_at", table_name="messages")
    op.drop_index("idx_messages_lead_created", table_name="messages")
    op.drop_table("messages")
    op.drop_index("idx_leads_updated_at", table_name="leads")
    op.drop_constraint("uq_leads_tenant_lead", "leads", type_="unique")
    op.drop_table("leads")
