"""Ensure leads.peer column exists with supporting constraints."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "1d7c3b4f5a6e"
down_revision = "9e4d1c2b3a6f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive guardrail
        raise RuntimeError("Database connection is required for this migration")

    inspector = sa.inspect(bind)
    inspector.clear_cache()

    table_name = "leads"
    table_names = inspector.get_table_names()
    if table_name not in table_names:
        raise RuntimeError("leads table must exist for this migration")

    columns = {column["name"]: column for column in inspector.get_columns(table_name)}
    if "peer" not in columns:
        op.add_column(table_name, sa.Column("peer", sa.String(length=255), nullable=True))
    else:
        column_info = columns["peer"]
        column_type = column_info.get("type")
        length = getattr(column_type, "length", None)
        if not isinstance(column_type, sa.String) or length != 255:
            existing_type = column_type if column_type is not None else sa.Text()
            op.alter_column(
                table_name,
                "peer",
                existing_type=existing_type,
                type_=sa.String(length=255),
                existing_nullable=True,
            )

    constraint_name = "ux_leads_tenant_channel_peer"
    unique_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints(table_name)
    }
    if constraint_name not in unique_constraints:
        op.create_unique_constraint(
            constraint_name,
            table_name,
            ["tenant_id", "channel", "peer"],
        )

    index_name = "idx_leads_tenant_channel_peer"
    indexes = {index["name"] for index in inspector.get_indexes(table_name)}
    if index_name not in indexes:
        op.create_index(index_name, table_name, ["tenant_id", "channel", "peer"])


def downgrade() -> None:
    bind = op.get_bind()
    if bind is None:  # pragma: no cover - defensive guardrail
        raise RuntimeError("Database connection is required for this migration")

    inspector = sa.inspect(bind)
    inspector.clear_cache()

    table_name = "leads"
    table_names = inspector.get_table_names()
    if table_name not in table_names:
        raise RuntimeError("leads table must exist for this migration")

    index_name = "idx_leads_tenant_channel_peer"
    indexes = {index["name"] for index in inspector.get_indexes(table_name)}
    if index_name in indexes:
        op.drop_index(index_name, table_name=table_name)

    constraint_name = "ux_leads_tenant_channel_peer"
    unique_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints(table_name)
    }
    if constraint_name in unique_constraints:
        op.drop_constraint(constraint_name, table_name=table_name, type_="unique")

    columns = {column["name"] for column in inspector.get_columns(table_name)}
    if "peer" in columns:
        op.drop_column(table_name, "peer")

