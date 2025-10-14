from __future__ import annotations

from pathlib import Path

import os

import pytest
try:
    import sqlalchemy as sa
except ImportError:  # pragma: no cover - optional dependency for local checks
    sa = None  # type: ignore[assignment]

try:
    from alembic import command
    from alembic.config import Config
except ImportError:  # pragma: no cover - alembic optional in test env
    command = None  # type: ignore[assignment]
    Config = None  # type: ignore[assignment]

if sa is None or command is None or Config is None:  # pragma: no cover
    pytestmark = pytest.mark.skip("alembic or sqlalchemy is not available")


def _alembic_config(database_url: str) -> Config:
    cfg_path = Path(__file__).resolve().parents[1] / "ops" / "alembic.ini"
    config = Config(str(cfg_path))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


@pytest.mark.skipif(
    not os.getenv("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL is not configured",
)
def test_leads_schema_after_upgrade() -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    engine = sa.create_engine(database_url)
    config = _alembic_config(database_url)

    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.downgrade(config, "base")
        command.upgrade(config, "head")

        columns = connection.execute(
            sa.text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'leads'
                ORDER BY ordinal_position
                """
            )
        ).scalars().all()

        assert "id" in columns
        assert "telegram_user_id" in columns
        assert "telegram_username" in columns

        index_defs = connection.execute(
            sa.text(
                """
                SELECT indexdef
                FROM pg_indexes
                WHERE schemaname = current_schema() AND tablename = 'leads'
                """
            )
        ).scalars().all()

        assert any(
            "ux_leads_tenant_telegram_user" in index
            and "WHERE ((telegram_user_id IS NOT NULL))" in index
            for index in index_defs
        )

        command.downgrade(config, "base")
