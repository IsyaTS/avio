from __future__ import annotations

import asyncio
import logging
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import MetaData, pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine, async_engine_from_config

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

metadata = MetaData()
_logger = logging.getLogger("alembic.env")


def _require_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        message = "DATABASE_URL environment variable is required for Alembic migrations"
        _logger.error(message)
        raise RuntimeError(message)

    config.set_main_option("sqlalchemy.url", url)
    return url


def run_migrations_offline() -> None:
    url = _require_database_url()
    context.configure(
        url=url,
        target_metadata=metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def _run_sync_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    dsn = _require_database_url()
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = dsn

    connectable: AsyncEngine = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(_run_sync_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
