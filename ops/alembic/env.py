from __future__ import annotations

import asyncio
import logging
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import MetaData, pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

metadata = MetaData()
_logger = logging.getLogger("alembic.env")


def _require_database_url() -> str:
    try:
        url = os.environ["DATABASE_URL"]
    except KeyError as exc:  # pragma: no cover - defensive guardrail
        message = "DATABASE_URL environment variable is required for Alembic migrations"
        _logger.error(message)
        raise RuntimeError(message) from exc

    config.set_main_option("sqlalchemy.url", url)
    return url


def _run_sync_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    dsn = _require_database_url()
    connectable: AsyncEngine = create_async_engine(
        dsn,
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(_run_sync_migrations)

    await connectable.dispose()


asyncio.run(run_migrations_online())
