from __future__ import annotations

import asyncio
import logging
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


def _configure_url() -> str:
    options = context.get_x_argument(as_dictionary=True)
    if "sqlalchemy.url" in options:
        config.set_main_option("sqlalchemy.url", options["sqlalchemy.url"])

    url = config.get_main_option("sqlalchemy.url")
    if not url or url == "postgresql://placeholder":
        message = "sqlalchemy.url must be provided via -x sqlalchemy.url=..."
        _logger.error(message)
        raise RuntimeError(message)

    return url


def _run_sync_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    url = _configure_url()
    connectable: AsyncEngine = create_async_engine(
        url,
        future=True,
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(_run_sync_migrations)

    await connectable.dispose()


asyncio.run(run_migrations_online())
