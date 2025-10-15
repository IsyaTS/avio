from __future__ import annotations

import os
import logging
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, MetaData

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

metadata = MetaData()
_logger = logging.getLogger("alembic.env")


def _set_sqlalchemy_url() -> str:
    explicit_url = os.getenv("DATABASE_URL") or os.getenv("OPS_DB_URL")
    if explicit_url:
        config.set_main_option("sqlalchemy.url", explicit_url)
        return explicit_url

    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    database = os.getenv("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST") or os.getenv("POSTGRES_SERVER") or "postgres"
    port = os.getenv("POSTGRES_PORT") or "5432"

    if user and password and database:
        dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        config.set_main_option("sqlalchemy.url", dsn)
        return dsn

    existing = config.get_main_option("sqlalchemy.url")
    if existing:
        return existing

    message = (
        "DATABASE_URL or POSTGRES_* environment variables must be provided "
        "to run Alembic migrations"
    )
    _logger.error(message)
    raise RuntimeError(message)

def run_migrations_offline() -> None:
    url = _set_sqlalchemy_url()
    context.configure(
        url=url,
        target_metadata=metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    dsn = _set_sqlalchemy_url()
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = dsn
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=metadata)
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
