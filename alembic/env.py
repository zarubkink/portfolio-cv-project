"""Alembic environment.

Loads SQLModel metadata, filters out extension-managed tables, and runs
migrations online. We use a *sync* psycopg2 engine here because asyncpg
+ SQLAlchemy 2.x has a known issue with ENUM types inside a single
DDL transaction: the prepared-statement cache races between
``CREATE TYPE`` and the first reference to that type, raising
``DuplicateObjectError`` even on a fresh database. psycopg2 does not
have this problem.

The runtime application still uses asyncpg — this is a migrations-only
choice.
"""

from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from sqlmodel import SQLModel

import src.models  # noqa: F401  -- ensure all models are imported
from alembic import context
from src.config.database import DatabaseSettings

config = context.config
settings = DatabaseSettings()
sync_url = settings.database_url.replace("postgresql+asyncpg", "postgresql")
config.set_main_option("sqlalchemy.url", sync_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = SQLModel.metadata


def include_object(object, name, type_, reflected, compare_to):
    # Skip extension-managed tables that we never declared in SQLModel.
    if type_ == "table" and name in {
        "spatial_ref_sys",  # PostGIS
        "_typmod_cache",  # ParadeDB (kept defensively in case it returns)
    }:
        return False
    return True


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
