"""Alembic migration environment for the orchestrator database."""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import URL

from orchestrator.database import Base
from orchestrator.settings import settings

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _database_url() -> URL:
    return URL.create(
        "postgresql+psycopg",
        username=settings.orchestrator_database_username,
        password=settings.orchestrator_database_password,
        host=settings.orchestrator_database_host,
        port=settings.orchestrator_database_port,
        database=settings.orchestrator_database_name,
    )


def run_migrations_offline() -> None:
    """Run migrations without opening a database connection."""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations using a synchronous Psycopg connection."""
    engine = create_engine(_database_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
