"""Alembic environment for PgRelay."""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import make_url

from pgrelay.config.settings import load_settings
from pgrelay.db import models
from pgrelay.db.base import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

database_url = config.get_main_option("sqlalchemy.url") or load_settings().database_url
url = make_url(database_url).set(drivername="postgresql+psycopg")
config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False))
target_metadata = Base.metadata
loaded_models = (models.PgRelayAttempt, models.PgRelayJob, models.PgRelayQueue, models.PgRelayWorker)


def run_migrations_offline() -> None:
    """Run migrations without creating an engine."""
    context.configure(
        url=url.render_as_string(hide_password=False),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table="pgrelay_alembic_version",
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations with a sync Alembic engine."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table="pgrelay_alembic_version",
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
