"""Programmatic Alembic helpers."""

from pathlib import Path

from alembic import command
from alembic.config import Config

from pgrelay.config.settings import Settings


def create_alembic_config(settings: Settings) -> Config:
    """Create Alembic configuration for PgRelay migrations."""
    root = Path(__file__).resolve().parents[3]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "migrations"))
    config.set_main_option("sqlalchemy.url", settings.database_url)
    config.set_main_option("version_table", "pgrelay_alembic_version")
    return config


def upgrade(settings: Settings, revision: str = "head") -> None:
    """Run Alembic upgrade."""
    command.upgrade(create_alembic_config(settings), revision)


def downgrade(settings: Settings, revision: str) -> None:
    """Run Alembic downgrade."""
    command.downgrade(create_alembic_config(settings), revision)
