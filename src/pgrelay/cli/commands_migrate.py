"""Migration CLI commands."""

import typer

from pgrelay.config.settings import load_settings
from pgrelay.db import migrations

migrate_app = typer.Typer(help="Run PgRelay migrations")


@migrate_app.command("upgrade")
def migrate_upgrade(revision: str = "head") -> None:
    """Run Alembic upgrade."""
    settings = load_settings()
    settings.validate_runtime()
    migrations.upgrade(settings, revision)


@migrate_app.command("downgrade")
def migrate_downgrade(revision: str) -> None:
    """Run Alembic downgrade."""
    settings = load_settings()
    settings.validate_runtime()
    migrations.downgrade(settings, revision)
