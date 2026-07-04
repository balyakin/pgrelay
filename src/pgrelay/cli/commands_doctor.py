"""Doctor CLI command."""

import asyncio

import typer
from sqlalchemy import text

from pgrelay.config.settings import load_settings
from pgrelay.constants import DEFAULT_QUEUE_NAME
from pgrelay.db.session import create_engine, create_session_factory


async def doctor_async() -> int:
    """Run doctor checks."""
    settings = load_settings()
    settings.validate_runtime()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        async with session_factory() as session:
            version = await session.scalar(text("SHOW server_version_num"))
            if version is None or int(str(version)) < 150000:
                typer.echo("PostgreSQL 15 or newer is required")
                return 1
            required_tables = [
                "pgrelay_queue",
                "pgrelay_job",
                "pgrelay_attempt",
                "pgrelay_worker",
                "pgrelay_alembic_version",
            ]
            for table_name in required_tables:
                exists = await session.scalar(text("SELECT to_regclass(:table_name)"), {"table_name": table_name})
                if exists is None:
                    typer.echo(f"Missing table: {table_name}")
                    return 1
            migration = await session.scalar(text("SELECT version_num FROM pgrelay_alembic_version LIMIT 1"))
            default_queue = await session.scalar(
                text("SELECT name FROM pgrelay_queue WHERE name = :queue_name"),
                {"queue_name": DEFAULT_QUEUE_NAME},
            )
            if default_queue is None:
                typer.echo(f"Missing queue: {DEFAULT_QUEUE_NAME}")
                return 1
            stale_pending = await session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pgrelay_job
                        WHERE status = 'pending' AND available_at < now() - interval '10 minutes'
                    )
                    """
                )
            )
            alive_worker = await session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pgrelay_worker
                        WHERE last_heartbeat_at > now() - interval '2 minutes'
                    )
                    """
                )
            )
            if stale_pending and not alive_worker:
                typer.echo("Pending jobs are stale and no alive worker was found")
                return 1
            typer.echo(f"ok postgresql={version} migration={migration} queue={default_queue}")
            return 0
    finally:
        await engine.dispose()


def doctor() -> None:
    """Run PgRelay environment checks."""
    raise typer.Exit(asyncio.run(doctor_async()))
