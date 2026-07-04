"""Job CLI commands."""

import asyncio
from uuid import UUID

import typer

from pgrelay.config.settings import load_settings
from pgrelay.db.session import create_engine, create_session_factory
from pgrelay.repositories.attempts import AttemptRepository
from pgrelay.repositories.jobs import JobRepository
from pgrelay.services.jobs import JobService
from pgrelay.services.purge import PurgeService
from pgrelay.utils.validation import validate_queue_name


async def replay_async(job_id: UUID, force: bool) -> None:
    """Replay a job asynchronously."""
    settings = load_settings()
    settings.validate_runtime()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        service = JobService(JobRepository(), AttemptRepository())
        async with session_factory() as session:
            result = await service.replay(session, job_id=job_id, force=force)
            await session.commit()
        typer.echo(str(result.new_job_id))
    finally:
        await engine.dispose()


def replay(job_id: UUID, force: bool = False) -> None:
    """Replay a job."""
    asyncio.run(replay_async(job_id, force))


async def drain_async(queue_name: str, timeout_seconds: int) -> int:
    """Wait for a queue to drain."""
    validate_queue_name(queue_name)
    settings = load_settings()
    settings.validate_runtime()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    repository = JobRepository()
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    try:
        while True:
            async with session_factory() as session:
                pending = await repository.list_jobs(
                    session,
                    status="pending",
                    queue_name=queue_name,
                    kind=None,
                    name=None,
                    dedupe_key=None,
                    limit=1,
                    offset=0,
                    include_total=True,
                )
                leased = await repository.list_jobs(
                    session,
                    status="leased",
                    queue_name=queue_name,
                    kind=None,
                    name=None,
                    dedupe_key=None,
                    limit=1,
                    offset=0,
                    include_total=True,
                )
            pending_total = pending.total or 0
            leased_total = leased.total or 0
            if pending_total + leased_total == 0:
                typer.echo("drained")
                return 0
            if asyncio.get_running_loop().time() >= deadline:
                typer.echo("drain timed out")
                return 1
            await asyncio.sleep(settings.worker_poll_interval_seconds)
    finally:
        await engine.dispose()


def drain(queue_name: str, timeout_seconds: int = 300) -> None:
    """Wait until a queue has no pending or leased jobs."""
    raise typer.Exit(asyncio.run(drain_async(queue_name, timeout_seconds)))


async def purge_async() -> None:
    """Purge old final jobs asynchronously."""
    settings = load_settings()
    settings.validate_runtime()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        service = PurgeService(JobRepository(), settings)
        summary = await service.purge_until_done(session_factory)
        typer.echo(f"purged succeeded={summary.succeeded} dead_letter={summary.dead_letter}")
    finally:
        await engine.dispose()


def purge() -> None:
    """Purge old final jobs."""
    asyncio.run(purge_async())
