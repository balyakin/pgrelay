"""Replay and cancel tests."""

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.errors import JobStateConflictError
from pgrelay.repositories.attempts import AttemptRepository
from pgrelay.repositories.jobs import JobRepository
from pgrelay.sdk.client import PgRelayClient
from pgrelay.services.jobs import JobService


async def _create_job_with_status(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    status: str,
) -> UUID:
    client = PgRelayClient(settings)
    async with session_factory() as session:
        result = await client.enqueue_handler(session=session, name="handler", payload={})
        await session.execute(
            text("UPDATE pgrelay_job SET status = :status, completed_at = now() WHERE id = :job_id"),
            {"status": status, "job_id": result.job_id},
        )
        await session.commit()
        return result.job_id


async def test_replay_dead_letter_job_creates_pending_job(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Replay dead-letter job creates pending job."""
    # ARRANGE
    job_id = await _create_job_with_status(settings, session_factory, "dead_letter")
    service = JobService(JobRepository(), AttemptRepository())

    # ACT
    async with session_factory() as session:
        replay = await service.replay(session, job_id=job_id, force=False)
        await session.commit()
        status = await session.scalar(
            text("SELECT status FROM pgrelay_job WHERE id = :job_id"),
            {"job_id": replay.new_job_id},
        )

    # ASSERT
    assert status == "pending"


async def test_replay_sets_replayed_from_job_id(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Replay sets replayed_from_job_id."""
    # ARRANGE
    job_id = await _create_job_with_status(settings, session_factory, "dead_letter")
    service = JobService(JobRepository(), AttemptRepository())

    # ACT
    async with session_factory() as session:
        replay = await service.replay(session, job_id=job_id, force=False)
        await session.commit()
        source = await session.scalar(
            text("SELECT replayed_from_job_id::text FROM pgrelay_job WHERE id = :job_id"),
            {"job_id": replay.new_job_id},
        )

    # ASSERT
    assert source == str(job_id)


async def test_replay_succeeded_without_force_returns_conflict(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Replay succeeded without force returns conflict."""
    # ARRANGE
    job_id = await _create_job_with_status(settings, session_factory, "succeeded")
    service = JobService(JobRepository(), AttemptRepository())

    # ACT
    async with session_factory() as session:
        try:
            await service.replay(session, job_id=job_id, force=False)
        except JobStateConflictError:
            conflict = True
        else:
            conflict = False

    # ASSERT
    assert conflict is True


async def test_replay_succeeded_with_force_creates_job(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Replay succeeded with force creates job."""
    # ARRANGE
    job_id = await _create_job_with_status(settings, session_factory, "succeeded")
    service = JobService(JobRepository(), AttemptRepository())

    # ACT
    async with session_factory() as session:
        replay = await service.replay(session, job_id=job_id, force=True)
        await session.commit()

    # ASSERT
    assert replay.source_job_id == job_id


async def test_cancel_pending_job_works(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Cancel pending job works."""
    # ARRANGE
    client = PgRelayClient(settings)
    service = JobService(JobRepository(), AttemptRepository())
    async with session_factory() as session:
        result = await client.enqueue_handler(session=session, name="handler", payload={})
        await session.commit()

    # ACT
    async with session_factory() as session:
        cancelled = await service.cancel(session, job_id=result.job_id)
        await session.commit()

    # ASSERT
    assert cancelled.status == "cancelled"


async def test_cancel_leased_job_returns_conflict(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Cancel leased job returns conflict."""
    # ARRANGE
    repository = JobRepository()
    service = JobService(repository, AttemptRepository())
    client = PgRelayClient(settings)
    async with session_factory() as session:
        result = await client.enqueue_handler(session=session, name="handler", payload={})
        await session.commit()
    async with session_factory() as session:
        await repository.claim_jobs(
            session,
            worker_id="worker",
            queue_names=["default"],
            batch_size=1,
            lease_seconds=30,
        )
        await session.commit()

    # ACT
    async with session_factory() as session:
        try:
            await service.cancel(session, job_id=result.job_id)
        except JobStateConflictError:
            conflict = True
        else:
            conflict = False

    # ASSERT
    assert conflict is True
