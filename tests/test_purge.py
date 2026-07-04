"""Purge tests."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.repositories.jobs import JobRepository
from pgrelay.sdk.client import PgRelayClient
from pgrelay.services.purge import PurgeService


async def _create_status(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    status: str,
    old: bool,
) -> str:
    client = PgRelayClient(settings)
    async with session_factory() as session:
        result = await client.enqueue_handler(session=session, name="handler", payload={})
        completed_sql = "now() - interval '40 days'" if old else "now()"
        await session.execute(
            text(f"UPDATE pgrelay_job SET status = :status, completed_at = {completed_sql} WHERE id = :job_id"),
            {"status": status, "job_id": result.job_id},
        )
        await session.commit()
        return str(result.job_id)


async def test_purge_removes_old_succeeded_jobs(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Purge removes old succeeded jobs."""
    # ARRANGE
    await _create_status(settings, session_factory, "succeeded", old=True)
    service = PurgeService(JobRepository(), settings)

    # ACT
    async with session_factory() as session:
        result = await service.purge_batch(session)
        await session.commit()

    # ASSERT
    assert result.succeeded == 1


async def test_purge_removes_old_dead_letter_jobs(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Purge removes old dead-letter jobs according to separate retention."""
    # ARRANGE
    await _create_status(settings, session_factory, "dead_letter", old=True)
    service = PurgeService(JobRepository(), settings)

    # ACT
    async with session_factory() as session:
        result = await service.purge_batch(session)
        await session.commit()

    # ASSERT
    assert result.dead_letter == 1


async def test_purge_until_done_removes_old_cancelled_jobs(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Purge until done removes old cancelled jobs."""
    # ARRANGE
    await _create_status(settings, session_factory, "cancelled", old=True)
    await _create_status(settings, session_factory, "cancelled", old=True)
    configured = settings.model_copy(update={"purge_batch_size": 1})
    service = PurgeService(JobRepository(), configured)

    # ACT
    summary = await service.purge_until_done(session_factory)
    async with session_factory() as session:
        remaining = await session.scalar(text("SELECT count(*) FROM pgrelay_job"))

    # ASSERT
    assert summary.cancelled == 2
    assert summary.total == 2
    assert int(remaining or 0) == 0


async def test_purge_does_not_remove_active_jobs(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Purge does not remove active jobs."""
    # ARRANGE
    client = PgRelayClient(settings)
    service = PurgeService(JobRepository(), settings)
    async with session_factory() as session:
        await client.enqueue_handler(session=session, name="handler", payload={})
        await session.commit()

    # ACT
    async with session_factory() as session:
        result = await service.purge_batch(session)
        count = await session.scalar(text("SELECT count(*) FROM pgrelay_job"))

    # ASSERT
    assert result.total == 0
    assert int(count or 0) == 1


async def test_purge_batch_size_limits_total_deleted_jobs(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Purge deletes no more than one total configured batch."""
    # ARRANGE
    for _index in range(2):
        await _create_status(settings, session_factory, "succeeded", old=True)
        await _create_status(settings, session_factory, "dead_letter", old=True)
    configured = settings.model_copy(update={"purge_batch_size": 3})
    service = PurgeService(JobRepository(), configured)

    # ACT
    async with session_factory() as session:
        result = await service.purge_batch(session)
        await session.commit()
        remaining = await session.scalar(text("SELECT count(*) FROM pgrelay_job"))

    # ASSERT
    assert result.total == 3
    assert int(remaining or 0) == 1


async def test_attempts_are_removed_by_cascade(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Attempts are removed by cascade."""
    # ARRANGE
    job_id = await _create_status(settings, session_factory, "succeeded", old=True)
    service = PurgeService(JobRepository(), settings)
    async with session_factory() as session:
        await session.execute(
            text(
                """
                INSERT INTO pgrelay_attempt (
                    job_id, attempt_number, worker_id, status, started_at, finished_at
                )
                VALUES (:job_id, 1, 'worker', 'succeeded', now(), now())
                """
            ),
            {"job_id": job_id},
        )
        await session.commit()

    # ACT
    async with session_factory() as session:
        await service.purge_batch(session)
        await session.commit()
        count = await session.scalar(text("SELECT count(*) FROM pgrelay_attempt"))

    # ASSERT
    assert int(count or 0) == 0
