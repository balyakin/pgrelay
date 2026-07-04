"""Idempotency tests."""

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.repositories.attempts import AttemptRepository
from pgrelay.repositories.jobs import JobRepository
from pgrelay.sdk.client import PgRelayClient
from pgrelay.services.jobs import JobService
from tests.conftest import job_count


async def test_same_idempotency_key_same_queue_returns_same_job(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Same idempotency key in same queue returns same job id."""
    # ARRANGE
    client = PgRelayClient(settings)

    # ACT
    async with session_factory() as session:
        first = await client.enqueue_handler(session=session, name="handler", payload={}, idempotency_key="same")
        second = await client.enqueue_handler(session=session, name="handler", payload={}, idempotency_key="same")
        await session.commit()

    # ASSERT
    assert first.job_id == second.job_id
    assert second.created is False


async def test_same_idempotency_key_different_queue_creates_different_job(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Same idempotency key in different queue creates different job."""
    # ARRANGE
    client = PgRelayClient(settings)

    # ACT
    async with session_factory() as session:
        first = await client.enqueue_handler(session=session, name="handler", payload={}, idempotency_key="same")
        second = await client.enqueue_handler(
            session=session,
            name="handler",
            payload={},
            queue_name="other",
            idempotency_key="same",
        )
        await session.commit()

    # ASSERT
    assert first.job_id != second.job_id


async def test_null_idempotency_key_always_creates_new_jobs(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Null idempotency key always creates new jobs."""
    # ARRANGE
    client = PgRelayClient(settings)

    # ACT
    async with session_factory() as session:
        first = await client.enqueue_handler(session=session, name="handler", payload={})
        second = await client.enqueue_handler(session=session, name="handler", payload={})
        await session.commit()

    # ASSERT
    assert first.job_id != second.job_id


async def test_concurrent_same_key_creates_one_job(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Two concurrent transactions with same idempotency key create one job."""
    # ARRANGE
    client = PgRelayClient(settings)

    async def enqueue_once() -> str:
        async with session_factory() as session:
            result = await client.enqueue_handler(session=session, name="handler", payload={}, idempotency_key="race")
            await session.commit()
            return str(result.job_id)

    # ACT
    ids = await asyncio.gather(enqueue_once(), enqueue_once())

    # ASSERT
    async with session_factory() as session:
        assert ids[0] == ids[1]
        assert await job_count(session) == 1


async def test_replay_default_idempotency_prevents_duplicate_replay_job(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Replay idempotency prevents duplicate replay jobs."""
    # ARRANGE
    client = PgRelayClient(settings)
    service = JobService(JobRepository(), AttemptRepository())
    async with session_factory() as session:
        source = await client.enqueue_handler(session=session, name="handler", payload={})
        await session.execute(
            text("UPDATE pgrelay_job SET status = 'dead_letter', completed_at = now() WHERE id = :job_id"),
            {"job_id": source.job_id},
        )
        await session.commit()

    # ACT
    async with session_factory() as session:
        first = await service.replay(session, job_id=source.job_id, force=False)
        second = await service.replay(session, job_id=source.job_id, force=False)
        await session.commit()
        replay_count = await session.scalar(
            text("SELECT count(*) FROM pgrelay_job WHERE replayed_from_job_id = :job_id"),
            {"job_id": source.job_id},
        )

    # ASSERT
    assert first.new_job_id == second.new_job_id
    assert replay_count == 1
