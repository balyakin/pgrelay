"""Claim tests."""

import asyncio
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.repositories.jobs import JobRepository
from pgrelay.repositories.queues import QueueRepository
from pgrelay.sdk.client import PgRelayClient


async def _enqueue(settings: Settings, session: AsyncSession, name: str, queue_name: str = "default") -> None:
    client = PgRelayClient(settings)
    await client.enqueue_handler(session=session, name=name, payload={}, queue_name=queue_name)


async def test_claim_returns_due_pending_jobs(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Claim returns due pending jobs."""
    # ARRANGE
    repository = JobRepository()
    async with session_factory() as session:
        await _enqueue(settings, session, "a")
        await session.commit()

    # ACT
    async with session_factory() as session:
        jobs = await repository.claim_jobs(
            session,
            worker_id="worker-1",
            queue_names=["default"],
            batch_size=2,
            lease_seconds=30,
        )
        await session.commit()

    # ASSERT
    assert len(jobs) == 1
    assert jobs[0].status == "leased"


async def test_claim_ignores_future_available_at(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Claim ignores future available_at."""
    # ARRANGE
    client = PgRelayClient(settings)
    repository = JobRepository()
    async with session_factory() as session:
        await client.enqueue_handler(
            session=session,
            name="future",
            payload={},
            available_at=datetime.now(UTC) + timedelta(hours=1),
        )
        await session.commit()

    # ACT
    async with session_factory() as session:
        jobs = await repository.claim_jobs(
            session,
            worker_id="worker-1",
            queue_names=["default"],
            batch_size=2,
            lease_seconds=30,
        )

    # ASSERT
    assert jobs == []


async def test_claim_ignores_paused_queues(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Claim ignores paused queues."""
    # ARRANGE
    jobs = JobRepository()
    queues = QueueRepository()
    async with session_factory() as session:
        await _enqueue(settings, session, "a")
        await queues.pause_queue(session, queue_name="default")
        await session.commit()

    # ACT
    async with session_factory() as session:
        claimed = await jobs.claim_jobs(
            session,
            worker_id="worker-1",
            queue_names=["default"],
            batch_size=2,
            lease_seconds=30,
        )

    # ASSERT
    assert claimed == []


async def test_concurrent_claim_returns_disjoint_jobs(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Concurrent claim from two sessions returns disjoint jobs."""
    # ARRANGE
    repository = JobRepository()
    async with session_factory() as session:
        for index in range(4):
            await _enqueue(settings, session, f"job-{index}")
        await session.commit()

    async def claim(worker_id: str) -> set[str]:
        async with session_factory() as session:
            rows = await repository.claim_jobs(
                session,
                worker_id=worker_id,
                queue_names=["default"],
                batch_size=2,
                lease_seconds=30,
            )
            await session.commit()
            return {str(row.id) for row in rows}

    # ACT
    first, second = await asyncio.gather(claim("worker-1"), claim("worker-2"))

    # ASSERT
    assert first.isdisjoint(second)
    assert len(first | second) == 4


async def test_worker_claims_no_more_than_free_slots(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Worker claims no more than free slots."""
    # ARRANGE
    repository = JobRepository()
    async with session_factory() as session:
        for index in range(3):
            await _enqueue(settings, session, f"job-{index}")
        await session.commit()

    # ACT
    async with session_factory() as session:
        claimed = await repository.claim_jobs(
            session,
            worker_id="worker-1",
            queue_names=["default"],
            batch_size=1,
            lease_seconds=30,
        )

    # ASSERT
    assert len(claimed) == 1


async def test_queue_concurrency_limit_is_respected(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Queue concurrency_limit is respected best-effort."""
    # ARRANGE
    repository = JobRepository()
    queues = QueueRepository()
    async with session_factory() as session:
        for index in range(3):
            await _enqueue(settings, session, f"job-{index}")
        await queues.update_queue(session, queue_name="default", paused=False, concurrency_limit=1)
        await session.commit()

    # ACT
    async with session_factory() as session:
        first = await repository.claim_jobs(
            session,
            worker_id="worker-1",
            queue_names=["default"],
            batch_size=3,
            lease_seconds=30,
        )
        await session.commit()
    async with session_factory() as session:
        second = await repository.claim_jobs(
            session,
            worker_id="worker-2",
            queue_names=["default"],
            batch_size=3,
            lease_seconds=30,
        )

    # ASSERT
    assert len(first) == 1
    assert second == []


async def test_concurrent_claim_respects_queue_concurrency_limit(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Concurrent claim respects queue concurrency_limit."""
    # ARRANGE
    repository = JobRepository()
    queues = QueueRepository()
    async with session_factory() as session:
        for index in range(2):
            await _enqueue(settings, session, f"job-{index}")
        await queues.update_queue(session, queue_name="default", paused=False, concurrency_limit=1)
        await session.commit()

    async def claim(worker_id: str) -> int:
        async with session_factory() as session:
            rows = await repository.claim_jobs(
                session,
                worker_id=worker_id,
                queue_names=["default"],
                batch_size=1,
                lease_seconds=30,
            )
            await asyncio.sleep(0.1)
            await session.commit()
            return len(rows)

    # ACT
    first_task = asyncio.create_task(claim("worker-1"))
    second_task = asyncio.create_task(claim("worker-2"))
    first_count = await first_task
    second_count = await second_task

    # ASSERT
    assert first_count + second_count == 1


async def test_claim_does_not_skip_whole_queue_due_to_locked_queue_row(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Claim SQL does not skip whole queue due to locked queue row."""
    # ARRANGE
    repository = JobRepository()
    async with session_factory() as session:
        await _enqueue(settings, session, "job")
        await session.commit()

    # ACT
    async with session_factory() as locker:
        claimer = session_factory()
        await locker.execute(text("SELECT * FROM pgrelay_queue WHERE name = 'default' FOR UPDATE"))
        async with claimer as session:
            claim_task = asyncio.create_task(
                repository.claim_jobs(
                    session,
                    worker_id="worker-1",
                    queue_names=["default"],
                    batch_size=1,
                    lease_seconds=30,
                )
            )
            await asyncio.sleep(0.1)
            await locker.commit()
            claimed = await claim_task
            await session.commit()

    # ASSERT
    assert len(claimed) == 1
