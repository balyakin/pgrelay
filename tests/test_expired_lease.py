"""Expired lease tests."""

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.repositories.attempts import AttemptRepository
from pgrelay.repositories.jobs import JobRepository
from pgrelay.sdk.client import PgRelayClient


async def _claim_one(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    max_attempts: int,
) -> UUID:
    client = PgRelayClient(settings)
    repository = JobRepository()
    async with session_factory() as session:
        result = await client.enqueue_handler(session=session, name="handler", payload={}, max_attempts=max_attempts)
        await session.commit()
    async with session_factory() as session:
        await repository.claim_jobs(
            session,
            worker_id="worker-1",
            queue_names=["default"],
            batch_size=1,
            lease_seconds=5,
        )
        await session.execute(
            text("UPDATE pgrelay_job SET locked_until = now() - interval '1 second' WHERE id = :job_id"),
            {"job_id": result.job_id},
        )
        await session.commit()
    return result.job_id


async def test_expired_leased_job_returns_to_pending(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Expired leased job returns to pending."""
    # ARRANGE
    repository = JobRepository()
    job_id = await _claim_one(settings, session_factory, max_attempts=3)

    # ACT
    async with session_factory() as session:
        recovered = await repository.recover_expired_leases(session, batch_size=10)
        await session.commit()
        status = await session.scalar(text("SELECT status FROM pgrelay_job WHERE id = :job_id"), {"job_id": job_id})

    # ASSERT
    assert recovered == 1
    assert status == "pending"


async def test_expired_final_attempt_goes_to_dlq(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Expired leased final attempt goes to DLQ."""
    # ARRANGE
    repository = JobRepository()
    job_id = await _claim_one(settings, session_factory, max_attempts=1)

    # ACT
    async with session_factory() as session:
        await repository.recover_expired_leases(session, batch_size=10)
        await session.commit()
        status = await session.scalar(text("SELECT status FROM pgrelay_job WHERE id = :job_id"), {"job_id": job_id})

    # ASSERT
    assert status == "dead_letter"


async def test_non_expired_lease_is_untouched(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Non-expired lease is untouched."""
    # ARRANGE
    client = PgRelayClient(settings)
    repository = JobRepository()
    async with session_factory() as session:
        result = await client.enqueue_handler(session=session, name="handler", payload={})
        await session.commit()
    async with session_factory() as session:
        await repository.claim_jobs(
            session,
            worker_id="worker-1",
            queue_names=["default"],
            batch_size=1,
            lease_seconds=30,
        )
        await session.commit()

    # ACT
    async with session_factory() as session:
        recovered = await repository.recover_expired_leases(session, batch_size=10)
        status = await session.scalar(
            text("SELECT status FROM pgrelay_job WHERE id = :job_id"),
            {"job_id": result.job_id},
        )

    # ASSERT
    assert recovered == 0
    assert status == "leased"


async def test_lease_expired_attempt_is_written(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """lease_expired attempt is written."""
    # ARRANGE
    repository = JobRepository()
    attempts = AttemptRepository()
    job_id = await _claim_one(settings, session_factory, max_attempts=3)

    # ACT
    async with session_factory() as session:
        await repository.recover_expired_leases(session, batch_size=10)
        await session.commit()
        rows = await attempts.list_by_job_id(session, job_id=job_id)

    # ASSERT
    assert len(rows) == 1
    assert rows[0].status == "lease_expired"
