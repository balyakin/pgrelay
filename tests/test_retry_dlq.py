"""Retry and DLQ tests."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.repositories.attempts import AttemptRepository
from pgrelay.repositories.jobs import JobRepository, JobRow
from pgrelay.sdk.client import PgRelayClient


async def _claimed_job(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    max_attempts: int = 3,
) -> JobRow:
    client = PgRelayClient(settings)
    repository = JobRepository()
    async with session_factory() as session:
        await client.enqueue_handler(session=session, name="handler", payload={}, max_attempts=max_attempts)
        await session.commit()
    async with session_factory() as session:
        rows = await repository.claim_jobs(
            session,
            worker_id="worker-1",
            queue_names=["default"],
            batch_size=1,
            lease_seconds=30,
        )
        await session.commit()
    return rows[0]


async def test_retryable_failure_schedules_retry(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """HTTP 500 schedules retry."""
    # ARRANGE
    repository = JobRepository()
    job = await _claimed_job(settings, session_factory)

    # ACT
    async with session_factory() as session:
        updated = await repository.fail_job(
            session,
            job=job,
            worker_id="worker-1",
            retryable=True,
            timed_out=False,
            retry_delay_seconds=10,
            duration_ms=1,
            error_type="http_status",
            error_message="HTTP status 500",
            response_status=500,
            response_body_preview=None,
        )
        await session.commit()
        row = await repository.get_job(session, job.id)

    # ASSERT
    assert updated is True
    assert row is not None
    assert row.status == "pending"


async def test_permanent_failure_goes_to_dlq(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """HTTP 400 goes to DLQ immediately."""
    # ARRANGE
    repository = JobRepository()
    job = await _claimed_job(settings, session_factory)

    # ACT
    async with session_factory() as session:
        await repository.fail_job(
            session,
            job=job,
            worker_id="worker-1",
            retryable=False,
            timed_out=False,
            retry_delay_seconds=10,
            duration_ms=1,
            error_type="http_status",
            error_message="HTTP status 400",
            response_status=400,
            response_body_preview=None,
        )
        await session.commit()
        row = await repository.get_job(session, job.id)

    # ASSERT
    assert row is not None
    assert row.status == "dead_letter"


async def test_retryable_final_attempt_goes_to_dlq(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Retryable final attempt goes to DLQ."""
    # ARRANGE
    repository = JobRepository()
    job = await _claimed_job(settings, session_factory, max_attempts=1)

    # ACT
    async with session_factory() as session:
        await repository.fail_job(
            session,
            job=job,
            worker_id="worker-1",
            retryable=True,
            timed_out=False,
            retry_delay_seconds=10,
            duration_ms=1,
            error_type="http_status",
            error_message="HTTP status 500",
            response_status=500,
            response_body_preview=None,
        )
        await session.commit()
        row = await repository.get_job(session, job.id)

    # ASSERT
    assert row is not None
    assert row.status == "dead_letter"


async def test_permanent_handler_error_writes_attempt(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Permanent handler error goes to DLQ and writes attempt row."""
    # ARRANGE
    repository = JobRepository()
    attempts = AttemptRepository()
    job = await _claimed_job(settings, session_factory)

    # ACT
    async with session_factory() as session:
        await repository.fail_job(
            session,
            job=job,
            worker_id="worker-1",
            retryable=False,
            timed_out=False,
            retry_delay_seconds=10,
            duration_ms=1,
            error_type="ValueError",
            error_message="bad handler",
            response_status=None,
            response_body_preview=None,
        )
        await session.commit()
        rows = await attempts.list_by_job_id(session, job_id=job.id)

    # ASSERT
    assert len(rows) == 1
    assert rows[0].status == "failed"


async def test_failed_job_with_wrong_worker_does_not_write_attempt(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Wrong worker cannot write a failed attempt."""
    # ARRANGE
    repository = JobRepository()
    attempts = AttemptRepository()
    job = await _claimed_job(settings, session_factory)

    # ACT
    async with session_factory() as session:
        updated = await repository.fail_job(
            session,
            job=job,
            worker_id="worker-2",
            retryable=False,
            timed_out=False,
            retry_delay_seconds=10,
            duration_ms=1,
            error_type="ValueError",
            error_message="bad handler",
            response_status=None,
            response_body_preview=None,
        )
        await session.commit()
        rows = await attempts.list_by_job_id(session, job_id=job.id)
        status = await session.scalar(text("SELECT status FROM pgrelay_job WHERE id = :job_id"), {"job_id": job.id})

    # ASSERT
    assert updated is False
    assert rows == []
    assert status == "leased"
