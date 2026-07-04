"""Worker runner tests."""

import asyncio
from typing import Any, cast

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.repositories.jobs import JobRepository, JobRow
from pgrelay.sdk.client import PgRelayClient
from pgrelay.worker.dispatcher import ExecutorResult, JobDispatcher
from pgrelay.worker.runner import WorkerRunner
from tests.conftest import make_job_row


class RaisingDispatcher:
    """Dispatcher that raises during execution."""

    async def dispatch(self, job: JobRow) -> ExecutorResult:
        """Raise an execution error."""
        raise RuntimeError("executor failed")


class BlockingDispatcher:
    """Dispatcher that blocks until cancelled."""

    def __init__(self) -> None:
        """Initialize synchronization state."""
        self.started = asyncio.Event()

    async def dispatch(self, job: JobRow) -> ExecutorResult:
        """Block until the task is cancelled."""
        self.started.set()
        await asyncio.sleep(30)
        return ExecutorResult(
            outcome="succeeded",
            error_type=None,
            error_message=None,
            response_status=None,
            response_body_preview=None,
            duration_ms=0,
        )


class SuccessDispatcher:
    """Dispatcher that returns success."""

    async def dispatch(self, job: JobRow) -> ExecutorResult:
        """Return a successful execution result."""
        return ExecutorResult(
            outcome="succeeded",
            error_type=None,
            error_message=None,
            response_status=None,
            response_body_preview=None,
            duration_ms=7,
        )


class FakeLogger:
    """Logger that stores structured events."""

    def __init__(self) -> None:
        """Initialize event storage."""
        self.events: list[dict[str, object]] = []

    def exception(self, event: str, **kwargs: object) -> None:
        """Store exception log event."""
        values: dict[str, object] = {"event": event}
        values.update(kwargs)
        self.events.append(values)

    def info(self, event: str, **kwargs: object) -> None:
        """Store info log event."""
        values: dict[str, object] = {"event": event}
        values.update(kwargs)
        self.events.append(values)


async def _claim_handler_job(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    worker_id: str,
    max_attempts: int,
) -> JobRow:
    client = PgRelayClient(settings)
    repository = JobRepository()
    async with session_factory() as session:
        await client.enqueue_handler(session=session, name="handler", payload={}, max_attempts=max_attempts)
        await session.commit()
    async with session_factory() as session:
        rows = await repository.claim_jobs(
            session,
            worker_id=worker_id,
            queue_names=["default"],
            batch_size=1,
            lease_seconds=30,
        )
        await session.commit()
    return rows[0]


async def test_execute_job_records_unhandled_executor_exception(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Unhandled executor exceptions are recorded as failures."""
    # ARRANGE
    runner = WorkerRunner(settings=settings, session_factory=session_factory)
    job = await _claim_handler_job(settings, session_factory, worker_id=runner.worker_id, max_attempts=1)

    # ACT
    await runner._execute_job(cast(JobDispatcher, RaisingDispatcher()), job)

    # ASSERT
    async with session_factory() as session:
        row = (
            (
                await session.execute(
                    text(
                        """
                    SELECT j.status, a.error_type
                    FROM pgrelay_job j
                    JOIN pgrelay_attempt a ON a.job_id = j.id
                    WHERE j.id = :job_id
                    """
                    ),
                    {"job_id": job.id},
                )
            )
            .mappings()
            .one()
        )
    assert row["status"] == "dead_letter"
    assert row["error_type"] == "RuntimeError"


async def test_execute_job_cancellation_returns_owned_job_to_pending(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Cancelled execution returns the owned leased job to pending."""
    # ARRANGE
    runner = WorkerRunner(settings=settings, session_factory=session_factory)
    dispatcher = BlockingDispatcher()
    job = await _claim_handler_job(settings, session_factory, worker_id=runner.worker_id, max_attempts=1)
    task = asyncio.create_task(runner._execute_job(cast(JobDispatcher, dispatcher), job))
    await dispatcher.started.wait()

    # ACT
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # ASSERT
    async with session_factory() as session:
        status = await session.scalar(text("SELECT status FROM pgrelay_job WHERE id = :job_id"), {"job_id": job.id})
        attempts = await session.scalar(
            text("SELECT count(*) FROM pgrelay_attempt WHERE job_id = :job_id"),
            {"job_id": job.id},
        )
        attempt_count = await session.scalar(
            text("SELECT attempt_count FROM pgrelay_job WHERE id = :job_id"),
            {"job_id": job.id},
        )
    assert status == "pending"
    assert attempts == 0
    assert attempt_count == 0


async def test_heartbeat_failure_log_includes_error_message(settings: Settings) -> None:
    """Heartbeat failure log includes exception message."""

    # ARRANGE
    async def fail_heartbeat() -> bool:
        raise RuntimeError("heartbeat boom")

    runner = WorkerRunner(settings=settings, session_factory=cast(async_sessionmaker[AsyncSession], object()))
    logger = FakeLogger()
    runner._logger = cast(Any, logger)
    task = asyncio.create_task(fail_heartbeat())
    await asyncio.sleep(0)

    # ACT
    lease_owned = await runner._finish_heartbeat(task)

    # ASSERT
    assert lease_owned is False
    assert {
        "event": "lease_heartbeat_failed",
        "error_type": "RuntimeError",
        "error_message": "heartbeat boom",
    } in logger.events


async def test_dispatch_job_routes_non_http_without_timeout(settings: Settings) -> None:
    """Non HTTP jobs are dispatched without HTTP timeout wrapper."""
    # ARRANGE
    runner = WorkerRunner(settings=settings, session_factory=cast(async_sessionmaker[AsyncSession], object()))
    job = make_job_row(kind="handler")

    # ACT
    result = await runner._dispatch_job(cast(JobDispatcher, SuccessDispatcher()), job)

    # ASSERT
    assert result.outcome == "succeeded"
    assert result.duration_ms == 7


async def test_dispatch_job_returns_timeout_result(settings: Settings) -> None:
    """HTTP dispatch timeout returns timeout result."""
    # ARRANGE
    runner = WorkerRunner(settings=settings, session_factory=cast(async_sessionmaker[AsyncSession], object()))
    dispatcher = BlockingDispatcher()
    job = make_job_row(kind="http", timeout_seconds=-1)

    # ACT
    result = await runner._dispatch_job(cast(JobDispatcher, dispatcher), job)

    # ASSERT
    assert result.outcome == "timeout"
    assert result.error_type == "TimeoutError"
    assert result.error_message == "HTTP job timed out"


async def test_finish_heartbeat_cancels_pending_task(settings: Settings) -> None:
    """Pending heartbeat cancellation keeps lease ownership."""

    # ARRANGE
    async def wait_forever() -> bool:
        await asyncio.sleep(30)
        return False

    runner = WorkerRunner(settings=settings, session_factory=cast(async_sessionmaker[AsyncSession], object()))
    task = asyncio.create_task(wait_forever())

    # ACT
    lease_owned = await runner._finish_heartbeat(task)

    # ASSERT
    assert lease_owned is True
    assert task.cancelled() is True


async def test_await_cancelled_logs_unexpected_task_error(settings: Settings) -> None:
    """Await cancelled helper logs non cancellation errors."""

    # ARRANGE
    async def fail_task() -> None:
        raise RuntimeError("cleanup boom")

    runner = WorkerRunner(settings=settings, session_factory=cast(async_sessionmaker[AsyncSession], object()))
    logger = FakeLogger()
    runner._logger = cast(Any, logger)
    task = asyncio.create_task(fail_task())
    await asyncio.sleep(0)

    # ACT
    await runner._await_cancelled(task)

    # ASSERT
    assert {
        "event": "worker_cancelled_task_failed",
        "error_type": "RuntimeError",
        "error_message": "cleanup boom",
    } in logger.events


async def test_shutdown_inflight_cancels_pending_task(settings: Settings) -> None:
    """Shutdown cancels pending in-flight tasks after grace timeout."""

    # ARRANGE
    async def wait_forever() -> None:
        await asyncio.sleep(30)

    runner_settings = settings.model_copy(update={"worker_shutdown_grace_seconds": 0})
    runner = WorkerRunner(settings=runner_settings, session_factory=cast(async_sessionmaker[AsyncSession], object()))
    task = asyncio.create_task(wait_forever())
    runner._inflight[task] = make_job_row()

    # ACT
    await runner._shutdown_inflight()

    # ASSERT
    assert task.cancelled() is True
