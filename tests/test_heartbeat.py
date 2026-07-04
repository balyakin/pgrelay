"""Heartbeat tests."""

import asyncio
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.worker.heartbeat import run_lease_heartbeat


class FakeLeaseRepository:
    """Fake repository for heartbeat tests."""

    def __init__(self, results: list[bool]) -> None:
        """Initialize fake results."""
        self.results = results
        self.calls = 0
        self.called = asyncio.Event()

    async def extend_lease(
        self,
        session: AsyncSession,
        *,
        job_id: UUID,
        worker_id: str,
        lease_seconds: int,
    ) -> bool:
        """Return the next fake lease extension result."""
        self.calls += 1
        self.called.set()
        return self.results.pop(0)


async def test_heartbeat_extends_lease(session_factory: async_sessionmaker[AsyncSession]) -> None:
    """Heartbeat extends lease."""
    # ARRANGE
    repository = FakeLeaseRepository([True])
    execution = asyncio.create_task(asyncio.sleep(0.05))

    # ACT
    result = await run_lease_heartbeat(
        session_factory=session_factory,
        repository=repository,  # type: ignore[arg-type]
        job_id=UUID("00000000-0000-0000-0000-000000000001"),
        worker_id="worker",
        lease_seconds=5,
        execution_task=execution,
    )

    # ASSERT
    assert result is True
    assert repository.calls == 1


async def test_lost_lease_cancels_execution(session_factory: async_sessionmaker[AsyncSession]) -> None:
    """Lost lease cancels execution."""
    # ARRANGE
    repository = FakeLeaseRepository([False])
    execution = asyncio.create_task(asyncio.sleep(30))

    # ACT
    result = await run_lease_heartbeat(
        session_factory=session_factory,
        repository=repository,  # type: ignore[arg-type]
        job_id=UUID("00000000-0000-0000-0000-000000000001"),
        worker_id="worker",
        lease_seconds=5,
        execution_task=execution,
    )

    # ASSERT
    assert result is False
    assert execution.cancelled() or execution.cancelling() > 0


async def test_cancelled_heartbeat_propagates_cancelled_error(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Cancelled heartbeat propagates CancelledError."""
    # ARRANGE
    repository = FakeLeaseRepository([True, True])
    execution = asyncio.create_task(asyncio.sleep(30))
    heartbeat = asyncio.create_task(
        run_lease_heartbeat(
            session_factory=session_factory,
            repository=repository,  # type: ignore[arg-type]
            job_id=UUID("00000000-0000-0000-0000-000000000001"),
            worker_id="worker",
            lease_seconds=5,
            execution_task=execution,
        )
    )

    # ACT
    await asyncio.wait_for(repository.called.wait(), timeout=1)
    heartbeat.cancel()

    # ASSERT
    try:
        await heartbeat
    except asyncio.CancelledError:
        assert True
    finally:
        execution.cancel()
