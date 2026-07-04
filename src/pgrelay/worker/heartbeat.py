"""Lease heartbeat loop."""

import asyncio
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.repositories.protocols import JobRepositoryProtocol


async def run_lease_heartbeat(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    repository: JobRepositoryProtocol,
    job_id: UUID,
    worker_id: str,
    lease_seconds: int,
    execution_task: asyncio.Task[object],
) -> bool:
    """Extend a job lease until execution completes or ownership is lost."""
    interval = max(1, lease_seconds // 3)
    while not execution_task.done():
        async with session_factory() as session:
            extended = await repository.extend_lease(
                session,
                job_id=job_id,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
            )
            await session.commit()
        if not extended:
            execution_task.cancel()
            return False
        await asyncio.sleep(interval)
    return True
