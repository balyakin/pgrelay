"""Worker admin use cases."""

from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.repositories.protocols import WorkerRepositoryProtocol
from pgrelay.schemas.workers import WorkerResponse


class WorkerService:
    """Worker service."""

    def __init__(self, repository: WorkerRepositoryProtocol) -> None:
        """Initialize the service."""
        self.repository = repository

    async def list_workers(self, session: AsyncSession) -> list[WorkerResponse]:
        """List workers."""
        rows = await self.repository.list_workers(session)
        return [WorkerResponse.model_validate(row, from_attributes=True) for row in rows]
