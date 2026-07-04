"""Queue admin use cases."""

from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.errors import QueueNotFoundError
from pgrelay.repositories.protocols import QueueRepositoryProtocol
from pgrelay.schemas.queues import QueueResponse, QueueUpdateRequest
from pgrelay.utils.validation import validate_queue_name


class QueueService:
    """Admin queue service."""

    def __init__(self, repository: QueueRepositoryProtocol) -> None:
        """Initialize the service."""
        self.repository = repository

    async def list_queues(self, session: AsyncSession) -> list[QueueResponse]:
        """List queues."""
        rows = await self.repository.list_queues(session)
        return [QueueResponse.model_validate(row, from_attributes=True) for row in rows]

    async def upsert_or_update(
        self,
        session: AsyncSession,
        *,
        queue_name: str,
        request: QueueUpdateRequest,
    ) -> QueueResponse:
        """Create or update a queue."""
        validate_queue_name(queue_name)
        row = await self.repository.update_queue(
            session,
            queue_name=queue_name,
            paused=request.paused,
            concurrency_limit=request.concurrency_limit,
        )
        return QueueResponse.model_validate(row, from_attributes=True)

    async def pause(self, session: AsyncSession, *, queue_name: str) -> QueueResponse:
        """Pause a queue."""
        validate_queue_name(queue_name)
        row = await self.repository.update_queue(session, queue_name=queue_name, paused=True, concurrency_limit=None)
        if row is None:
            raise QueueNotFoundError("Queue not found")
        return QueueResponse.model_validate(row, from_attributes=True)

    async def resume(self, session: AsyncSession, *, queue_name: str) -> QueueResponse:
        """Resume a queue."""
        validate_queue_name(queue_name)
        row = await self.repository.update_queue(session, queue_name=queue_name, paused=False, concurrency_limit=None)
        if row is None:
            raise QueueNotFoundError("Queue not found")
        return QueueResponse.model_validate(row, from_attributes=True)
