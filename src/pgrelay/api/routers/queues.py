"""Queue admin endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.api.dependencies import get_queue_service, get_session, require_auth, require_write_auth
from pgrelay.schemas.queues import QueueResponse, QueueUpdateRequest
from pgrelay.services.queues import QueueService

router = APIRouter(prefix="/v1/queues", tags=["queues"], dependencies=[Depends(require_auth)])
SESSION_DEPENDENCY = Depends(get_session)
QUEUE_SERVICE_DEPENDENCY = Depends(get_queue_service)


@router.get("", response_model=list[QueueResponse])
async def list_queues(
    session: AsyncSession = SESSION_DEPENDENCY,
    service: QueueService = QUEUE_SERVICE_DEPENDENCY,
) -> list[QueueResponse]:
    """List queues."""
    return await service.list_queues(session)


@router.put("/{queue_name}", response_model=QueueResponse, dependencies=[Depends(require_write_auth)])
async def update_queue(
    queue_name: str,
    request: QueueUpdateRequest,
    session: AsyncSession = SESSION_DEPENDENCY,
    service: QueueService = QUEUE_SERVICE_DEPENDENCY,
) -> QueueResponse:
    """Create or update a queue."""
    return await service.upsert_or_update(session, queue_name=queue_name, request=request)


@router.post("/{queue_name}/pause", response_model=QueueResponse, dependencies=[Depends(require_write_auth)])
async def pause_queue(
    queue_name: str,
    session: AsyncSession = SESSION_DEPENDENCY,
    service: QueueService = QUEUE_SERVICE_DEPENDENCY,
) -> QueueResponse:
    """Pause a queue."""
    return await service.pause(session, queue_name=queue_name)


@router.post("/{queue_name}/resume", response_model=QueueResponse, dependencies=[Depends(require_write_auth)])
async def resume_queue(
    queue_name: str,
    session: AsyncSession = SESSION_DEPENDENCY,
    service: QueueService = QUEUE_SERVICE_DEPENDENCY,
) -> QueueResponse:
    """Resume a queue."""
    return await service.resume(session, queue_name=queue_name)
