"""Worker endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.api.dependencies import get_session, get_worker_service, require_auth
from pgrelay.schemas.workers import WorkerResponse
from pgrelay.services.workers import WorkerService

router = APIRouter(prefix="/v1/workers", tags=["workers"], dependencies=[Depends(require_auth)])
SESSION_DEPENDENCY = Depends(get_session)
WORKER_SERVICE_DEPENDENCY = Depends(get_worker_service)


@router.get("", response_model=list[WorkerResponse])
async def list_workers(
    session: AsyncSession = SESSION_DEPENDENCY,
    service: WorkerService = WORKER_SERVICE_DEPENDENCY,
) -> list[WorkerResponse]:
    """List worker heartbeats."""
    return await service.list_workers(session)
