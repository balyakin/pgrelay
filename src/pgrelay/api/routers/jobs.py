"""Job admin endpoints."""

from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.api.dependencies import get_enqueue_service, get_job_service, get_session, require_auth
from pgrelay.schemas.enqueue import EnqueueJobRequest
from pgrelay.schemas.jobs import (
    AttemptResponse,
    CancelJobResponse,
    JobListResponse,
    JobResponse,
    ReplayJobRequest,
    ReplayJobResponse,
)
from pgrelay.sdk.result import EnqueueResult
from pgrelay.services.enqueue import EnqueueService
from pgrelay.services.jobs import JobService

router = APIRouter(prefix="/v1/jobs", tags=["jobs"], dependencies=[Depends(require_auth)])
SESSION_DEPENDENCY = Depends(get_session)
ENQUEUE_SERVICE_DEPENDENCY = Depends(get_enqueue_service)
JOB_SERVICE_DEPENDENCY = Depends(get_job_service)


@router.post("", response_model=EnqueueResult, status_code=status.HTTP_201_CREATED)
async def enqueue_job(
    request: EnqueueJobRequest,
    response: Response,
    session: AsyncSession = SESSION_DEPENDENCY,
    service: EnqueueService = ENQUEUE_SERVICE_DEPENDENCY,
) -> EnqueueResult:
    """Enqueue a job through the admin API."""
    result = await service.enqueue(session, request)
    if not result.created:
        response.status_code = status.HTTP_200_OK
    return result


@router.get("", response_model=JobListResponse)
async def list_jobs(
    status_filter: str | None = Query(default=None, alias="status"),
    queue_name: str | None = None,
    kind: str | None = None,
    name: str | None = None,
    dedupe_key: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    include_total: bool = False,
    session: AsyncSession = SESSION_DEPENDENCY,
    service: JobService = JOB_SERVICE_DEPENDENCY,
) -> JobListResponse:
    """List jobs without payload fields."""
    return await service.list_jobs(
        session,
        status=status_filter,
        queue_name=queue_name,
        kind=kind,
        name=name,
        dedupe_key=dedupe_key,
        limit=limit,
        offset=offset,
        include_total=include_total,
    )


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: UUID,
    session: AsyncSession = SESSION_DEPENDENCY,
    service: JobService = JOB_SERVICE_DEPENDENCY,
) -> JobResponse:
    """Return job detail."""
    return await service.detail(session, job_id=job_id)


@router.get("/{job_id}/attempts", response_model=list[AttemptResponse])
async def get_attempts(
    job_id: UUID,
    session: AsyncSession = SESSION_DEPENDENCY,
    service: JobService = JOB_SERVICE_DEPENDENCY,
) -> list[AttemptResponse]:
    """Return attempts for a job."""
    return await service.attempts_for_job(session, job_id=job_id)


@router.post("/{job_id}/replay", response_model=ReplayJobResponse, status_code=status.HTTP_201_CREATED)
async def replay_job(
    job_id: UUID,
    request: ReplayJobRequest,
    session: AsyncSession = SESSION_DEPENDENCY,
    service: JobService = JOB_SERVICE_DEPENDENCY,
) -> ReplayJobResponse:
    """Replay a job."""
    return await service.replay(session, job_id=job_id, force=request.force)


@router.post("/{job_id}/cancel", response_model=CancelJobResponse)
async def cancel_job(
    job_id: UUID,
    session: AsyncSession = SESSION_DEPENDENCY,
    service: JobService = JOB_SERVICE_DEPENDENCY,
) -> CancelJobResponse:
    """Cancel a pending job."""
    return await service.cancel(session, job_id=job_id)
