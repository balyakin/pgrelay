"""Job admin use cases."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.constants import JOB_STATUS_CANCELLED, JOB_STATUS_DEAD_LETTER, JOB_STATUS_PENDING, JOB_STATUS_SUCCEEDED
from pgrelay.errors import JobNotFoundError, JobStateConflictError
from pgrelay.repositories.protocols import AttemptRepositoryProtocol, JobRepositoryProtocol
from pgrelay.schemas.jobs import (
    AttemptResponse,
    CancelJobResponse,
    JobListItem,
    JobListResponse,
    JobResponse,
    ReplayJobResponse,
)


class JobService:
    """Admin job service."""

    def __init__(self, jobs: JobRepositoryProtocol, attempts: AttemptRepositoryProtocol) -> None:
        """Initialize the service."""
        self.jobs = jobs
        self.attempts = attempts

    async def list_jobs(
        self,
        session: AsyncSession,
        *,
        status: str | None,
        queue_name: str | None,
        kind: str | None,
        name: str | None,
        dedupe_key: str | None,
        limit: int,
        offset: int,
        include_total: bool,
    ) -> JobListResponse:
        """Return filtered jobs without payload fields."""
        result = await self.jobs.list_jobs(
            session,
            status=status,
            queue_name=queue_name,
            kind=kind,
            name=name,
            dedupe_key=dedupe_key,
            limit=limit,
            offset=offset,
            include_total=include_total,
        )
        return JobListResponse(
            items=[JobListItem.model_validate(item, from_attributes=True) for item in result.items],
            total=result.total,
            limit=limit,
            offset=offset,
        )

    async def detail(self, session: AsyncSession, *, job_id: UUID) -> JobResponse:
        """Return a job detail response."""
        row = await self.jobs.get_job(session, job_id)
        if row is None:
            raise JobNotFoundError("Job not found")
        return JobResponse.model_validate(row, from_attributes=True)

    async def attempts_for_job(self, session: AsyncSession, *, job_id: UUID) -> list[AttemptResponse]:
        """Return attempts for a job."""
        row = await self.jobs.get_job(session, job_id)
        if row is None:
            raise JobNotFoundError("Job not found")
        attempts = await self.attempts.list_by_job_id(session, job_id=job_id)
        return [AttemptResponse.model_validate(item, from_attributes=True) for item in attempts]

    async def replay(self, session: AsyncSession, *, job_id: UUID, force: bool) -> ReplayJobResponse:
        """Replay a dead-letter or cancelled job."""
        source = await self.jobs.get_job(session, job_id)
        if source is None:
            raise JobNotFoundError("Job not found")
        allowed = source.status in {JOB_STATUS_DEAD_LETTER, JOB_STATUS_CANCELLED}
        if source.status == JOB_STATUS_SUCCEEDED and force:
            allowed = True
        if not allowed:
            raise JobStateConflictError("Job status cannot be replayed")
        replay, _created = await self.jobs.replay_job(session, source=source, idempotency_key=f"replay:{job_id}")
        return ReplayJobResponse(
            new_job_id=replay.id,
            source_job_id=source.id,
            status=replay.status,
            replayed_at=replay.created_at,
        )

    async def cancel(self, session: AsyncSession, *, job_id: UUID) -> CancelJobResponse:
        """Cancel a pending job."""
        row = await self.jobs.get_job(session, job_id)
        if row is None:
            raise JobNotFoundError("Job not found")
        if row.status != JOB_STATUS_PENDING:
            raise JobStateConflictError("Only pending jobs can be cancelled")
        cancelled = await self.jobs.cancel_pending_job(session, job_id=job_id)
        if cancelled is None:
            raise JobStateConflictError("Only pending jobs can be cancelled")
        if cancelled.completed_at is None:
            raise JobStateConflictError("Cancelled job has no completion timestamp")
        return CancelJobResponse(job_id=cancelled.id, status=cancelled.status, cancelled_at=cancelled.completed_at)
