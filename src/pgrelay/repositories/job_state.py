"""Job state transition repository mixin."""

from typing import cast
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.errors import RepositoryError
from pgrelay.repositories.job_rows import JobRow, PurgeResult, row_from_mapping
from pgrelay.repositories.job_sql import (
    CANCEL_PENDING_JOB_SQL,
    COMPLETE_JOB_SQL,
    EXTEND_LEASE_SQL,
    FAIL_JOB_SQL,
    INSERT_ATTEMPT_SQL,
    PURGE_JOBS_SQL,
    RECOVER_EXPIRED_LEASES_SQL,
    RETURN_TO_PENDING_SQL,
)


class JobStateRepositoryMixin:
    """Repository methods for job state transitions."""

    async def extend_lease(self, session: AsyncSession, *, job_id: UUID, worker_id: str, lease_seconds: int) -> bool:
        """Extend a lease owned by a worker."""
        try:
            result = await session.execute(
                text(EXTEND_LEASE_SQL),
                {"job_id": job_id, "worker_id": worker_id, "lease_seconds": lease_seconds},
            )
            return cast(int, getattr(result, "rowcount", 0)) > 0
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Lease extension failed") from exc

    async def return_leased_job_to_pending(self, session: AsyncSession, *, job_id: UUID, worker_id: str) -> bool:
        """Return an owned leased job to pending without writing an attempt."""
        try:
            result = await session.execute(text(RETURN_TO_PENDING_SQL), {"job_id": job_id, "worker_id": worker_id})
            return cast(int, getattr(result, "rowcount", 0)) > 0
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Return leased job failed") from exc

    async def complete_job(
        self,
        session: AsyncSession,
        *,
        job: JobRow,
        worker_id: str,
        duration_ms: int,
        response_status: int | None,
        response_body_preview: str | None,
    ) -> bool:
        """Write a success attempt and mark the leased job succeeded."""
        try:
            result = await session.execute(
                text(COMPLETE_JOB_SQL),
                {"job_id": job.id, "worker_id": worker_id, "response_status": response_status},
            )
            updated = cast(int, getattr(result, "rowcount", 0)) > 0
            if updated:
                await self._insert_attempt(
                    session,
                    job=job,
                    worker_id=worker_id,
                    status="succeeded",
                    duration_ms=duration_ms,
                    error_type=None,
                    error_message=None,
                    response_status=response_status,
                    response_body_preview=response_body_preview,
                )
            return updated
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Job completion failed") from exc

    async def fail_job(
        self,
        session: AsyncSession,
        *,
        job: JobRow,
        worker_id: str,
        retryable: bool,
        timed_out: bool,
        retry_delay_seconds: int,
        duration_ms: int,
        error_type: str | None,
        error_message: str | None,
        response_status: int | None,
        response_body_preview: str | None,
    ) -> bool:
        """Write a failed attempt and schedule retry or dead-letter."""
        try:
            should_retry = retryable and job.attempt_count < job.max_attempts
            result = await session.execute(
                text(FAIL_JOB_SQL),
                {
                    "job_id": job.id,
                    "worker_id": worker_id,
                    "should_retry": should_retry,
                    "retry_delay_seconds": retry_delay_seconds,
                    "error_type": error_type,
                    "error_message": error_message,
                    "response_status": response_status,
                },
            )
            updated = cast(int, getattr(result, "rowcount", 0)) > 0
            if updated:
                await self._insert_attempt(
                    session,
                    job=job,
                    worker_id=worker_id,
                    status="timeout" if timed_out else "failed",
                    duration_ms=duration_ms,
                    error_type=error_type,
                    error_message=error_message,
                    response_status=response_status,
                    response_body_preview=response_body_preview,
                )
            return updated
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Job failure update failed") from exc

    async def _insert_attempt(
        self,
        session: AsyncSession,
        *,
        job: JobRow,
        worker_id: str,
        status: str,
        duration_ms: int,
        error_type: str | None,
        error_message: str | None,
        response_status: int | None,
        response_body_preview: str | None,
    ) -> None:
        await session.execute(
            text(INSERT_ATTEMPT_SQL),
            {
                "job_id": job.id,
                "attempt_number": job.attempt_count,
                "worker_id": worker_id,
                "status": status,
                "duration_ms": duration_ms,
                "error_type": error_type,
                "error_message": error_message,
                "response_status": response_status,
                "response_body_preview": response_body_preview,
            },
        )

    async def recover_expired_leases(self, session: AsyncSession, *, batch_size: int) -> int:
        """Recover expired leases and write lease-expired attempts."""
        try:
            result = await session.execute(text(RECOVER_EXPIRED_LEASES_SQL), {"batch_size": batch_size})
            return len(result.mappings().all())
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Expired lease recovery failed") from exc

    async def cancel_pending_job(self, session: AsyncSession, *, job_id: UUID) -> JobRow | None:
        """Cancel a pending job."""
        try:
            result = await session.execute(text(CANCEL_PENDING_JOB_SQL), {"job_id": job_id})
            row = result.mappings().one_or_none()
            return None if row is None else row_from_mapping(row)
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Job cancel failed") from exc

    async def purge_jobs(
        self,
        session: AsyncSession,
        *,
        succeeded_days: int,
        dead_letter_days: int,
        batch_size: int,
    ) -> PurgeResult:
        """Delete old final jobs without touching active jobs."""
        try:
            result = await session.execute(
                text(PURGE_JOBS_SQL),
                {
                    "succeeded_days": succeeded_days,
                    "dead_letter_days": dead_letter_days,
                    "batch_size": batch_size,
                },
            )
            statuses = [str(row["status"]) for row in result.mappings().all()]
            return PurgeResult(
                succeeded=statuses.count("succeeded"),
                dead_letter=statuses.count("dead_letter"),
                cancelled=statuses.count("cancelled"),
            )
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Job purge failed") from exc
