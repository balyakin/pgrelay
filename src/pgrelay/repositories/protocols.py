"""Repository protocol contracts."""

from collections.abc import Sequence
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.repositories.attempts import AttemptRow
from pgrelay.repositories.jobs import JobListResult, JobRow, PurgeResult
from pgrelay.repositories.queues import QueueRow
from pgrelay.repositories.stats import QueueDepthRow, StatsCountsRow
from pgrelay.repositories.workers import WorkerRow


class JobRepositoryProtocol(Protocol):
    """Job repository contract."""

    async def insert_job(
        self,
        session: AsyncSession,
        *,
        queue_name: str,
        kind: str,
        name: str,
        payload: dict[str, Any],
        headers: dict[str, Any],
        metadata: dict[str, Any],
        priority: int,
        max_attempts: int,
        timeout_seconds: int,
        available_at: datetime | None,
        idempotency_key: str | None,
        dedupe_key: str | None,
        trace_id: str | None,
        replayed_from_job_id: UUID | None,
    ) -> tuple[JobRow, bool]:
        """Insert a job or return an existing idempotent job."""

    async def get_job(self, session: AsyncSession, job_id: UUID) -> JobRow | None:
        """Return a job by id."""

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
    ) -> JobListResult:
        """List jobs."""

    async def claim_jobs(
        self,
        session: AsyncSession,
        *,
        worker_id: str,
        queue_names: Sequence[str],
        batch_size: int,
        lease_seconds: int,
    ) -> list[JobRow]:
        """Claim pending jobs."""

    async def extend_lease(self, session: AsyncSession, *, job_id: UUID, worker_id: str, lease_seconds: int) -> bool:
        """Extend an active lease."""

    async def return_leased_job_to_pending(self, session: AsyncSession, *, job_id: UUID, worker_id: str) -> bool:
        """Return an owned leased job to pending."""

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
        """Mark a leased job succeeded."""

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
        """Mark a leased job failed or retryable."""

    async def recover_expired_leases(self, session: AsyncSession, *, batch_size: int) -> int:
        """Recover expired leases."""

    async def replay_job(self, session: AsyncSession, *, source: JobRow, idempotency_key: str) -> tuple[JobRow, bool]:
        """Create a replay job."""

    async def cancel_pending_job(self, session: AsyncSession, *, job_id: UUID) -> JobRow | None:
        """Cancel a pending job."""

    async def purge_jobs(
        self, session: AsyncSession, *, succeeded_days: int, dead_letter_days: int, batch_size: int
    ) -> PurgeResult:
        """Delete old final jobs."""


class QueueRepositoryProtocol(Protocol):
    """Queue repository contract."""

    async def upsert_queue(self, session: AsyncSession, *, queue_name: str) -> QueueRow:
        """Create a queue if missing."""

    async def get_queue(self, session: AsyncSession, *, queue_name: str) -> QueueRow | None:
        """Get a queue."""

    async def list_queues(self, session: AsyncSession) -> list[QueueRow]:
        """List queues."""

    async def update_queue(
        self,
        session: AsyncSession,
        *,
        queue_name: str,
        paused: bool | None,
        concurrency_limit: int | None,
    ) -> QueueRow:
        """Update queue settings."""


class AttemptRepositoryProtocol(Protocol):
    """Attempt repository contract."""

    async def list_by_job_id(self, session: AsyncSession, *, job_id: UUID) -> list[AttemptRow]:
        """List attempts for a job."""


class WorkerRepositoryProtocol(Protocol):
    """Worker repository contract."""

    async def register_worker(
        self, session: AsyncSession, *, worker_id: str, queues: Sequence[str], hostname: str
    ) -> WorkerRow:
        """Register or refresh a worker."""

    async def heartbeat(self, session: AsyncSession, *, worker_id: str) -> bool:
        """Update worker heartbeat."""

    async def list_workers(self, session: AsyncSession) -> list[WorkerRow]:
        """List workers with alive state."""


class StatsRepositoryProtocol(Protocol):
    """Stats repository contract."""

    async def counts_by_status(self, session: AsyncSession, *, approximate: bool) -> list[StatsCountsRow]:
        """Return counts by status."""

    async def queue_depth(self, session: AsyncSession, *, approximate: bool) -> list[QueueDepthRow]:
        """Return queue depth."""

    async def oldest_pending_age_seconds(self, session: AsyncSession) -> float | None:
        """Return age of the oldest pending job."""
