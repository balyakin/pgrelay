"""Job repository."""

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Select, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.db.models import PgRelayJob
from pgrelay.errors import RepositoryError
from pgrelay.repositories.job_rows import JobListResult, JobRow, PurgeResult, row_from_mapping, row_from_model
from pgrelay.repositories.job_sql import claim_jobs_statement, insert_job_statement, lock_claim_queues_statement
from pgrelay.repositories.job_state import JobStateRepositoryMixin
from pgrelay.repositories.queues import QueueRepository

__all__ = ["JobListResult", "JobRepository", "JobRow", "PurgeResult"]


class JobRepository(JobStateRepositoryMixin):
    """PostgreSQL job repository."""

    def __init__(self, queue_repository: QueueRepository | None = None) -> None:
        """Initialize the repository."""
        self.queue_repository = queue_repository or QueueRepository()

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
        """Insert a job and return idempotent duplicates as existing rows."""
        try:
            await self.queue_repository.upsert_queue(session, queue_name=queue_name)
            result = await session.execute(
                insert_job_statement(),
                {
                    "queue_name": queue_name,
                    "kind": kind,
                    "name": name,
                    "payload": payload,
                    "headers": headers,
                    "metadata": metadata,
                    "priority": priority,
                    "max_attempts": max_attempts,
                    "timeout_seconds": timeout_seconds,
                    "available_at": available_at,
                    "idempotency_key": idempotency_key,
                    "dedupe_key": dedupe_key,
                    "trace_id": trace_id,
                    "replayed_from_job_id": replayed_from_job_id,
                },
            )
            inserted = result.mappings().one_or_none()
            if inserted is not None:
                return row_from_mapping(inserted), True
            if idempotency_key is None:
                raise RepositoryError("Insert without idempotency key returned no row")
            existing = await self._get_by_idempotency_key(session, queue_name, idempotency_key)
            if existing is None:
                raise RepositoryError("Idempotent job conflict did not return an existing row")
            return existing, False
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Job insert failed") from exc

    async def _get_by_idempotency_key(
        self,
        session: AsyncSession,
        queue_name: str,
        idempotency_key: str,
    ) -> JobRow | None:
        result = await session.execute(
            select(PgRelayJob).where(
                PgRelayJob.queue_name == queue_name,
                PgRelayJob.idempotency_key == idempotency_key,
            )
        )
        model = result.scalar_one_or_none()
        return None if model is None else row_from_model(model)

    async def get_job(self, session: AsyncSession, job_id: UUID) -> JobRow | None:
        """Return a job by id."""
        try:
            result = await session.execute(select(PgRelayJob).where(PgRelayJob.id == job_id))
            model = result.scalar_one_or_none()
            return None if model is None else row_from_model(model)
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Job lookup failed") from exc

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
        """List jobs with optional filters."""
        try:
            statement = self._filtered_select(status, queue_name, kind, name, dedupe_key)
            count_statement = select(func.count()).select_from(statement.subquery())
            statement = statement.order_by(PgRelayJob.created_at.desc()).limit(limit).offset(offset)
            result = await session.execute(statement)
            total = int(await session.scalar(count_statement)) if include_total else None
            return JobListResult([row_from_model(model) for model in result.scalars().all()], total)
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Job list failed") from exc

    def _filtered_select(
        self,
        status: str | None,
        queue_name: str | None,
        kind: str | None,
        name: str | None,
        dedupe_key: str | None,
    ) -> Select[tuple[PgRelayJob]]:
        """Build a filtered job select statement."""
        statement = select(PgRelayJob)
        if status is not None:
            statement = statement.where(PgRelayJob.status == status)
        if queue_name is not None:
            statement = statement.where(PgRelayJob.queue_name == queue_name)
        if kind is not None:
            statement = statement.where(PgRelayJob.kind == kind)
        if name is not None:
            statement = statement.where(PgRelayJob.name == name)
        if dedupe_key is not None:
            statement = statement.where(PgRelayJob.dedupe_key == dedupe_key)
        return statement

    async def claim_jobs(
        self,
        session: AsyncSession,
        *,
        worker_id: str,
        queue_names: Sequence[str],
        batch_size: int,
        lease_seconds: int,
    ) -> list[JobRow]:
        """Claim due jobs using SKIP LOCKED and queue concurrency slots."""
        try:
            queue_names_list = list(queue_names)
            await session.execute(lock_claim_queues_statement(), {"queue_names": queue_names_list})
            result = await session.execute(
                claim_jobs_statement(),
                {
                    "worker_id": worker_id,
                    "queue_names": queue_names_list,
                    "batch_size": batch_size,
                    "lease_seconds": lease_seconds,
                },
            )
            return [row_from_mapping(row) for row in result.mappings().all()]
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Job claim failed") from exc

    async def replay_job(self, session: AsyncSession, *, source: JobRow, idempotency_key: str) -> tuple[JobRow, bool]:
        """Create a pending replay job."""
        return await self.insert_job(
            session,
            queue_name=source.queue_name,
            kind=source.kind,
            name=source.name,
            payload=source.payload,
            headers=source.headers,
            metadata=source.metadata,
            priority=source.priority,
            max_attempts=source.max_attempts,
            timeout_seconds=source.timeout_seconds,
            available_at=None,
            idempotency_key=idempotency_key,
            dedupe_key=source.dedupe_key,
            trace_id=source.trace_id,
            replayed_from_job_id=source.id,
        )
