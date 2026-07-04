"""Attempt repository."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.db.models import PgRelayAttempt
from pgrelay.errors import RepositoryError


@dataclass(frozen=True, slots=True)
class AttemptRow:
    """Attempt repository row."""

    id: UUID
    job_id: UUID
    attempt_number: int
    worker_id: str
    status: str
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    error_type: str | None
    error_message: str | None
    response_status: int | None
    response_body_preview: str | None


def _to_row(model: PgRelayAttempt) -> AttemptRow:
    return AttemptRow(
        id=model.id,
        job_id=model.job_id,
        attempt_number=model.attempt_number,
        worker_id=model.worker_id,
        status=model.status,
        started_at=model.started_at,
        finished_at=model.finished_at,
        duration_ms=model.duration_ms,
        error_type=model.error_type,
        error_message=model.error_message,
        response_status=model.response_status,
        response_body_preview=model.response_body_preview,
    )


class AttemptRepository:
    """PostgreSQL attempt repository."""

    async def list_by_job_id(self, session: AsyncSession, *, job_id: UUID) -> list[AttemptRow]:
        """List attempts for a job ordered by start time."""
        try:
            statement = (
                select(PgRelayAttempt).where(PgRelayAttempt.job_id == job_id).order_by(PgRelayAttempt.started_at)
            )
            result = await session.execute(statement)
            return [_to_row(model) for model in result.scalars().all()]
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Attempt list failed") from exc
