"""Job repository row contracts and mappers."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.engine import RowMapping

from pgrelay.db.models import PgRelayJob


@dataclass(frozen=True, slots=True)
class JobRow:
    """Job repository row."""

    id: UUID
    queue_name: str
    kind: str
    name: str
    payload: dict[str, Any]
    headers: dict[str, Any]
    metadata: dict[str, Any]
    status: str
    priority: int
    max_attempts: int
    attempt_count: int
    available_at: datetime
    timeout_seconds: int
    idempotency_key: str | None
    dedupe_key: str | None
    replayed_from_job_id: UUID | None
    locked_by: str | None
    locked_until: datetime | None
    last_error_type: str | None
    last_error_message: str | None
    last_response_status: int | None
    trace_id: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None


@dataclass(frozen=True, slots=True)
class JobListResult:
    """Repository result for job list queries."""

    items: list[JobRow]
    total: int | None


@dataclass(frozen=True, slots=True)
class PurgeResult:
    """Deleted row counts for purge."""

    succeeded: int
    dead_letter: int
    cancelled: int

    @property
    def total(self) -> int:
        """Return total deleted jobs."""
        return self.succeeded + self.dead_letter + self.cancelled


def row_from_model(model: PgRelayJob) -> JobRow:
    """Map an ORM model to a JobRow."""
    return JobRow(
        id=model.id,
        queue_name=model.queue_name,
        kind=model.kind,
        name=model.name,
        payload=model.payload,
        headers=model.headers,
        metadata=model.metadata_,
        status=model.status,
        priority=model.priority,
        max_attempts=model.max_attempts,
        attempt_count=model.attempt_count,
        available_at=model.available_at,
        timeout_seconds=model.timeout_seconds,
        idempotency_key=model.idempotency_key,
        dedupe_key=model.dedupe_key,
        replayed_from_job_id=model.replayed_from_job_id,
        locked_by=model.locked_by,
        locked_until=model.locked_until,
        last_error_type=model.last_error_type,
        last_error_message=model.last_error_message,
        last_response_status=model.last_response_status,
        trace_id=model.trace_id,
        created_at=model.created_at,
        updated_at=model.updated_at,
        completed_at=model.completed_at,
    )


def row_from_mapping(row: RowMapping) -> JobRow:
    """Map a SQLAlchemy row mapping to a JobRow."""
    return JobRow(
        id=row["id"],
        queue_name=row["queue_name"],
        kind=row["kind"],
        name=row["name"],
        payload=row["payload"],
        headers=row["headers"],
        metadata=row["metadata"],
        status=row["status"],
        priority=row["priority"],
        max_attempts=row["max_attempts"],
        attempt_count=row["attempt_count"],
        available_at=row["available_at"],
        timeout_seconds=row["timeout_seconds"],
        idempotency_key=row["idempotency_key"],
        dedupe_key=row["dedupe_key"],
        replayed_from_job_id=row["replayed_from_job_id"],
        locked_by=row["locked_by"],
        locked_until=row["locked_until"],
        last_error_type=row["last_error_type"],
        last_error_message=row["last_error_message"],
        last_response_status=row["last_response_status"],
        trace_id=row["trace_id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row["completed_at"],
    )
