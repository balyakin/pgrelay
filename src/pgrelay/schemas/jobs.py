"""Job response schemas."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class JobResponse(BaseModel):
    """Detailed job response including payload."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    queue_name: str
    kind: Literal["http", "handler"]
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


class JobListItem(BaseModel):
    """Job list item that excludes payload data."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    queue_name: str
    kind: Literal["http", "handler"]
    name: str
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


class JobListResponse(BaseModel):
    """Paginated job list response."""

    items: list[JobListItem]
    total: int | None
    limit: int = Field(ge=1, le=200)
    offset: int = Field(ge=0)


class AttemptResponse(BaseModel):
    """Job attempt response."""

    model_config = ConfigDict(from_attributes=True)

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


class ReplayJobRequest(BaseModel):
    """Replay request."""

    force: bool = False


class ReplayJobResponse(BaseModel):
    """Replay response."""

    new_job_id: UUID
    source_job_id: UUID
    status: str
    replayed_at: datetime


class CancelJobResponse(BaseModel):
    """Cancel response."""

    job_id: UUID
    status: str
    cancelled_at: datetime
