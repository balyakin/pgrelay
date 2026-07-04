"""SDK result contracts."""

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    """Result returned by enqueue SDK and service operations."""

    job_id: UUID
    created: bool
    queue_name: str
    status: str
    idempotency_key: str | None
