"""Stats schemas."""

from pydantic import BaseModel, Field


class QueueDepthItem(BaseModel):
    """Queue depth for one queue and status."""

    queue_name: str
    status: str
    count: int = Field(ge=0)


class StatsResponse(BaseModel):
    """Admin stats response."""

    counts: dict[str, int]
    queue_depth: list[QueueDepthItem]
    oldest_pending_age_seconds: float | None
    approximate: bool
