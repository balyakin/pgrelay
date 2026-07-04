"""Queue schemas."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class QueueResponse(BaseModel):
    """Queue response."""

    model_config = ConfigDict(from_attributes=True)

    name: str
    paused: bool
    concurrency_limit: int
    created_at: datetime
    updated_at: datetime


class QueueUpdateRequest(BaseModel):
    """Queue update request."""

    paused: bool | None = None
    concurrency_limit: int | None = Field(default=None, ge=1, le=256)
