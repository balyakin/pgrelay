"""Schemas for enqueue contracts."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class HttpJobPayload(BaseModel):
    """Payload for HTTP jobs."""

    model_config = ConfigDict(populate_by_name=True)

    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    url: str = Field(min_length=1, max_length=2048)
    headers: dict[str, str] = Field(default_factory=dict)
    json_body: dict[str, Any] | list[Any] | None = Field(default=None, alias="json")
    body_base64: str | None = Field(default=None, max_length=10485760)

    @model_validator(mode="after")
    def validate_body_choice(self) -> "HttpJobPayload":
        """Ensure exactly one body representation at most is used."""
        if self.json_body is not None and self.body_base64 is not None:
            raise ValueError("Only one of json and body_base64 may be provided")
        return self


class EnqueueJobRequest(BaseModel):
    """Validated enqueue request for SDK and API."""

    queue_name: str = Field(default="default", min_length=1, max_length=128)
    kind: Literal["http", "handler"]
    name: str = Field(min_length=1, max_length=256)
    payload: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=-1000, le=1000)
    max_attempts: int = Field(default=10, ge=1, le=100)
    timeout_seconds: int = Field(default=30, ge=1, le=3600)
    available_at: datetime | None = None
    idempotency_key: str | None = Field(default=None, max_length=512)
    dedupe_key: str | None = Field(default=None, max_length=512)
    trace_id: str | None = Field(default=None, max_length=512)

    @field_validator("queue_name")
    @classmethod
    def validate_queue_name(cls, value: str) -> str:
        """Validate queue names at schema boundary."""
        from pgrelay.utils.validation import validate_queue_name

        validate_queue_name(value)
        return value
