"""Common API error schemas."""

from typing import Any

from pydantic import BaseModel, Field


class ApiError(BaseModel):
    """Structured API error body."""

    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    details: dict[str, Any] | None = None


class ApiErrorResponse(BaseModel):
    """Common API error response envelope."""

    error: ApiError
