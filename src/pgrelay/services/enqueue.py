"""Enqueue use case shared by SDK and API."""

from typing import Any

from pydantic import ValidationError as PydanticValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.config.settings import Settings
from pgrelay.errors import ValidationError
from pgrelay.repositories.protocols import JobRepositoryProtocol
from pgrelay.schemas.enqueue import EnqueueJobRequest, HttpJobPayload
from pgrelay.sdk.result import EnqueueResult
from pgrelay.utils.json import json_size_bytes
from pgrelay.utils.validation import validate_allowed_host, validate_headers, validate_queue_name, validate_url_syntax


class EnqueueService:
    """Validate and insert PgRelay jobs."""

    def __init__(self, repository: JobRepositoryProtocol, settings: Settings) -> None:
        """Initialize the service."""
        self.repository = repository
        self.settings = settings

    async def enqueue(self, session: AsyncSession, request: EnqueueJobRequest | dict[str, Any]) -> EnqueueResult:
        """Validate and enqueue a job without committing the session."""
        try:
            validated = request if isinstance(request, EnqueueJobRequest) else EnqueueJobRequest.model_validate(request)
        except PydanticValidationError as exc:
            raise ValidationError(str(exc)) from exc
        validate_queue_name(validated.queue_name)
        self._validate_size("payload", validated.payload, self.settings.max_payload_bytes)
        self._validate_size("headers", validated.headers, self.settings.max_headers_bytes)
        self._validate_size("metadata", validated.metadata, self.settings.max_metadata_bytes)
        if validated.timeout_seconds > self.settings.http_max_timeout_seconds:
            raise ValidationError("timeout_seconds exceeds configured maximum")
        payload = validated.payload
        if validated.kind == "http":
            payload = self._validate_http_payload(payload)
        row, created = await self.repository.insert_job(
            session,
            queue_name=validated.queue_name,
            kind=validated.kind,
            name=validated.name,
            payload=payload,
            headers=validated.headers,
            metadata=validated.metadata,
            priority=validated.priority,
            max_attempts=validated.max_attempts,
            timeout_seconds=validated.timeout_seconds,
            available_at=validated.available_at,
            idempotency_key=validated.idempotency_key,
            dedupe_key=validated.dedupe_key,
            trace_id=validated.trace_id,
            replayed_from_job_id=None,
        )
        return EnqueueResult(
            job_id=row.id,
            created=created,
            queue_name=row.queue_name,
            status=row.status,
            idempotency_key=row.idempotency_key,
        )

    def _validate_http_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            model = HttpJobPayload.model_validate(payload)
        except PydanticValidationError as exc:
            raise ValidationError(str(exc)) from exc
        validate_headers(model.headers)
        hostname = validate_url_syntax(model.url)
        validate_allowed_host(hostname, self.settings.get_allowed_hosts())
        return model.model_dump(by_alias=True, exclude_none=True)

    def _validate_size(self, name: str, value: Any, maximum: int) -> None:
        size = json_size_bytes(value)
        if size > maximum:
            raise ValidationError(f"{name} exceeds maximum JSON size")
