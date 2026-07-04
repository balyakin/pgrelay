"""Public PgRelay SDK client."""

from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.config.settings import Settings, load_settings
from pgrelay.repositories.jobs import JobRepository
from pgrelay.schemas.enqueue import EnqueueJobRequest, HttpJobPayload
from pgrelay.sdk.result import EnqueueResult
from pgrelay.services.enqueue import EnqueueService


class PgRelayClient:
    """Client for enqueuing PgRelay jobs inside an external transaction."""

    def __init__(self, settings: Settings, service: EnqueueService | None = None) -> None:
        """Initialize the SDK client."""
        self.settings = settings
        self.service = service or EnqueueService(JobRepository(), settings)

    @classmethod
    def from_env(cls) -> "PgRelayClient":
        """Create a client from PGRELAY environment settings."""
        settings = load_settings()
        settings.validate_runtime()
        return cls(settings)

    async def enqueue_handler(
        self,
        *,
        session: AsyncSession,
        name: str,
        payload: dict[str, Any],
        queue_name: str = "default",
        metadata: dict[str, Any] | None = None,
        priority: int = 0,
        max_attempts: int = 10,
        timeout_seconds: int = 30,
        available_at: datetime | None = None,
        idempotency_key: str | None = None,
        dedupe_key: str | None = None,
        trace_id: str | None = None,
    ) -> EnqueueResult:
        """Enqueue a Python handler job without committing the session."""
        request = EnqueueJobRequest(
            queue_name=queue_name,
            kind="handler",
            name=name,
            payload=payload,
            metadata=metadata or {},
            priority=priority,
            max_attempts=max_attempts,
            timeout_seconds=timeout_seconds,
            available_at=available_at,
            idempotency_key=idempotency_key,
            dedupe_key=dedupe_key,
            trace_id=trace_id,
        )
        return await self.service.enqueue(session, request)

    async def enqueue_http(
        self,
        *,
        session: AsyncSession,
        name: str,
        url: str,
        method: str = "POST",
        json_body: dict[str, Any] | list[Any] | None = None,
        body_base64: str | None = None,
        http_headers: dict[str, str] | None = None,
        queue_name: str = "default",
        metadata: dict[str, Any] | None = None,
        priority: int = 0,
        max_attempts: int = 10,
        timeout_seconds: int = 30,
        available_at: datetime | None = None,
        idempotency_key: str | None = None,
        dedupe_key: str | None = None,
        trace_id: str | None = None,
    ) -> EnqueueResult:
        """Enqueue an HTTP job without committing the session."""
        payload = HttpJobPayload.model_validate(
            {
                "method": method,
                "url": url,
                "headers": http_headers or {},
                "json": json_body,
                "body_base64": body_base64,
            }
        )
        request = EnqueueJobRequest(
            queue_name=queue_name,
            kind="http",
            name=name,
            payload=payload.model_dump(by_alias=True, exclude_none=True),
            metadata=metadata or {},
            priority=priority,
            max_attempts=max_attempts,
            timeout_seconds=timeout_seconds,
            available_at=available_at,
            idempotency_key=idempotency_key,
            dedupe_key=dedupe_key,
            trace_id=trace_id,
        )
        return await self.service.enqueue(session, request)
