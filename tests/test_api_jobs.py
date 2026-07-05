"""Admin API tests."""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from uuid import UUID

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.api.app import create_app
from pgrelay.config.settings import Settings
from pgrelay.schemas.jobs import AttemptResponse
from pgrelay.security.auth import require_api_token
from pgrelay.utils.redaction import REDACTED_VALUE


@pytest.fixture()
async def api_client(settings: Settings) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Return an HTTPX ASGI client."""
    app = create_app(settings=settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


async def test_missing_token_returns_401(api_client: httpx.AsyncClient) -> None:
    """Missing token returns 401."""
    # ARRANGE
    payload = {"kind": "handler", "name": "handler", "payload": {}}

    # ACT
    response = await api_client.post("/v1/jobs", json=payload)

    # ASSERT
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


async def test_invalid_token_returns_401(api_client: httpx.AsyncClient) -> None:
    """Invalid token returns 401."""
    # ARRANGE
    payload = {"kind": "handler", "name": "handler", "payload": {}}

    # ACT
    response = await api_client.post("/v1/jobs", json=payload, headers={"Authorization": "Bearer wrong"})

    # ASSERT
    assert response.status_code == 401


async def test_non_ascii_token_returns_401(settings: Settings) -> None:
    """Non-ASCII authorization returns 401."""
    # ARRANGE
    authorization = "Bearer тест"

    # ACT
    with pytest.raises(HTTPException) as exc_info:
        await require_api_token(settings, authorization)

    # ASSERT
    assert exc_info.value.status_code == 401


async def test_valid_token_works(api_client: httpx.AsyncClient) -> None:
    """Valid token works."""
    # ARRANGE
    payload = {"kind": "handler", "name": "handler", "payload": {}}

    # ACT
    response = await api_client.post("/v1/jobs", json=payload, headers={"Authorization": "Bearer test-token"})

    # ASSERT
    assert response.status_code == 201


async def test_read_only_token_cannot_enqueue(settings: Settings) -> None:
    """Read-only token cannot enqueue jobs."""
    # ARRANGE
    settings = settings.model_copy(update={"api_read_only_auth_tokens": "read-token"})
    app = create_app(settings=settings)
    payload = {"kind": "handler", "name": "handler", "payload": {}}
    headers = {"Authorization": "Bearer read-token"}

    # ACT
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/v1/jobs", json=payload, headers=headers)
            stats = await client.get("/v1/stats", headers=headers)

    # ASSERT
    assert response.status_code == 403
    assert stats.status_code == 200


async def test_duplicate_idempotency_returns_200(api_client: httpx.AsyncClient) -> None:
    """Duplicate idempotency returns 200 and created=false."""
    # ARRANGE
    payload = {"kind": "handler", "name": "handler", "payload": {}, "idempotency_key": "api-key"}
    headers = {"Authorization": "Bearer test-token"}

    # ACT
    first = await api_client.post("/v1/jobs", json=payload, headers=headers)
    second = await api_client.post("/v1/jobs", json=payload, headers=headers)

    # ASSERT
    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["created"] is False


async def test_list_jobs_excludes_payload(api_client: httpx.AsyncClient) -> None:
    """List jobs excludes payload."""
    # ARRANGE
    headers = {"Authorization": "Bearer test-token"}
    await api_client.post(
        "/v1/jobs", json={"kind": "handler", "name": "handler", "payload": {"secret": 1}}, headers=headers
    )

    # ACT
    response = await api_client.get("/v1/jobs", headers=headers)

    # ASSERT
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert "payload" not in item
    assert "headers" not in item
    assert "metadata" not in item


async def test_detail_job_redacts_sensitive_fields(api_client: httpx.AsyncClient) -> None:
    """Detail job redacts sensitive fields."""
    # ARRANGE
    headers = {"Authorization": "Bearer test-token"}
    created = await api_client.post(
        "/v1/jobs",
        json={
            "kind": "http",
            "name": "webhook",
            "payload": {
                "method": "POST",
                "url": "https://example.com/webhook",
                "headers": {"Authorization": "Bearer target-secret", "X-Trace": "trace"},
                "json": {"card": "4111111111111111"},
            },
            "headers": {"X-Webhook-Signature": "sha256=target-secret", "X-Trace": "trace"},
            "metadata": {"tenant": "acme"},
        },
        headers=headers,
    )
    job_id = created.json()["job_id"]

    # ACT
    response = await api_client.get(f"/v1/jobs/{job_id}", headers=headers)

    # ASSERT
    assert response.status_code == 200
    body = response.json()
    assert body["payload"] == {"redacted": REDACTED_VALUE}
    assert body["headers"] == {"redacted": REDACTED_VALUE}
    assert body["metadata"] == {"redacted": REDACTED_VALUE}


def test_attempt_response_redacts_response_body_preview() -> None:
    """Attempt response redacts response body preview."""
    # ARRANGE
    now = datetime(2026, 1, 1, tzinfo=UTC)
    attempt = {
        "id": UUID("00000000-0000-0000-0000-000000000001"),
        "job_id": UUID("00000000-0000-0000-0000-000000000002"),
        "attempt_number": 1,
        "worker_id": "worker",
        "status": "failed",
        "started_at": now,
        "finished_at": now,
        "duration_ms": 10,
        "error_type": None,
        "error_message": None,
        "response_status": 500,
        "response_body_preview": '{"token":"target-secret"}',
    }

    # ACT
    response = AttemptResponse.model_validate(attempt)

    # ASSERT
    assert response.response_body_preview == REDACTED_VALUE


async def test_missing_job_returns_404(api_client: httpx.AsyncClient) -> None:
    """Missing job returns 404."""
    # ARRANGE
    headers = {"Authorization": "Bearer test-token"}

    # ACT
    response = await api_client.get("/v1/jobs/00000000-0000-0000-0000-000000000001", headers=headers)

    # ASSERT
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "job_not_found"


async def test_request_validation_error_uses_common_shape(api_client: httpx.AsyncClient) -> None:
    """Request validation errors use the common API envelope."""
    # ARRANGE
    headers = {"Authorization": "Bearer test-token"}

    # ACT
    response = await api_client.get("/v1/jobs/not-a-uuid", headers=headers)

    # ASSERT
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert isinstance(response.json()["error"]["details"]["errors"], list)


async def test_invalid_payload_returns_422(api_client: httpx.AsyncClient) -> None:
    """Invalid payload returns 422."""
    # ARRANGE
    headers = {"Authorization": "Bearer test-token"}

    # ACT
    response = await api_client.post("/v1/jobs", json={"kind": "event"}, headers=headers)

    # ASSERT
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_queue_pause_resume_stats_and_workers(
    api_client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Queue pause/resume endpoints, stats and workers work."""
    # ARRANGE
    headers = {"Authorization": "Bearer test-token"}

    # ACT
    updated = await api_client.put("/v1/queues/default", json={"concurrency_limit": 2}, headers=headers)
    paused = await api_client.post("/v1/queues/default/pause", headers=headers)
    resumed = await api_client.post("/v1/queues/default/resume", headers=headers)
    stats = await api_client.get("/v1/stats?approximate=true", headers=headers)
    workers = await api_client.get("/v1/workers", headers=headers)

    # ASSERT
    assert updated.status_code == 200
    assert paused.json()["paused"] is True
    assert resumed.json()["paused"] is False
    assert stats.json()["approximate"] is True
    assert workers.status_code == 200
