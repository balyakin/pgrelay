"""HTTP worker executor tests."""

import socket
from collections.abc import AsyncIterator

import httpx
import pytest
import respx

from pgrelay.config.settings import Settings
from pgrelay.errors import ValidationError
from pgrelay.worker.http_executor import HttpJobExecutor, create_http_client, resolve_public_address
from tests.conftest import make_job_row


class PreviewOnlyStream(httpx.AsyncByteStream):
    """Response stream that fails when executor reads past preview bytes."""

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Yield preview-sized chunks."""
        chunk_count = 0
        while chunk_count < 4:
            chunk_count += 1
            yield b"x" * 512
        raise AssertionError("response body was read past preview")


@respx.mock
async def test_http_200_completes_job(settings: Settings) -> None:
    """HTTP 200 completes job."""
    # ARRANGE
    route = respx.post("https://example.com/webhook").mock(return_value=httpx.Response(200, text="ok"))
    job = make_job_row(
        payload={"method": "POST", "url": "https://example.com/webhook", "headers": {}, "json": {"a": 1}},
        idempotency_key="idem",
    )
    async with httpx.AsyncClient(follow_redirects=False) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        result = await executor.execute(job)

    # ASSERT
    assert result.outcome == "succeeded"
    assert route.called
    assert route.calls.last.request.headers["Idempotency-Key"] == "idem"


@respx.mock
async def test_http_500_retries_job(settings: Settings) -> None:
    """HTTP 500 retries job."""
    # ARRANGE
    respx.post("https://example.com/webhook").mock(return_value=httpx.Response(500, text="bad"))
    job = make_job_row(payload={"method": "POST", "url": "https://example.com/webhook"})
    async with httpx.AsyncClient(follow_redirects=False) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        result = await executor.execute(job)

    # ASSERT
    assert result.outcome == "retryable_failure"
    assert result.response_status == 500


@respx.mock
async def test_http_400_dead_letters_job(settings: Settings) -> None:
    """HTTP 400 dead-letters job."""
    # ARRANGE
    respx.post("https://example.com/webhook").mock(return_value=httpx.Response(400, text="bad"))
    job = make_job_row(payload={"method": "POST", "url": "https://example.com/webhook"})
    async with httpx.AsyncClient(follow_redirects=False) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        result = await executor.execute(job)

    # ASSERT
    assert result.outcome == "permanent_failure"
    assert result.response_status == 400


@respx.mock
async def test_timeout_retries_job(settings: Settings) -> None:
    """Timeout retries job."""
    # ARRANGE
    respx.post("https://example.com/webhook").mock(side_effect=httpx.ReadTimeout("timeout"))
    job = make_job_row(payload={"method": "POST", "url": "https://example.com/webhook"})
    async with httpx.AsyncClient(follow_redirects=False) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        result = await executor.execute(job)

    # ASSERT
    assert result.outcome == "timeout"


@pytest.mark.parametrize(
    "error",
    [
        httpx.ReadError("read failed"),
        httpx.WriteError("write failed"),
        httpx.ProxyError("proxy failed"),
    ],
)
@respx.mock
async def test_network_errors_retry_job(settings: Settings, error: httpx.HTTPError) -> None:
    """Network failures retry job."""
    # ARRANGE
    respx.post("https://example.com/webhook").mock(side_effect=error)
    job = make_job_row(payload={"method": "POST", "url": "https://example.com/webhook"})
    async with httpx.AsyncClient(follow_redirects=False) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        result = await executor.execute(job)

    # ASSERT
    assert result.outcome == "retryable_failure"


async def test_response_preview_does_not_read_full_body(settings: Settings) -> None:
    """Response preview does not read the whole response body."""
    # ARRANGE
    transport = httpx.MockTransport(lambda _request: httpx.Response(500, stream=PreviewOnlyStream()))
    job = make_job_row(payload={"method": "POST", "url": "https://example.com/webhook"})
    async with httpx.AsyncClient(transport=transport, follow_redirects=False) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        result = await executor.execute(job)

    # ASSERT
    assert result.outcome == "retryable_failure"
    assert result.response_body_preview is not None
    assert len(result.response_body_preview) == 2000


async def test_safe_http_client_blocks_private_dns_resolution(
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Safe HTTP client blocks private DNS resolution during TCP connect."""

    # ARRANGE
    def get_private_address(*args: object, **kwargs: object) -> list[object]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 443))]

    monkeypatch.setattr(socket, "getaddrinfo", get_private_address)
    job = make_job_row(payload={"method": "POST", "url": "https://example.com/webhook"})
    async with create_http_client(settings) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        result = await executor.execute(job)

    # ASSERT
    assert result.outcome == "permanent_failure"
    assert result.error_type == "ValidationError"


async def test_public_address_resolution_rejects_blocked_addresses(monkeypatch: pytest.MonkeyPatch) -> None:
    """Public address resolution rejects blocked addresses."""

    # ARRANGE
    def get_private_address(*args: object, **kwargs: object) -> list[object]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("169.254.169.254", 80))]

    monkeypatch.setattr(socket, "getaddrinfo", get_private_address)

    # ACT
    with pytest.raises(ValidationError) as exc_info:
        await resolve_public_address("example.com", 80)

    # ASSERT
    assert exc_info.value.error_code == "validation_error"


async def test_dns_preflight_errors_retry_job(settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
    """DNS failures during target validation retry the job."""

    # ARRANGE
    def fail_getaddrinfo(*args: object, **kwargs: object) -> list[object]:
        raise socket.gaierror("temporary DNS failure")

    monkeypatch.setattr(socket, "getaddrinfo", fail_getaddrinfo)
    job = make_job_row(payload={"method": "POST", "url": "https://example.com/webhook"})
    async with create_http_client(settings) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        result = await executor.execute(job)

    # ASSERT
    assert result.outcome == "retryable_failure"
    assert result.error_type == "gaierror"


async def test_invalid_base64_body_is_permanent_failure(settings: Settings) -> None:
    """Invalid base64 body is a permanent failure."""
    # ARRANGE
    job = make_job_row(payload={"method": "POST", "url": "https://example.com/webhook", "body_base64": "not base64"})
    async with httpx.AsyncClient(follow_redirects=False) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        result = await executor.execute(job)

    # ASSERT
    assert result.outcome == "permanent_failure"
    assert result.error_type == "ValidationError"


async def test_private_network_target_blocked_without_request(settings: Settings) -> None:
    """Private network target is blocked without outgoing request."""
    # ARRANGE
    job = make_job_row(payload={"method": "POST", "url": "http://127.0.0.1/webhook"})
    async with create_http_client(settings) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        result = await executor.execute(job)

    # ASSERT
    assert result.outcome == "permanent_failure"
    assert result.error_type == "ValidationError"


@respx.mock
async def test_redirect_is_not_followed(settings: Settings) -> None:
    """Redirect is not followed."""
    # ARRANGE
    route = respx.get("https://example.com/redirect").mock(
        return_value=httpx.Response(302, headers={"Location": "https://example.com/next"})
    )
    next_route = respx.get("https://example.com/next").mock(return_value=httpx.Response(200))
    job = make_job_row(payload={"method": "GET", "url": "https://example.com/redirect"})
    async with httpx.AsyncClient(follow_redirects=False) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        result = await executor.execute(job)

    # ASSERT
    assert result.outcome == "permanent_failure"
    assert route.called
    assert not next_route.called


@respx.mock
async def test_secret_header_data_is_not_logged(settings: Settings, caplog: pytest.LogCaptureFixture) -> None:
    """Secret response/header data is not logged."""
    # ARRANGE
    route = respx.post("https://example.com/webhook").mock(return_value=httpx.Response(200, text="ok"))
    job = make_job_row(
        payload={"method": "POST", "url": "https://example.com/webhook", "headers": {"Authorization": "secret"}}
    )
    async with httpx.AsyncClient(follow_redirects=False) as client:
        executor = HttpJobExecutor(client, settings)

        # ACT
        await executor.execute(job)

    # ASSERT
    assert route.called
    assert "secret" not in caplog.text
