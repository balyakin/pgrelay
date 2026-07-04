"""HTTP job executor."""

import asyncio
import base64
import binascii
import socket
import time
from collections.abc import Iterable
from typing import Any, cast

import httpcore
import httpx

from pgrelay.config.settings import Settings
from pgrelay.errors import ValidationError
from pgrelay.observability.metrics import HTTP_DELIVERY_TOTAL
from pgrelay.repositories.jobs import JobRow
from pgrelay.schemas.enqueue import HttpJobPayload
from pgrelay.utils.validation import (
    is_blocked_ip_address,
    validate_allowed_host,
    validate_headers,
    validate_url_syntax,
)
from pgrelay.worker.dispatcher import ExecutorOutcome, ExecutorResult

RETRYABLE_HTTP_STATUSES = {408, 409, 425, 429}
PERMANENT_HTTP_STATUSES = {400, 401, 403, 404, 405, 410, 413, 415, 422}
RESPONSE_PREVIEW_BYTES = 2000
RESPONSE_CHUNK_BYTES = 512


class SafeAsyncNetworkBackend(httpcore.AsyncNetworkBackend):
    """Network backend that validates the peer IP used by TCP connect"""

    def __init__(self) -> None:
        """Initialize backend"""
        self.backend = httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[Any] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        """Connect to a checked public address"""
        address = await resolve_public_address(host, port)
        return await self.backend.connect_tcp(
            address,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[Any] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        """Connect to a Unix socket"""
        return await self.backend.connect_unix_socket(path, timeout=timeout, socket_options=socket_options)

    async def sleep(self, seconds: float) -> None:
        """Sleep using the wrapped backend"""
        await self.backend.sleep(seconds)


class SafeAsyncHTTPTransport(httpx.AsyncHTTPTransport):
    """HTTP transport that validates TCP peer addresses"""

    def __init__(self, limits: httpx.Limits) -> None:
        """Initialize transport"""
        super().__init__(limits=limits)
        pool = cast(Any, self)._pool
        pool._network_backend = SafeAsyncNetworkBackend()


def create_http_client(settings: Settings, limits: httpx.Limits | None = None) -> httpx.AsyncClient:
    """Create an HTTP worker client"""
    if limits is None:
        limits = httpx.Limits()
    if settings.block_private_network_targets:
        transport = SafeAsyncHTTPTransport(limits)
        return httpx.AsyncClient(follow_redirects=False, transport=transport)
    return httpx.AsyncClient(follow_redirects=False, limits=limits)


async def resolve_public_address(hostname: str, port: int) -> str:
    """Resolve hostname and return a checked public address"""
    loop = asyncio.get_running_loop()
    infos = await loop.run_in_executor(None, socket.getaddrinfo, hostname, port, 0, socket.SOCK_STREAM, 0, 0)
    addresses = []
    for info in infos:
        sockaddr = info[4]
        address = str(sockaddr[0])
        if is_blocked_ip_address(address):
            raise ValidationError("HTTP target resolves to a blocked network address")
        addresses.append(address)
    if not addresses:
        raise ValidationError("HTTP target did not resolve")
    return addresses[0]


class HttpJobExecutor:
    """Executor for HTTP jobs."""

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        """Initialize the executor."""
        self.client = client
        self.settings = settings

    async def execute(self, job: JobRow) -> ExecutorResult:
        """Execute one HTTP job."""
        started = time.monotonic()
        try:
            payload = HttpJobPayload.model_validate(job.payload)
            validate_headers(payload.headers)
            hostname = validate_url_syntax(payload.url)
            validate_allowed_host(hostname, self.settings.get_allowed_hosts())
            async with self.client.stream(
                payload.method,
                payload.url,
                headers=self._headers(job, payload),
                json=payload.json_body,
                content=self._content(payload),
                timeout=min(job.timeout_seconds, self.settings.http_max_timeout_seconds),
            ) as response:
                preview = await self._read_response_preview(response)
                return self._classify_response(response.status_code, preview, started)
        except httpx.TimeoutException as exc:
            return self._failure("timeout", type(exc).__name__, str(exc), None, started)
        except (httpx.NetworkError, httpx.ProxyError) as exc:
            return self._failure("retryable_failure", type(exc).__name__, str(exc), None, started)
        except (httpx.ProtocolError, httpx.UnsupportedProtocol, httpx.InvalidURL) as exc:
            return self._failure("permanent_failure", type(exc).__name__, str(exc), None, started)
        except OSError as exc:
            return self._failure("retryable_failure", type(exc).__name__, str(exc), None, started)
        except ValidationError as exc:
            return self._failure("permanent_failure", type(exc).__name__, str(exc), None, started)

    def _headers(self, job: JobRow, payload: HttpJobPayload) -> dict[str, str]:
        headers = dict(payload.headers)
        headers["X-PgRelay-Job-Id"] = str(job.id)
        headers["X-PgRelay-Attempt"] = str(job.attempt_count)
        if job.trace_id:
            headers["X-PgRelay-Trace-Id"] = job.trace_id
        if job.idempotency_key and not any(name.lower() == "idempotency-key" for name in headers):
            headers["Idempotency-Key"] = job.idempotency_key
        return headers

    def _content(self, payload: HttpJobPayload) -> bytes | None:
        if payload.body_base64 is None:
            return None
        try:
            return base64.b64decode(payload.body_base64, validate=True)
        except binascii.Error as exc:
            raise ValidationError("Invalid base64 request body") from exc

    async def _read_response_preview(self, response: httpx.Response) -> str:
        chunks = []
        size = 0
        async for chunk in response.aiter_bytes(RESPONSE_CHUNK_BYTES):
            remaining = RESPONSE_PREVIEW_BYTES - size
            chunk_preview = chunk[:remaining]
            chunks.append(chunk_preview)
            size += len(chunk_preview)
            if size >= RESPONSE_PREVIEW_BYTES:
                break
        return b"".join(chunks).decode(response.encoding or "utf-8", errors="replace")

    def _classify_response(self, status: int, preview: str, started: float) -> ExecutorResult:
        HTTP_DELIVERY_TOTAL.labels(status_class=f"{status // 100}xx").inc()
        if 200 <= status < 300:
            outcome: ExecutorOutcome = "succeeded"
            error_type = None
            error_message = None
        elif status in RETRYABLE_HTTP_STATUSES or status >= 500:
            outcome = "retryable_failure"
            error_type = "http_status"
            error_message = f"HTTP status {status}"
        elif status in PERMANENT_HTTP_STATUSES or 300 <= status < 400:
            outcome = "permanent_failure"
            error_type = "http_status"
            error_message = f"HTTP status {status}"
        else:
            outcome = "permanent_failure"
            error_type = "http_status"
            error_message = f"HTTP status {status}"
        return ExecutorResult(
            outcome=outcome,
            error_type=error_type,
            error_message=error_message,
            response_status=status,
            response_body_preview=preview,
            duration_ms=self._duration_ms(started),
        )

    def _failure(
        self,
        outcome: ExecutorOutcome,
        error_type: str,
        error_message: str,
        response_status: int | None,
        started: float,
    ) -> ExecutorResult:
        return ExecutorResult(
            outcome=outcome,
            error_type=error_type,
            error_message=error_message[:2000],
            response_status=response_status,
            response_body_preview=None,
            duration_ms=self._duration_ms(started),
        )

    def _duration_ms(self, started: float) -> int:
        return max(0, int((time.monotonic() - started) * 1000))
