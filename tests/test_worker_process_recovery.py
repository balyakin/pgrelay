"""Worker process recovery tests."""

import asyncio
import os
import sys
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.sdk.client import PgRelayClient


class OneHangThenOkServer:
    """HTTP server that hangs once and then returns success."""

    def __init__(self) -> None:
        """Initialize server state."""
        self.host = "127.0.0.1"
        self.port = 0
        self.first_request_received = asyncio.Event()
        self.release_first_request = asyncio.Event()
        self.server: asyncio.AbstractServer | None = None

    async def __aenter__(self) -> "OneHangThenOkServer":
        """Start the server."""
        self.server = await asyncio.start_server(self.handle_request, self.host, 0)
        sockets = self.server.sockets
        if sockets is None:
            raise RuntimeError("HTTP test server has no listening sockets")
        server_socket = next(iter(sockets))
        socket_info = server_socket.getsockname()
        self.port = int(socket_info[1])
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Stop the server."""
        self.release_first_request.set()
        if self.server is None:
            return
        self.server.close()
        await self.server.wait_closed()

    def get_url(self) -> str:
        """Return server URL."""
        return f"http://{self.host}:{self.port}/job"

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle one HTTP request."""
        await reader.read(1024)
        if not self.first_request_received.is_set():
            self.first_request_received.set()
            await self.release_first_request.wait()
            writer.close()
            await writer.wait_closed()
            return
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
        await writer.drain()
        writer.close()
        await writer.wait_closed()


def build_worker_env(settings: Settings) -> dict[str, str]:
    """Build environment for a worker subprocess."""
    worker_env = os.environ.copy()
    worker_env.update(
        {
            "PGRELAY_DATABASE_URL": settings.database_url,
            "PGRELAY_ENV": "test",
            "PGRELAY_API_AUTH_TOKENS": "test-token",
            "PGRELAY_HTTP_ALLOWED_HOSTS": "127.0.0.1",
            "PGRELAY_BLOCK_PRIVATE_NETWORK_TARGETS": "false",
            "PGRELAY_WORKER_CONCURRENCY": "1",
            "PGRELAY_WORKER_BATCH_SIZE": "1",
            "PGRELAY_WORKER_LEASE_SECONDS": "5",
            "PGRELAY_WORKER_POLL_INTERVAL_SECONDS": "0.1",
            "PGRELAY_WORKER_SHUTDOWN_GRACE_SECONDS": "1",
            "PGRELAY_DB_POOL_SIZE": "4",
            "PGRELAY_DB_MAX_OVERFLOW": "1",
            "PGRELAY_LOG_LEVEL": "ERROR",
        }
    )
    return worker_env


async def start_worker_process(worker_env: dict[str, str]) -> asyncio.subprocess.Process:
    """Start a worker subprocess."""
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "pgrelay",
        "worker",
        env=worker_env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )


async def stop_worker_process(process: asyncio.subprocess.Process) -> None:
    """Stop a worker subprocess."""
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        process.kill()
        await process.wait()


async def get_job_status(session_factory: async_sessionmaker[AsyncSession], job_id: UUID) -> str:
    """Return job status."""
    async with session_factory() as session:
        value = await session.scalar(text("SELECT status FROM pgrelay_job WHERE id = :job_id"), {"job_id": job_id})
    return str(value)


async def wait_for_job_status(
    session_factory: async_sessionmaker[AsyncSession],
    job_id: UUID,
    expected_status: str,
) -> None:
    """Wait until a job reaches the expected status."""
    attempts_remaining = 100
    while attempts_remaining > 0:
        status = await get_job_status(session_factory, job_id)
        if status == expected_status:
            return
        attempts_remaining -= 1
        await asyncio.sleep(0.1)
    raise AssertionError(f"Job did not reach {expected_status}")


async def test_killed_worker_process_http_job_is_recovered(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Killed worker process HTTP job is recovered by another worker."""
    # ARRANGE
    worker_settings = settings.model_copy(
        update={
            "block_private_network_targets": False,
            "http_allowed_hosts": "127.0.0.1",
            "worker_concurrency": 1,
            "worker_batch_size": 1,
            "worker_lease_seconds": 5,
            "worker_poll_interval_seconds": 0.1,
            "worker_shutdown_grace_seconds": 1,
            "db_pool_size": 4,
            "db_max_overflow": 1,
        }
    )
    worker_env = build_worker_env(worker_settings)
    async with OneHangThenOkServer() as server:
        client = PgRelayClient(worker_settings)
        async with session_factory() as session:
            result = await client.enqueue_http(
                session=session,
                name="chaos.http",
                url=server.get_url(),
                max_attempts=3,
                timeout_seconds=30,
            )
            await session.commit()

        # ACT
        first_worker = await start_worker_process(worker_env)
        try:
            await asyncio.wait_for(server.first_request_received.wait(), timeout=10)
            first_worker.kill()
            await asyncio.wait_for(first_worker.wait(), timeout=10)
            await asyncio.sleep(5.5)
            second_worker = await start_worker_process(worker_env)
            try:
                await wait_for_job_status(session_factory, result.job_id, "succeeded")
            finally:
                await stop_worker_process(second_worker)
        finally:
            await stop_worker_process(first_worker)

    # ASSERT
    status = await get_job_status(session_factory, result.job_id)
    assert status == "succeeded"
