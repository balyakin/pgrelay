"""Shared pytest fixtures."""

import os
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from testcontainers.postgres import PostgresContainer

from pgrelay.config.settings import Settings
from pgrelay.db import migrations
from pgrelay.db.session import create_engine, create_session_factory
from pgrelay.repositories.jobs import JobRow


@pytest.fixture(scope="session")
def database_url() -> Generator[str, None, None]:
    """Start a PostgreSQL container and yield an asyncpg URL."""
    with PostgresContainer("postgres:15-alpine", driver="asyncpg") as container:
        yield container.get_connection_url()


@pytest.fixture(scope="session")
def settings(database_url: str) -> Settings:
    """Return test settings."""
    return Settings(
        env="test",
        database_url=database_url,
        api_auth_tokens="test-token",
        http_allowed_hosts="example.com,httpbin.org",
        block_private_network_targets=True,
        worker_concurrency=4,
        worker_batch_size=4,
        worker_lease_seconds=5,
        db_pool_size=8,
        db_max_overflow=4,
    )


@pytest.fixture(scope="session", autouse=True)
def migrated_database(settings: Settings) -> Generator[None, None, None]:
    """Apply migrations once for the PostgreSQL container."""
    os.environ["PGRELAY_DATABASE_URL"] = settings.database_url
    os.environ["PGRELAY_ENV"] = "test"
    os.environ["PGRELAY_API_AUTH_TOKENS"] = "test-token"
    migrations.upgrade(settings, "head")
    yield


@pytest_asyncio.fixture()
async def session_factory(settings: Settings) -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    """Return an async session factory."""
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    yield factory
    await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def clean_tables(session_factory: async_sessionmaker[AsyncSession]) -> AsyncGenerator[None, None]:
    """Truncate PgRelay tables before each test."""
    async with session_factory() as session:
        await session.execute(
            text("TRUNCATE pgrelay_attempt, pgrelay_job, pgrelay_worker, pgrelay_queue RESTART IDENTITY CASCADE")
        )
        await session.commit()
    yield


@pytest_asyncio.fixture()
async def db_session(session_factory: async_sessionmaker[AsyncSession]) -> AsyncGenerator[AsyncSession, None]:
    """Yield a database session."""
    async with session_factory() as session:
        yield session


def make_job_row(**overrides: Any) -> JobRow:
    """Return a JobRow for executor unit tests."""
    now = datetime.now(UTC)
    values: dict[str, Any] = {
        "id": uuid4(),
        "queue_name": "default",
        "kind": "http",
        "name": "job",
        "payload": {},
        "headers": {},
        "metadata": {},
        "status": "leased",
        "priority": 0,
        "max_attempts": 3,
        "attempt_count": 1,
        "available_at": now,
        "timeout_seconds": 2,
        "idempotency_key": None,
        "dedupe_key": None,
        "replayed_from_job_id": None,
        "locked_by": "worker",
        "locked_until": now,
        "last_error_type": None,
        "last_error_message": None,
        "last_response_status": None,
        "trace_id": "trace",
        "created_at": now,
        "updated_at": now,
        "completed_at": None,
    }
    values.update(overrides)
    return JobRow(**values)


async def job_count(session: AsyncSession) -> int:
    """Return total job count."""
    value = await session.scalar(text("SELECT count(*) FROM pgrelay_job"))
    return int(value or 0)


async def job_status(session: AsyncSession, job_id: UUID) -> str:
    """Return status for a job."""
    value = await session.scalar(text("SELECT status FROM pgrelay_job WHERE id = :job_id"), {"job_id": job_id})
    return str(value)
