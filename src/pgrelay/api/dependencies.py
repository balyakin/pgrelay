"""FastAPI dependency helpers."""

from collections.abc import AsyncGenerator
from typing import cast

from fastapi import Header, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.security.auth import require_api_token
from pgrelay.services.enqueue import EnqueueService
from pgrelay.services.jobs import JobService
from pgrelay.services.queues import QueueService
from pgrelay.services.stats import StatsService
from pgrelay.services.workers import WorkerService


def get_settings(request: Request) -> Settings:
    """Return application settings."""
    return cast(Settings, request.app.state.settings)


async def get_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Yield an API database session."""
    session_factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_enqueue_service(request: Request) -> EnqueueService:
    """Return enqueue service."""
    return cast(EnqueueService, request.app.state.enqueue_service)


def get_job_service(request: Request) -> JobService:
    """Return job service."""
    return cast(JobService, request.app.state.job_service)


def get_queue_service(request: Request) -> QueueService:
    """Return queue service."""
    return cast(QueueService, request.app.state.queue_service)


def get_stats_service(request: Request) -> StatsService:
    """Return stats service."""
    return cast(StatsService, request.app.state.stats_service)


def get_worker_service(request: Request) -> WorkerService:
    """Return worker service."""
    return cast(WorkerService, request.app.state.worker_service)


async def require_auth(request: Request, authorization: str | None = Header(default=None)) -> None:
    """Require API authentication for admin endpoints."""
    await require_api_token(get_settings(request), authorization)
