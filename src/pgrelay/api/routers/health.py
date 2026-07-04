"""Health endpoints."""

from fastapi import APIRouter, Request
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.errors import DatabaseUnavailableError

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Return process liveness."""
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(request: Request) -> dict[str, str]:
    """Return database readiness."""
    session_factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    try:
        async with session_factory() as session:
            await session.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise DatabaseUnavailableError("Database unavailable") from exc
    return {"status": "ready", "database": "ok"}
