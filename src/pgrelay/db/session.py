"""Async SQLAlchemy session helpers."""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from pgrelay.config.settings import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    """Create the runtime async SQLAlchemy engine."""
    return create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout_seconds,
        connect_args={
            "command_timeout": settings.db_statement_timeout_ms / 1000,
            "server_settings": {
                "statement_timeout": str(settings.db_statement_timeout_ms),
                "lock_timeout": str(settings.db_lock_timeout_ms),
            },
        },
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create an async session factory."""
    return async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


async def get_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """Yield a session and commit or roll back around the caller."""
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
