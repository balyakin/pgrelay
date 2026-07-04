"""Example database setup."""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.db.session import create_engine

settings = Settings()
engine = create_engine(settings)
SessionFactory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False, autoflush=False)
