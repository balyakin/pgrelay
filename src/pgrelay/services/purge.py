"""Purge use cases."""

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.repositories.jobs import PurgeResult
from pgrelay.repositories.protocols import JobRepositoryProtocol


@dataclass(frozen=True, slots=True)
class PurgeSummary:
    """Purge summary."""

    succeeded: int
    dead_letter: int
    cancelled: int

    @property
    def total(self) -> int:
        """Return total deleted rows."""
        return self.succeeded + self.dead_letter + self.cancelled


class PurgeService:
    """Purge service for final jobs."""

    def __init__(self, repository: JobRepositoryProtocol, settings: Settings) -> None:
        """Initialize the service."""
        self.repository = repository
        self.settings = settings

    async def purge_batch(self, session: AsyncSession) -> PurgeResult:
        """Purge one limited batch."""
        return await self.repository.purge_jobs(
            session,
            succeeded_days=self.settings.retention_succeeded_days,
            dead_letter_days=self.settings.retention_dead_letter_days,
            batch_size=self.settings.purge_batch_size,
        )

    async def purge_until_done(self, session_factory: async_sessionmaker[AsyncSession]) -> PurgeSummary:
        """Purge batches in separate transactions until the final partial batch."""
        succeeded = 0
        dead_letter = 0
        cancelled = 0
        while True:
            async with session_factory() as session:
                result = await self.purge_batch(session)
                await session.commit()
            succeeded += result.succeeded
            dead_letter += result.dead_letter
            cancelled += result.cancelled
            if result.total < self.settings.purge_batch_size:
                return PurgeSummary(succeeded=succeeded, dead_letter=dead_letter, cancelled=cancelled)
