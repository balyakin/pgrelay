"""Expired lease recovery orchestration."""

from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.config.settings import Settings
from pgrelay.repositories.protocols import JobRepositoryProtocol


class LeaseRecovery:
    """Recover expired job leases."""

    def __init__(self, repository: JobRepositoryProtocol, settings: Settings) -> None:
        """Initialize recovery."""
        self.repository = repository
        self.settings = settings

    async def recover(self, session: AsyncSession) -> int:
        """Recover one batch of expired leases."""
        return await self.repository.recover_expired_leases(session, batch_size=self.settings.worker_batch_size)
