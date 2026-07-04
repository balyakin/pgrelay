"""Stats use cases."""

from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.repositories.protocols import StatsRepositoryProtocol
from pgrelay.schemas.stats import QueueDepthItem, StatsResponse


class StatsService:
    """Stats service."""

    def __init__(self, repository: StatsRepositoryProtocol) -> None:
        """Initialize the service."""
        self.repository = repository

    async def get_stats(self, session: AsyncSession, *, approximate: bool) -> StatsResponse:
        """Return queue and status stats."""
        counts = await self.repository.counts_by_status(session, approximate=approximate)
        depths = await self.repository.queue_depth(session, approximate=approximate)
        oldest = await self.repository.oldest_pending_age_seconds(session)
        return StatsResponse(
            counts={item.status: item.count for item in counts},
            queue_depth=[
                QueueDepthItem(queue_name=item.queue_name, status=item.status, count=item.count) for item in depths
            ],
            oldest_pending_age_seconds=oldest,
            approximate=approximate,
        )
