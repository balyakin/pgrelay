"""Stats endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.api.dependencies import get_session, get_stats_service, require_auth
from pgrelay.schemas.stats import StatsResponse
from pgrelay.services.stats import StatsService

router = APIRouter(prefix="/v1/stats", tags=["stats"], dependencies=[Depends(require_auth)])
SESSION_DEPENDENCY = Depends(get_session)
STATS_SERVICE_DEPENDENCY = Depends(get_stats_service)


@router.get("", response_model=StatsResponse)
async def get_stats(
    approximate: bool = False,
    session: AsyncSession = SESSION_DEPENDENCY,
    service: StatsService = STATS_SERVICE_DEPENDENCY,
) -> StatsResponse:
    """Return PgRelay stats."""
    return await service.get_stats(session, approximate=approximate)
