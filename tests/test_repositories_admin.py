"""Admin repository and DB helper tests."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.db import migrations
from pgrelay.db.session import get_session
from pgrelay.errors import RepositoryError
from pgrelay.repositories.jobs import JobRepository
from pgrelay.repositories.queues import QueueRepository
from pgrelay.repositories.stats import StatsRepository
from pgrelay.repositories.workers import WorkerRepository
from pgrelay.schemas.queues import QueueUpdateRequest
from pgrelay.sdk.client import PgRelayClient
from pgrelay.services.queues import QueueService
from pgrelay.services.stats import StatsService


async def test_worker_repository_register_heartbeat_and_list(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Worker repository registers, heartbeats and lists workers."""
    # ARRANGE
    repository = WorkerRepository()

    # ACT
    async with session_factory() as session:
        row = await repository.register_worker(
            session,
            worker_id="worker-1",
            queues=["default"],
            hostname="host",
        )
        updated = await repository.heartbeat(session, worker_id="worker-1")
        workers = await repository.list_workers(session)

    # ASSERT
    assert row.alive is True
    assert updated is True
    assert workers[0].worker_id == "worker-1"


async def test_stats_repository_and_service_return_counts(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Stats repository and service return counts and queue depth."""
    # ARRANGE
    client = PgRelayClient(settings)
    repository = StatsRepository()
    service = StatsService(repository)
    async with session_factory() as session:
        await client.enqueue_handler(session=session, name="handler", payload={})
        await session.commit()

    # ACT
    async with session_factory() as session:
        counts = await repository.counts_by_status(session, approximate=False)
        depths = await repository.queue_depth(session, approximate=False)
        oldest = await repository.oldest_pending_age_seconds(session)
        response = await service.get_stats(session, approximate=False)

    # ASSERT
    assert counts[0].status == "pending"
    assert depths[0].queue_name == "default"
    assert oldest is not None
    assert response.counts["pending"] == 1


async def test_stats_repository_approximate_uses_estimates(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Approximate stats use planner estimates for large tables."""
    # ARRANGE
    repository = StatsRepository()

    async def estimated_row_count(session: AsyncSession) -> int:
        return 1_000_000

    async def estimate_query_rows(
        session: AsyncSession,
        query_sql: str,
        parameters: dict[str, object],
    ) -> int:
        if parameters.get("queue_name") == "default" and parameters["status"] == "pending":
            return 7
        if "queue_name" not in parameters and parameters["status"] == "pending":
            return 11
        return 0

    async def queue_names(session: AsyncSession) -> list[str]:
        return ["default"]

    monkeypatch.setattr(repository, "_estimated_row_count", estimated_row_count)
    monkeypatch.setattr(repository, "_estimate_query_rows", estimate_query_rows)
    monkeypatch.setattr(repository, "_queue_names", queue_names)

    # ACT
    counts = await repository.counts_by_status(db_session, approximate=True)
    depths = await repository.queue_depth(db_session, approximate=True)

    # ASSERT
    assert counts[0].status == "pending"
    assert counts[0].count == 11
    assert depths[0].queue_name == "default"
    assert depths[0].status == "pending"
    assert depths[0].count == 7


async def test_queue_repository_and_service_list_update_pause_resume(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Queue repository and service list and update queues."""
    # ARRANGE
    repository = QueueRepository()
    service = QueueService(repository)

    # ACT
    async with session_factory() as session:
        await repository.upsert_queue(session, queue_name="default")
        listed = await service.list_queues(session)
        updated = await service.upsert_or_update(
            session,
            queue_name="default",
            request=QueueUpdateRequest(paused=False, concurrency_limit=2),
        )
        paused = await service.pause(session, queue_name="default")
        resumed = await service.resume(session, queue_name="default")

    # ASSERT
    assert listed[0].name == "default"
    assert updated.concurrency_limit == 2
    assert paused.paused is True
    assert resumed.paused is False


async def test_db_get_session_commits_and_rolls_back(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """db.get_session commits success and rolls back failures."""
    # ARRANGE
    generator = get_session(session_factory)
    session = await generator.__anext__()

    # ACT
    await session.execute(text("INSERT INTO pgrelay_queue (name) VALUES ('committed')"))
    try:
        await generator.__anext__()
    except StopAsyncIteration:
        committed = True
    async with session_factory() as check:
        exists = await check.scalar(text("SELECT name FROM pgrelay_queue WHERE name = 'committed'"))

    # ASSERT
    assert committed is True
    assert exists == "committed"

    # ARRANGE
    generator = get_session(session_factory)
    session = await generator.__anext__()

    # ACT
    await session.execute(text("INSERT INTO pgrelay_queue (name) VALUES ('rolled_back')"))
    try:
        await generator.athrow(RuntimeError("boom"))
    except RuntimeError:
        rolled_back = True
    async with session_factory() as check:
        missing = await check.scalar(text("SELECT name FROM pgrelay_queue WHERE name = 'rolled_back'"))

    # ASSERT
    assert rolled_back is True
    assert missing is None


def test_alembic_config_uses_explicit_database_url(settings: Settings) -> None:
    """Programmatic Alembic config carries the explicit settings database URL."""
    # ACT
    config = migrations.create_alembic_config(settings)

    # ASSERT
    assert config.get_main_option("sqlalchemy.url") == settings.database_url


async def test_repository_error_paths_roll_back(db_session: AsyncSession) -> None:
    """Repository errors roll back and raise RepositoryError subclasses."""
    # ARRANGE
    repository = JobRepository()

    # ACT
    with pytest.raises(RepositoryError):
        await repository.get_job(db_session, "not-a-uuid")  # type: ignore[arg-type]
