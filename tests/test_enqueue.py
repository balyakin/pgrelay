"""Enqueue integration tests."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.repositories.jobs import JobRepository
from pgrelay.sdk.client import PgRelayClient
from tests.conftest import job_count


async def test_enqueue_handler_job_creates_row(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Enqueue handler job creates row."""
    # ARRANGE
    client = PgRelayClient(settings)

    # ACT
    async with session_factory() as session:
        result = await client.enqueue_handler(session=session, name="send_email", payload={"user_id": 1})
        await session.commit()

    # ASSERT
    async with session_factory() as session:
        assert result.created is True
        assert await job_count(session) == 1


async def test_enqueue_creates_missing_queue(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Enqueue creates missing queue."""
    # ARRANGE
    client = PgRelayClient(settings)

    # ACT
    async with session_factory() as session:
        await client.enqueue_handler(session=session, name="send_email", payload={}, queue_name="mail")
        await session.commit()

    # ASSERT
    async with session_factory() as session:
        queue_name = await session.scalar(text("SELECT name FROM pgrelay_queue WHERE name = 'mail'"))
        assert queue_name == "mail"


async def test_rollback_app_transaction_removes_domain_row_and_job(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Rollback app transaction removes domain row and job."""
    # ARRANGE
    client = PgRelayClient(settings)
    async with session_factory() as session:
        await session.execute(text("CREATE TABLE IF NOT EXISTS test_domain_event (id text PRIMARY KEY)"))
        await session.execute(text("TRUNCATE test_domain_event"))
        await session.commit()

    # ACT
    async with session_factory() as session:
        transaction = await session.begin()
        await session.execute(text("INSERT INTO test_domain_event (id) VALUES ('evt_rollback')"))
        await client.enqueue_handler(session=session, name="domain_event", payload={"id": "evt_rollback"})
        await transaction.rollback()

    # ASSERT
    async with session_factory() as session:
        domain_count = await session.scalar(text("SELECT count(*) FROM test_domain_event"))
        assert int(domain_count or 0) == 0
        assert await job_count(session) == 0


async def test_commit_app_transaction_persists_domain_row_and_job(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Commit app transaction persists domain row and job."""
    # ARRANGE
    client = PgRelayClient(settings)
    async with session_factory() as session:
        await session.execute(text("CREATE TABLE IF NOT EXISTS test_domain_event (id text PRIMARY KEY)"))
        await session.execute(text("TRUNCATE test_domain_event"))
        await session.commit()

    # ACT
    async with session_factory() as session, session.begin():
        await session.execute(text("INSERT INTO test_domain_event (id) VALUES ('evt_commit')"))
        await client.enqueue_handler(session=session, name="domain_event", payload={"id": "evt_commit"})

    # ASSERT
    async with session_factory() as session:
        domain_count = await session.scalar(text("SELECT count(*) FROM test_domain_event"))
        assert int(domain_count or 0) == 1
        assert await job_count(session) == 1


async def test_sdk_does_not_commit_external_session(settings: Settings, db_session: AsyncSession) -> None:
    """SDK does not commit external session."""
    # ARRANGE
    client = PgRelayClient(settings)
    repository = JobRepository()

    # ACT
    result = await client.enqueue_handler(session=db_session, name="send_email", payload={})

    # ASSERT
    duplicate = await repository.get_job(db_session, result.job_id)
    assert duplicate is not None
    await db_session.rollback()
    assert await repository.get_job(db_session, result.job_id) is None
