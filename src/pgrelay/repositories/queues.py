"""Queue repository."""

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.db.models import PgRelayQueue
from pgrelay.errors import QueueNotFoundError, RepositoryError


@dataclass(frozen=True, slots=True)
class QueueRow:
    """Queue repository row."""

    name: str
    paused: bool
    concurrency_limit: int
    created_at: datetime
    updated_at: datetime


def _to_row(model: PgRelayQueue) -> QueueRow:
    return QueueRow(
        name=model.name,
        paused=model.paused,
        concurrency_limit=model.concurrency_limit,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )


class QueueRepository:
    """PostgreSQL queue repository."""

    async def upsert_queue(self, session: AsyncSession, *, queue_name: str) -> QueueRow:
        """Create a queue if missing and return it."""
        try:
            await session.execute(
                text("INSERT INTO pgrelay_queue (name) VALUES (:queue_name) ON CONFLICT (name) DO NOTHING"),
                {"queue_name": queue_name},
            )
            row = await self.get_queue(session, queue_name=queue_name)
            if row is None:
                raise RepositoryError("Queue upsert did not return a row")
            return row
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Queue upsert failed") from exc

    async def get_queue(self, session: AsyncSession, *, queue_name: str) -> QueueRow | None:
        """Return a queue by name."""
        try:
            result = await session.execute(select(PgRelayQueue).where(PgRelayQueue.name == queue_name))
            model = result.scalar_one_or_none()
            return None if model is None else _to_row(model)
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Queue lookup failed") from exc

    async def list_queues(self, session: AsyncSession) -> list[QueueRow]:
        """Return all queues."""
        try:
            result = await session.execute(select(PgRelayQueue).order_by(PgRelayQueue.name.asc()))
            return [_to_row(model) for model in result.scalars().all()]
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Queue list failed") from exc

    async def update_queue(
        self,
        session: AsyncSession,
        *,
        queue_name: str,
        paused: bool | None,
        concurrency_limit: int | None,
    ) -> QueueRow:
        """Update queue settings."""
        try:
            await self.upsert_queue(session, queue_name=queue_name)
            updates = []
            params: dict[str, object] = {"queue_name": queue_name}
            if paused is not None:
                updates.append("paused = :paused")
                params["paused"] = paused
            if concurrency_limit is not None:
                updates.append("concurrency_limit = :concurrency_limit")
                params["concurrency_limit"] = concurrency_limit
            if updates:
                updates.append("updated_at = now()")
                await session.execute(
                    text(f"UPDATE pgrelay_queue SET {', '.join(updates)} WHERE name = :queue_name"),
                    params,
                )
            row = await self.get_queue(session, queue_name=queue_name)
            if row is None:
                raise QueueNotFoundError("Queue not found")
            return row
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Queue update failed") from exc

    async def pause_queue(self, session: AsyncSession, *, queue_name: str) -> QueueRow:
        """Pause a queue."""
        return await self.update_queue(session, queue_name=queue_name, paused=True, concurrency_limit=None)

    async def resume_queue(self, session: AsyncSession, *, queue_name: str) -> QueueRow:
        """Resume a queue."""
        return await self.update_queue(session, queue_name=queue_name, paused=False, concurrency_limit=None)
