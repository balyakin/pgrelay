"""Worker repository."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import cast

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.errors import RepositoryError


@dataclass(frozen=True, slots=True)
class WorkerRow:
    """Worker repository row."""

    worker_id: str
    queues: list[str]
    hostname: str
    started_at: datetime
    last_heartbeat_at: datetime
    alive: bool


class WorkerRepository:
    """PostgreSQL worker repository."""

    async def register_worker(
        self,
        session: AsyncSession,
        *,
        worker_id: str,
        queues: Sequence[str],
        hostname: str,
    ) -> WorkerRow:
        """Register a worker heartbeat row."""
        try:
            result = await session.execute(
                text(
                    """
                    INSERT INTO pgrelay_worker (worker_id, queues, hostname)
                    VALUES (:worker_id, :queues, :hostname)
                    ON CONFLICT (worker_id) DO UPDATE
                    SET queues = EXCLUDED.queues,
                        hostname = EXCLUDED.hostname,
                        last_heartbeat_at = now()
                    RETURNING worker_id, queues, hostname, started_at, last_heartbeat_at,
                              last_heartbeat_at > now() - interval '2 minutes' AS alive
                    """
                ),
                {"worker_id": worker_id, "queues": list(queues), "hostname": hostname},
            )
            row = result.mappings().one()
            return WorkerRow(**row)
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Worker registration failed") from exc

    async def heartbeat(self, session: AsyncSession, *, worker_id: str) -> bool:
        """Update worker heartbeat."""
        try:
            result = await session.execute(
                text("UPDATE pgrelay_worker SET last_heartbeat_at = now() WHERE worker_id = :worker_id"),
                {"worker_id": worker_id},
            )
            return cast(int, getattr(result, "rowcount", 0)) > 0
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Worker heartbeat failed") from exc

    async def list_workers(self, session: AsyncSession) -> list[WorkerRow]:
        """List workers with database-computed alive state."""
        try:
            result = await session.execute(
                text(
                    """
                    SELECT worker_id, queues, hostname, started_at, last_heartbeat_at,
                           last_heartbeat_at > now() - interval '2 minutes' AS alive
                    FROM pgrelay_worker
                    ORDER BY worker_id
                    """
                )
            )
            return [WorkerRow(**row) for row in result.mappings().all()]
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Worker list failed") from exc
