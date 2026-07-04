"""Stats repository."""

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from pgrelay.constants import JOB_STATUSES
from pgrelay.errors import RepositoryError

APPROXIMATE_ROW_THRESHOLD = 1_000_000


@dataclass(frozen=True, slots=True)
class StatsCountsRow:
    """Count by status row."""

    status: str
    count: int


@dataclass(frozen=True, slots=True)
class QueueDepthRow:
    """Queue depth row."""

    queue_name: str
    status: str
    count: int


class StatsRepository:
    """PostgreSQL stats repository."""

    async def counts_by_status(self, session: AsyncSession, *, approximate: bool) -> list[StatsCountsRow]:
        """Return job counts by status."""
        try:
            if approximate and await self._uses_estimates(session):
                return await self._estimated_counts_by_status(session)
            return await self._exact_counts_by_status(session)
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Stats count failed") from exc

    async def queue_depth(self, session: AsyncSession, *, approximate: bool) -> list[QueueDepthRow]:
        """Return queue depth by queue and status."""
        try:
            if approximate and await self._uses_estimates(session):
                return await self._estimated_queue_depth(session)
            return await self._exact_queue_depth(session)
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Queue depth failed") from exc

    async def oldest_pending_age_seconds(self, session: AsyncSession) -> float | None:
        """Return oldest pending age in seconds."""
        try:
            result = await session.execute(
                text(
                    """
                    SELECT EXTRACT(EPOCH FROM (now() - min(created_at))) AS age_seconds
                    FROM pgrelay_job
                    WHERE status = 'pending'
                    """
                )
            )
            value = result.scalar_one()
            return None if value is None else float(value)
        except SQLAlchemyError as exc:
            await session.rollback()
            raise RepositoryError("Oldest pending age failed") from exc

    async def _exact_counts_by_status(self, session: AsyncSession) -> list[StatsCountsRow]:
        result = await session.execute(
            text("SELECT status, count(*)::bigint AS count FROM pgrelay_job GROUP BY status ORDER BY status")
        )
        return [StatsCountsRow(status=row["status"], count=int(row["count"])) for row in result.mappings().all()]

    async def _exact_queue_depth(self, session: AsyncSession) -> list[QueueDepthRow]:
        result = await session.execute(
            text(
                """
                SELECT queue_name, status, count(*)::bigint AS count
                FROM pgrelay_job
                GROUP BY queue_name, status
                ORDER BY queue_name, status
                """
            )
        )
        return [
            QueueDepthRow(queue_name=row["queue_name"], status=row["status"], count=int(row["count"]))
            for row in result.mappings().all()
        ]

    async def _uses_estimates(self, session: AsyncSession) -> bool:
        return await self._estimated_row_count(session) >= APPROXIMATE_ROW_THRESHOLD

    async def _estimated_row_count(self, session: AsyncSession) -> int:
        value = await session.scalar(
            text(
                """
                SELECT GREATEST(COALESCE(NULLIF(reltuples, -1), 0), 0)::bigint
                FROM pg_class
                WHERE oid = 'pgrelay_job'::regclass
                """
            )
        )
        return int(value or 0)

    async def _estimated_counts_by_status(self, session: AsyncSession) -> list[StatsCountsRow]:
        rows = []
        for status in JOB_STATUSES:
            count = await self._estimate_query_rows(
                session,
                "SELECT 1 FROM pgrelay_job WHERE status = :status",
                {"status": status},
            )
            if count > 0:
                rows.append(StatsCountsRow(status=status, count=count))
        return rows

    async def _estimated_queue_depth(self, session: AsyncSession) -> list[QueueDepthRow]:
        rows = []
        for queue_name in await self._queue_names(session):
            for status in JOB_STATUSES:
                count = await self._estimate_query_rows(
                    session,
                    "SELECT 1 FROM pgrelay_job WHERE queue_name = :queue_name AND status = :status",
                    {"queue_name": queue_name, "status": status},
                )
                if count > 0:
                    rows.append(QueueDepthRow(queue_name=queue_name, status=status, count=count))
        return rows

    async def _queue_names(self, session: AsyncSession) -> list[str]:
        result = await session.execute(text("SELECT name FROM pgrelay_queue ORDER BY name"))
        return [str(name) for name in result.scalars().all()]

    async def _estimate_query_rows(
        self,
        session: AsyncSession,
        query_sql: str,
        parameters: dict[str, object],
    ) -> int:
        plan_value = await session.scalar(text(f"EXPLAIN (FORMAT JSON) {query_sql}"), parameters)
        return self._extract_plan_rows(plan_value)

    def _extract_plan_rows(self, plan_value: object) -> int:
        data: Any = json.loads(plan_value) if isinstance(plan_value, str) else plan_value
        if not isinstance(data, list) or not data:
            return 0
        root = data[0]
        if not isinstance(root, dict):
            return 0
        plan = root.get("Plan")
        if not isinstance(plan, dict):
            return 0
        rows = plan.get("Plan Rows", 0)
        return max(0, int(rows))
