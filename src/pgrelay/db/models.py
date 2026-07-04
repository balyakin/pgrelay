"""SQLAlchemy models for PgRelay tables."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Boolean, DateTime

from pgrelay.db.base import Base


class PgRelayQueue(Base):
    """Queue row model."""

    __tablename__ = "pgrelay_queue"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    paused: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    concurrency_limit: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("8"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))

    __table_args__ = (CheckConstraint("concurrency_limit > 0", name="ck_pgrelay_queue_concurrency_limit"),)


class PgRelayJob(Base):
    """Job row model."""

    __tablename__ = "pgrelay_job"

    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    queue_name: Mapped[str] = mapped_column(ForeignKey("pgrelay_queue.name"), nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    headers: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'pending'"))
    priority: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("10"))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("30"))
    idempotency_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    dedupe_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    replayed_from_job_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("pgrelay_job.id", ondelete="SET NULL"),
        nullable=True,
    )
    locked_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_response_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    trace_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint("kind IN ('http', 'handler')", name="ck_pgrelay_job_kind"),
        CheckConstraint(
            "status IN ('pending', 'leased', 'succeeded', 'dead_letter', 'cancelled')",
            name="ck_pgrelay_job_status",
        ),
        CheckConstraint("max_attempts > 0", name="ck_pgrelay_job_max_attempts"),
        Index(
            "idx_pgrelay_job_claim",
            "queue_name",
            text("priority DESC"),
            "available_at",
            "created_at",
            postgresql_where=text("status = 'pending'"),
        ),
        Index(
            "idx_pgrelay_job_locked_expired",
            "locked_until",
            postgresql_where=text("status = 'leased'"),
        ),
        Index("idx_pgrelay_job_status_created", "status", "created_at"),
        Index("idx_pgrelay_job_dedupe_key", "dedupe_key", postgresql_where=text("dedupe_key IS NOT NULL")),
        Index(
            "uq_pgrelay_job_queue_idempotency_key",
            "queue_name",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        Index(
            "idx_pgrelay_job_replayed_from",
            "replayed_from_job_id",
            postgresql_where=text("replayed_from_job_id IS NOT NULL"),
        ),
    )


class PgRelayAttempt(Base):
    """Attempt row model."""

    __tablename__ = "pgrelay_attempt"

    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    job_id: Mapped[UUID] = mapped_column(ForeignKey("pgrelay_job.id", ondelete="CASCADE"), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    duration_ms: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    error_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    response_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body_preview: Mapped[str | None] = mapped_column(String(2000), nullable=True)

    __table_args__ = (
        CheckConstraint("attempt_number > 0", name="ck_pgrelay_attempt_number"),
        CheckConstraint(
            "status IN ('succeeded', 'failed', 'timeout', 'lease_expired')",
            name="ck_pgrelay_attempt_status",
        ),
        Index("idx_pgrelay_attempt_job_started", "job_id", "started_at"),
    )


class PgRelayWorker(Base):
    """Worker heartbeat row model."""

    __tablename__ = "pgrelay_worker"

    worker_id: Mapped[str] = mapped_column(Text, primary_key=True)
    queues: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    hostname: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    last_heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )

    __table_args__ = (Index("idx_pgrelay_worker_last_heartbeat", "last_heartbeat_at"),)
