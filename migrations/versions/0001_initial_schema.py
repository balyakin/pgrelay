"""Initial PgRelay schema."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_schema"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create PgRelay tables and indexes."""
    op.create_table(
        "pgrelay_queue",
        sa.Column("name", sa.Text(), primary_key=True),
        sa.Column("paused", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("concurrency_limit", sa.Integer(), nullable=False, server_default=sa.text("8")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("concurrency_limit > 0", name="ck_pgrelay_queue_concurrency_limit"),
    )
    op.create_table(
        "pgrelay_job",
        sa.Column("id", postgresql.UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("queue_name", sa.Text(), sa.ForeignKey("pgrelay_queue.name"), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("headers", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default=sa.text("10")),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False, server_default=sa.text("30")),
        sa.Column("idempotency_key", sa.Text(), nullable=True),
        sa.Column("dedupe_key", sa.Text(), nullable=True),
        sa.Column(
            "replayed_from_job_id",
            postgresql.UUID(),
            sa.ForeignKey("pgrelay_job.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("locked_by", sa.Text(), nullable=True),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_type", sa.Text(), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column("last_response_status", sa.Integer(), nullable=True),
        sa.Column("trace_id", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("kind IN ('http', 'handler')", name="ck_pgrelay_job_kind"),
        sa.CheckConstraint(
            "status IN ('pending', 'leased', 'succeeded', 'dead_letter', 'cancelled')",
            name="ck_pgrelay_job_status",
        ),
        sa.CheckConstraint("max_attempts > 0", name="ck_pgrelay_job_max_attempts"),
    )
    op.create_table(
        "pgrelay_attempt",
        sa.Column("id", postgresql.UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "job_id",
            postgresql.UUID(),
            sa.ForeignKey("pgrelay_job.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("duration_ms", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("error_type", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("response_status", sa.Integer(), nullable=True),
        sa.Column("response_body_preview", sa.String(length=2000), nullable=True),
        sa.CheckConstraint("attempt_number > 0", name="ck_pgrelay_attempt_number"),
        sa.CheckConstraint(
            "status IN ('succeeded', 'failed', 'timeout', 'lease_expired')",
            name="ck_pgrelay_attempt_status",
        ),
    )
    op.create_table(
        "pgrelay_worker",
        sa.Column("worker_id", sa.Text(), primary_key=True),
        sa.Column("queues", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("hostname", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index(
        "idx_pgrelay_job_claim",
        "pgrelay_job",
        ["queue_name", sa.literal_column("priority DESC"), "available_at", "created_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "idx_pgrelay_job_locked_expired",
        "pgrelay_job",
        ["locked_until"],
        postgresql_where=sa.text("status = 'leased'"),
    )
    op.create_index("idx_pgrelay_job_status_created", "pgrelay_job", ["status", "created_at"])
    op.create_index(
        "idx_pgrelay_job_dedupe_key",
        "pgrelay_job",
        ["dedupe_key"],
        postgresql_where=sa.text("dedupe_key IS NOT NULL"),
    )
    op.create_index(
        "uq_pgrelay_job_queue_idempotency_key",
        "pgrelay_job",
        ["queue_name", "idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )
    op.create_index(
        "idx_pgrelay_job_replayed_from",
        "pgrelay_job",
        ["replayed_from_job_id"],
        postgresql_where=sa.text("replayed_from_job_id IS NOT NULL"),
    )
    op.create_index("idx_pgrelay_attempt_job_started", "pgrelay_attempt", ["job_id", "started_at"])
    op.create_index("idx_pgrelay_worker_last_heartbeat", "pgrelay_worker", ["last_heartbeat_at"])


def downgrade() -> None:
    """Drop PgRelay tables and indexes."""
    op.drop_index("idx_pgrelay_worker_last_heartbeat", table_name="pgrelay_worker")
    op.drop_index("idx_pgrelay_attempt_job_started", table_name="pgrelay_attempt")
    op.drop_index("idx_pgrelay_job_replayed_from", table_name="pgrelay_job")
    op.drop_index("uq_pgrelay_job_queue_idempotency_key", table_name="pgrelay_job")
    op.drop_index("idx_pgrelay_job_dedupe_key", table_name="pgrelay_job")
    op.drop_index("idx_pgrelay_job_status_created", table_name="pgrelay_job")
    op.drop_index("idx_pgrelay_job_locked_expired", table_name="pgrelay_job")
    op.drop_index("idx_pgrelay_job_claim", table_name="pgrelay_job")
    op.drop_table("pgrelay_worker")
    op.drop_table("pgrelay_attempt")
    op.drop_table("pgrelay_job")
    op.drop_table("pgrelay_queue")
