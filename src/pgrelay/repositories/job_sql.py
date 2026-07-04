"""Raw SQL statements for job repository."""

from sqlalchemy import Text, bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.sql.elements import TextClause


def insert_job_statement() -> TextClause:
    """Return typed insert job statement."""
    return text(
        """
        INSERT INTO pgrelay_job (
            queue_name, kind, name, payload, headers, metadata, priority, max_attempts,
            timeout_seconds, available_at, idempotency_key, dedupe_key, trace_id, replayed_from_job_id
        )
        VALUES (
            :queue_name, :kind, :name, :payload, :headers, :metadata, :priority, :max_attempts,
            :timeout_seconds, COALESCE(:available_at, now()), :idempotency_key, :dedupe_key, :trace_id,
            :replayed_from_job_id
        )
        ON CONFLICT (queue_name, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
        RETURNING *
        """
    ).bindparams(
        bindparam("payload", type_=JSONB()),
        bindparam("headers", type_=JSONB()),
        bindparam("metadata", type_=JSONB()),
    )


def claim_jobs_statement() -> TextClause:
    """Return typed claim jobs statement."""
    return text(
        """
        WITH locked_queues AS (
            SELECT q.name, q.concurrency_limit
            FROM pgrelay_queue q
            WHERE q.paused = false AND q.name = ANY(:queue_names)
            ORDER BY q.name
            FOR UPDATE
        ), active_queues AS (
            SELECT q.name, GREATEST(q.concurrency_limit - COALESCE(l.leased_count, 0), 0) AS slots
            FROM locked_queues q
            LEFT JOIN (
                SELECT queue_name, count(*)::int AS leased_count
                FROM pgrelay_job
                WHERE status = 'leased'
                GROUP BY queue_name
            ) l ON l.queue_name = q.name
        ), picked AS (
            SELECT p.id
            FROM active_queues q
            CROSS JOIN LATERAL (
                SELECT j.id, j.priority, j.available_at, j.created_at
                FROM pgrelay_job j
                WHERE j.queue_name = q.name
                  AND j.status = 'pending'
                  AND j.available_at <= now()
                  AND j.attempt_count < j.max_attempts
                ORDER BY j.priority DESC, j.available_at ASC, j.created_at ASC
                LIMIT q.slots
                FOR UPDATE OF j SKIP LOCKED
            ) p
            ORDER BY p.priority DESC, p.available_at ASC, p.created_at ASC
            LIMIT :batch_size
        )
        UPDATE pgrelay_job j
        SET status = 'leased',
            locked_by = :worker_id,
            locked_until = now() + (:lease_seconds * interval '1 second'),
            attempt_count = j.attempt_count + 1,
            updated_at = now()
        FROM picked
        WHERE j.id = picked.id
        RETURNING j.*
        """
    ).bindparams(bindparam("queue_names", type_=ARRAY(Text())))


EXTEND_LEASE_SQL = """
UPDATE pgrelay_job
SET locked_until = now() + (:lease_seconds * interval '1 second'), updated_at = now()
WHERE id = :job_id AND locked_by = :worker_id AND status = 'leased'
"""

RETURN_TO_PENDING_SQL = """
UPDATE pgrelay_job
SET status = 'pending',
    attempt_count = GREATEST(attempt_count - 1, 0),
    locked_by = NULL,
    locked_until = NULL,
    updated_at = now()
WHERE id = :job_id AND locked_by = :worker_id AND status = 'leased'
"""

INSERT_ATTEMPT_SQL = """
INSERT INTO pgrelay_attempt (
    job_id, attempt_number, worker_id, status, started_at, finished_at, duration_ms,
    error_type, error_message, response_status, response_body_preview
)
VALUES (
    :job_id, :attempt_number, :worker_id, :status,
    now() - (:duration_ms * interval '1 millisecond'), now(), :duration_ms,
    :error_type, :error_message, :response_status, :response_body_preview
)
"""

COMPLETE_JOB_SQL = """
UPDATE pgrelay_job
SET status = 'succeeded', locked_by = NULL, locked_until = NULL,
    last_response_status = :response_status, completed_at = now(), updated_at = now()
WHERE id = :job_id AND locked_by = :worker_id AND status = 'leased'
"""

FAIL_JOB_SQL = """
UPDATE pgrelay_job
SET status = CASE WHEN :should_retry THEN 'pending' ELSE 'dead_letter' END,
    available_at = CASE
        WHEN :should_retry THEN now() + (:retry_delay_seconds * interval '1 second')
        ELSE available_at
    END,
    locked_by = NULL,
    locked_until = NULL,
    last_error_type = :error_type,
    last_error_message = :error_message,
    last_response_status = :response_status,
    completed_at = CASE WHEN :should_retry THEN completed_at ELSE now() END,
    updated_at = now()
WHERE id = :job_id AND locked_by = :worker_id AND status = 'leased'
"""

RECOVER_EXPIRED_LEASES_SQL = """
WITH picked AS (
    SELECT id, locked_by, locked_until
    FROM pgrelay_job
    WHERE status = 'leased' AND locked_until < now()
    ORDER BY locked_until ASC
    LIMIT :batch_size
    FOR UPDATE SKIP LOCKED
), recovered AS (
    UPDATE pgrelay_job j
    SET status = CASE WHEN j.attempt_count >= j.max_attempts THEN 'dead_letter' ELSE 'pending' END,
        locked_by = NULL,
        locked_until = NULL,
        last_error_type = 'lease_expired',
        last_error_message = 'Worker lease expired before completion',
        completed_at = CASE WHEN j.attempt_count >= j.max_attempts THEN now() ELSE j.completed_at END,
        updated_at = now()
    FROM picked
    WHERE j.id = picked.id
    RETURNING j.id, j.attempt_count, picked.locked_by, picked.locked_until
)
INSERT INTO pgrelay_attempt (
    job_id, attempt_number, worker_id, status, started_at, finished_at, duration_ms,
    error_type, error_message
)
SELECT id, attempt_count, COALESCE(locked_by, 'unknown'), 'lease_expired',
       COALESCE(locked_until, now()), now(), 0, 'lease_expired',
       'Worker lease expired before completion'
FROM recovered
RETURNING job_id
"""

CANCEL_PENDING_JOB_SQL = """
UPDATE pgrelay_job
SET status = 'cancelled', completed_at = now(), updated_at = now()
WHERE id = :job_id AND status = 'pending'
RETURNING *
"""

PURGE_JOBS_SQL = """
WITH picked AS (
    SELECT id, status
    FROM pgrelay_job
    WHERE (
        status = 'succeeded'
        AND completed_at < now() - (:succeeded_days * interval '1 day')
    ) OR (
        status = 'dead_letter'
        AND completed_at < now() - (:dead_letter_days * interval '1 day')
    ) OR (
        status = 'cancelled'
        AND completed_at < now() - (:dead_letter_days * interval '1 day')
    )
    ORDER BY completed_at ASC
    LIMIT :batch_size
)
DELETE FROM pgrelay_job j
USING picked
WHERE j.id = picked.id
RETURNING picked.status
"""
