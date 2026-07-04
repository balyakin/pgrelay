"""Prometheus metrics for PgRelay."""

from prometheus_client import Counter, Gauge, Histogram

JOBS_CLAIMED_TOTAL = Counter("pgrelay_jobs_claimed_total", "Claimed jobs", ["queue"])
JOBS_COMPLETED_TOTAL = Counter("pgrelay_jobs_completed_total", "Completed jobs", ["queue", "outcome"])
JOB_DURATION_SECONDS = Histogram("pgrelay_job_duration_seconds", "Job execution duration", ["queue", "kind"])
QUEUE_DEPTH = Gauge("pgrelay_queue_depth", "Queue depth", ["queue", "status"])
OLDEST_PENDING_AGE_SECONDS = Gauge("pgrelay_oldest_pending_age_seconds", "Oldest pending age", ["queue"])
CLAIM_BATCH_SIZE = Histogram("pgrelay_claim_batch_size", "Claim batch size")
HTTP_DELIVERY_TOTAL = Counter("pgrelay_http_delivery_total", "HTTP delivery outcomes", ["status_class"])
WORKER_HEARTBEAT_TIMESTAMP_SECONDS = Gauge(
    "pgrelay_worker_heartbeat_timestamp_seconds",
    "Worker heartbeat timestamp",
    ["worker_id"],
)
