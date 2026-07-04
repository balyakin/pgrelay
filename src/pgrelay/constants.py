"""Shared PgRelay constants."""

DEFAULT_QUEUE_NAME = "default"
DEFAULT_API_TOKEN = "dev-token-change-me"

JOB_KIND_HTTP = "http"
JOB_KIND_HANDLER = "handler"
JOB_KINDS = (JOB_KIND_HTTP, JOB_KIND_HANDLER)

JOB_STATUS_PENDING = "pending"
JOB_STATUS_LEASED = "leased"
JOB_STATUS_SUCCEEDED = "succeeded"
JOB_STATUS_DEAD_LETTER = "dead_letter"
JOB_STATUS_CANCELLED = "cancelled"
JOB_STATUSES = (
    JOB_STATUS_PENDING,
    JOB_STATUS_LEASED,
    JOB_STATUS_SUCCEEDED,
    JOB_STATUS_DEAD_LETTER,
    JOB_STATUS_CANCELLED,
)

ATTEMPT_STATUS_SUCCEEDED = "succeeded"
ATTEMPT_STATUS_FAILED = "failed"
ATTEMPT_STATUS_TIMEOUT = "timeout"
ATTEMPT_STATUS_LEASE_EXPIRED = "lease_expired"
ATTEMPT_STATUSES = (
    ATTEMPT_STATUS_SUCCEEDED,
    ATTEMPT_STATUS_FAILED,
    ATTEMPT_STATUS_TIMEOUT,
    ATTEMPT_STATUS_LEASE_EXPIRED,
)

SECRET_HEADER_NAMES = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
    }
)
