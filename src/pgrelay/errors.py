"""Domain errors and API status mapping."""

from typing import ClassVar


class PgRelayError(Exception):
    """Base PgRelay domain error."""

    error_code: ClassVar[str] = "pgrelay_error"


class ValidationError(PgRelayError):
    """Raised when a request violates PgRelay validation rules."""

    error_code: ClassVar[str] = "validation_error"


class RepositoryError(PgRelayError):
    """Raised when a database operation fails."""

    error_code: ClassVar[str] = "repository_error"


class DatabaseUnavailableError(PgRelayError):
    """Raised when PostgreSQL cannot be reached."""

    error_code: ClassVar[str] = "database_unavailable"


class JobNotFoundError(PgRelayError):
    """Raised when a job does not exist."""

    error_code: ClassVar[str] = "job_not_found"


class QueueNotFoundError(PgRelayError):
    """Raised when a queue does not exist."""

    error_code: ClassVar[str] = "queue_not_found"


class JobStateConflictError(PgRelayError):
    """Raised when a job state transition is not allowed."""

    error_code: ClassVar[str] = "job_state_conflict"


class PermanentJobError(PgRelayError):
    """Raised when job execution fails permanently."""

    error_code: ClassVar[str] = "permanent_job_error"


class RetryableJobError(PgRelayError):
    """Raised when job execution should be retried."""

    error_code: ClassVar[str] = "retryable_job_error"


ERROR_STATUS_CODES: dict[str, int] = {
    "validation_error": 422,
    "database_unavailable": 503,
    "job_not_found": 404,
    "queue_not_found": 404,
    "job_state_conflict": 409,
    "permanent_job_error": 422,
    "retryable_job_error": 500,
    "repository_error": 500,
    "pgrelay_error": 500,
}
