"""Job dispatching across executors."""

from dataclasses import dataclass
from typing import Literal, Protocol

from pgrelay.repositories.jobs import JobRow

ExecutorOutcome = Literal["succeeded", "retryable_failure", "permanent_failure", "timeout"]


@dataclass(frozen=True, slots=True)
class ExecutorResult:
    """Normalized executor result."""

    outcome: ExecutorOutcome
    error_type: str | None
    error_message: str | None
    response_status: int | None
    response_body_preview: str | None
    duration_ms: int


class JobExecutor(Protocol):
    """Executor protocol."""

    async def execute(self, job: JobRow) -> ExecutorResult:
        """Execute a job."""


class JobDispatcher:
    """Dispatch jobs by kind."""

    def __init__(self, http_executor: JobExecutor, handler_executor: JobExecutor) -> None:
        """Initialize dispatcher."""
        self.http_executor = http_executor
        self.handler_executor = handler_executor

    async def dispatch(self, job: JobRow) -> ExecutorResult:
        """Dispatch a job to its configured executor."""
        if job.kind == "http":
            return await self.http_executor.execute(job)
        if job.kind == "handler":
            return await self.handler_executor.execute(job)
        return ExecutorResult(
            outcome="permanent_failure",
            error_type="unknown_job_kind",
            error_message="Unknown job kind",
            response_status=None,
            response_body_preview=None,
            duration_ms=0,
        )
