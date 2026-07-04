"""Python handler job executor."""

import asyncio
import time

from pgrelay.errors import PermanentJobError, RetryableJobError
from pgrelay.repositories.jobs import JobRow
from pgrelay.worker.dispatcher import ExecutorOutcome, ExecutorResult
from pgrelay.worker.handlers import HandlerRegistry


class PythonHandlerExecutor:
    """Executor for Python handler jobs."""

    def __init__(self, registry: HandlerRegistry) -> None:
        """Initialize the executor."""
        self.registry = registry

    async def execute(self, job: JobRow) -> ExecutorResult:
        """Execute one handler job."""
        started = time.monotonic()
        try:
            handler = self.registry.get(job.name)
        except KeyError as exc:
            return self._result("permanent_failure", type(exc).__name__, "Handler not registered", started)
        try:
            async with asyncio.timeout(job.timeout_seconds):
                await handler(job.payload)
            return self._result("succeeded", None, None, started)
        except TimeoutError as exc:
            return self._result("timeout", type(exc).__name__, "Handler timed out", started)
        except asyncio.CancelledError:
            raise
        except RetryableJobError as exc:
            return self._result("retryable_failure", type(exc).__name__, str(exc), started)
        except PermanentJobError as exc:
            return self._result("permanent_failure", type(exc).__name__, str(exc), started)
        except Exception as exc:
            return self._result("permanent_failure", type(exc).__name__, str(exc), started)

    def _result(
        self,
        outcome: ExecutorOutcome,
        error_type: str | None,
        error_message: str | None,
        started: float,
    ) -> ExecutorResult:
        return ExecutorResult(
            outcome=outcome,
            error_type=error_type,
            error_message=error_message,
            response_status=None,
            response_body_preview=None,
            duration_ms=max(0, int((time.monotonic() - started) * 1000)),
        )
