"""Utility and handler tests."""

import asyncio

from pgrelay.config.settings import Settings
from pgrelay.errors import PermanentJobError, RetryableJobError, ValidationError
from pgrelay.utils.ids import generate_trace_id, generate_worker_id
from pgrelay.utils.json import sha256_json
from pgrelay.utils.redaction import REDACTED_VALUE, redact_mapping
from pgrelay.worker.backoff import calculate_retry_delay_seconds
from pgrelay.worker.dispatcher import ExecutorResult, JobDispatcher
from pgrelay.worker.handlers import HandlerRegistry
from pgrelay.worker.python_executor import PythonHandlerExecutor
from tests.conftest import make_job_row


async def test_handler_registry_registers_async_decorator() -> None:
    """HandlerRegistry registers async handlers with a decorator."""
    # ARRANGE
    registry = HandlerRegistry()

    @registry.register("send")
    async def send(payload: dict[str, object]) -> dict[str, object]:
        return payload

    # ACT
    handler = registry.get("send")

    # ASSERT
    assert await handler({"ok": True}) == {"ok": True}


def test_handler_registry_rejects_duplicate_and_sync_handler() -> None:
    """HandlerRegistry rejects duplicate names and sync functions."""
    # ARRANGE
    registry = HandlerRegistry()

    async def first(payload: dict[str, object]) -> None:
        await asyncio.sleep(0)

    def sync_handler(payload: dict[str, object]) -> None:
        return None

    # ACT
    registry.register("job", first)

    # ASSERT
    try:
        registry.register("job", first)
    except ValidationError:
        duplicate = True
    else:
        duplicate = False
    try:
        registry.register("sync", sync_handler)  # type: ignore[arg-type]
    except ValidationError:
        sync_rejected = True
    else:
        sync_rejected = False
    assert duplicate is True
    assert sync_rejected is True


async def test_python_executor_success_missing_timeout_and_exception() -> None:
    """PythonHandlerExecutor classifies handler outcomes."""
    # ARRANGE
    registry = HandlerRegistry()

    @registry.register("success")
    async def success(payload: dict[str, object]) -> None:
        await asyncio.sleep(0)

    @registry.register("fail")
    async def fail(payload: dict[str, object]) -> None:
        raise ValueError("bad")

    @registry.register("retry")
    async def retry(payload: dict[str, object]) -> None:
        raise RetryableJobError("try again")

    @registry.register("permanent")
    async def permanent(payload: dict[str, object]) -> None:
        raise PermanentJobError("stop")

    @registry.register("slow")
    async def slow(payload: dict[str, object]) -> None:
        await asyncio.sleep(1)

    executor = PythonHandlerExecutor(registry)

    # ACT
    succeeded = await executor.execute(make_job_row(kind="handler", name="success"))
    missing = await executor.execute(make_job_row(kind="handler", name="missing"))
    failed = await executor.execute(make_job_row(kind="handler", name="fail"))
    retryable = await executor.execute(make_job_row(kind="handler", name="retry"))
    permanent_failed = await executor.execute(make_job_row(kind="handler", name="permanent"))
    timed_out = await executor.execute(make_job_row(kind="handler", name="slow", timeout_seconds=0.01))

    # ASSERT
    assert succeeded.outcome == "succeeded"
    assert missing.outcome == "permanent_failure"
    assert failed.error_type == "ValueError"
    assert retryable.outcome == "retryable_failure"
    assert permanent_failed.outcome == "permanent_failure"
    assert timed_out.outcome == "timeout"


async def test_dispatcher_routes_jobs_and_handles_unknown_kind() -> None:
    """JobDispatcher routes by job kind and classifies unknown kinds."""

    # ARRANGE
    class FakeExecutor:
        async def execute(self, job: object) -> ExecutorResult:
            return ExecutorResult("succeeded", None, None, None, None, 1)

    dispatcher = JobDispatcher(FakeExecutor(), FakeExecutor())

    # ACT
    http_result = await dispatcher.dispatch(make_job_row(kind="http"))
    handler_result = await dispatcher.dispatch(make_job_row(kind="handler"))
    unknown_result = await dispatcher.dispatch(make_job_row(kind="unknown"))

    # ASSERT
    assert http_result.outcome == "succeeded"
    assert handler_result.outcome == "succeeded"
    assert unknown_result.outcome == "permanent_failure"


def test_backoff_ids_json_and_redaction(settings: Settings) -> None:
    """Utility helpers return deterministic and redacted values."""
    # ARRANGE
    configured = settings.model_copy(update={"retry_jitter_ratio": 0.0})

    # ACT
    delay = calculate_retry_delay_seconds(3, configured)
    trace_id = generate_trace_id()
    worker_id = generate_worker_id("worker")
    digest = sha256_json({"b": 2, "a": 1})
    redacted = redact_mapping({"payload": {"secret": True}, "nested": {"Authorization": "token"}})

    # ASSERT
    assert delay == 8
    assert len(trace_id) == 32
    assert worker_id.startswith("worker-")
    assert len(digest) == 64
    assert redacted["payload"] == REDACTED_VALUE
    assert redacted["nested"]["Authorization"] == REDACTED_VALUE
