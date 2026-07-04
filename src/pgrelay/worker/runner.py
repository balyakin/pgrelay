"""Asyncio worker runner."""

import asyncio
import socket
import time

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pgrelay.config.settings import Settings
from pgrelay.observability.metrics import (
    CLAIM_BATCH_SIZE,
    JOB_DURATION_SECONDS,
    JOBS_CLAIMED_TOTAL,
    JOBS_COMPLETED_TOTAL,
    WORKER_HEARTBEAT_TIMESTAMP_SECONDS,
)
from pgrelay.repositories.jobs import JobRepository, JobRow
from pgrelay.repositories.workers import WorkerRepository
from pgrelay.services.purge import PurgeService
from pgrelay.utils.ids import generate_worker_id
from pgrelay.worker.backoff import calculate_retry_delay_seconds
from pgrelay.worker.dispatcher import ExecutorResult, JobDispatcher
from pgrelay.worker.handlers import HandlerRegistry
from pgrelay.worker.heartbeat import run_lease_heartbeat
from pgrelay.worker.http_executor import HttpJobExecutor, create_http_client
from pgrelay.worker.python_executor import PythonHandlerExecutor
from pgrelay.worker.recovery import LeaseRecovery


class WorkerRunner:
    """Direct asyncio worker process."""

    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        handler_registry: HandlerRegistry | None = None,
    ) -> None:
        """Initialize the worker runner."""
        self.settings = settings
        self.session_factory = session_factory
        self.handler_registry = handler_registry or HandlerRegistry()
        self.job_repository = JobRepository()
        self.worker_repository = WorkerRepository()
        self.recovery = LeaseRecovery(self.job_repository, settings)
        self.purge_service = PurgeService(self.job_repository, settings)
        self.worker_id = generate_worker_id(settings.worker_id_prefix)
        self._stopping = asyncio.Event()
        self._inflight: dict[asyncio.Task[None], JobRow] = {}
        self._logger = structlog.get_logger(__name__)

    def request_stop(self) -> None:
        """Ask the runner to stop claiming new jobs."""
        self._stopping.set()

    async def run(self) -> None:
        """Run the worker until stopped or cancelled."""
        limits = httpx.Limits(
            max_connections=self.settings.http_max_connections,
            max_keepalive_connections=self.settings.http_max_keepalive_connections,
        )
        async with create_http_client(self.settings, limits) as client:
            dispatcher = JobDispatcher(
                HttpJobExecutor(client, self.settings),
                PythonHandlerExecutor(self.handler_registry),
            )
            purge_task = asyncio.create_task(self._purge_loop())
            try:
                await self._register_worker()
                await self._claim_loop(dispatcher)
            finally:
                self.request_stop()
                purge_task.cancel()
                await self._await_cancelled(purge_task)
                await self._shutdown_inflight()

    async def _claim_loop(self, dispatcher: JobDispatcher) -> None:
        while not self._stopping.is_set():
            try:
                await self._heartbeat_worker()
                async with self.session_factory() as session:
                    await self.recovery.recover(session)
                    free_slots = self.settings.worker_concurrency - len(self._inflight)
                    batch_size = min(self.settings.worker_batch_size, max(0, free_slots))
                    jobs = []
                    if batch_size > 0:
                        jobs = await self.job_repository.claim_jobs(
                            session,
                            worker_id=self.worker_id,
                            queue_names=self.settings.get_queue_names(),
                            batch_size=batch_size,
                            lease_seconds=self.settings.worker_lease_seconds,
                        )
                    await session.commit()
            except Exception as exc:
                self._logger.exception(
                    "worker_claim_loop_error",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                await asyncio.sleep(self.settings.worker_poll_interval_seconds)
                continue
            CLAIM_BATCH_SIZE.observe(len(jobs))
            for job in jobs:
                JOBS_CLAIMED_TOTAL.labels(queue=job.queue_name).inc()
                task = asyncio.create_task(self._execute_job(dispatcher, job))
                self._inflight[task] = job
                task.add_done_callback(self._on_inflight_done)
            if len(self._inflight) >= self.settings.worker_concurrency:
                await self._wait_for_one_inflight()
            elif not jobs:
                await asyncio.sleep(self.settings.worker_poll_interval_seconds)

    async def _execute_job(self, dispatcher: JobDispatcher, job: JobRow) -> None:
        execute_task = asyncio.create_task(self._dispatch_job(dispatcher, job))
        heartbeat_task = asyncio.create_task(
            run_lease_heartbeat(
                session_factory=self.session_factory,
                repository=self.job_repository,
                job_id=job.id,
                worker_id=self.worker_id,
                lease_seconds=self.settings.worker_lease_seconds,
                execution_task=execute_task,
            )
        )
        try:
            try:
                result = await execute_task
            except Exception as exc:
                result = self._exception_result(exc)
            lease_owned = await self._finish_heartbeat(heartbeat_task)
            if lease_owned:
                await self._record_result(job, result)
        except asyncio.CancelledError:
            heartbeat_task.cancel()
            await self._await_cancelled(heartbeat_task)
            try:
                await self._return_job_to_pending(job)
            except Exception as exc:
                self._logger.exception(
                    "worker_return_to_pending_failed",
                    job_id=str(job.id),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            raise
        finally:
            heartbeat_task.cancel()
            await self._await_cancelled(heartbeat_task)

    async def _dispatch_job(self, dispatcher: JobDispatcher, job: JobRow) -> ExecutorResult:
        if job.kind != "http":
            return await dispatcher.dispatch(job)
        started = time.monotonic()
        try:
            async with asyncio.timeout(job.timeout_seconds + 1):
                return await dispatcher.dispatch(job)
        except TimeoutError as exc:
            return ExecutorResult(
                outcome="timeout",
                error_type=type(exc).__name__,
                error_message="HTTP job timed out",
                response_status=None,
                response_body_preview=None,
                duration_ms=max(0, int((time.monotonic() - started) * 1000)),
            )

    def _exception_result(self, exc: Exception) -> ExecutorResult:
        return ExecutorResult(
            outcome="retryable_failure",
            error_type=type(exc).__name__,
            error_message=str(exc)[:2000],
            response_status=None,
            response_body_preview=None,
            duration_ms=0,
        )

    async def _record_result(self, job: JobRow, result: ExecutorResult) -> None:
        async with self.session_factory() as session:
            if result.outcome == "succeeded":
                updated = await self.job_repository.complete_job(
                    session,
                    job=job,
                    worker_id=self.worker_id,
                    duration_ms=result.duration_ms,
                    response_status=result.response_status,
                    response_body_preview=result.response_body_preview,
                )
            else:
                updated = await self.job_repository.fail_job(
                    session,
                    job=job,
                    worker_id=self.worker_id,
                    retryable=result.outcome in {"retryable_failure", "timeout"},
                    timed_out=result.outcome == "timeout",
                    retry_delay_seconds=calculate_retry_delay_seconds(job.attempt_count, self.settings),
                    duration_ms=result.duration_ms,
                    error_type=result.error_type,
                    error_message=result.error_message,
                    response_status=result.response_status,
                    response_body_preview=result.response_body_preview,
                )
            await session.commit()
        if updated:
            JOBS_COMPLETED_TOTAL.labels(queue=job.queue_name, outcome=result.outcome).inc()
            JOB_DURATION_SECONDS.labels(queue=job.queue_name, kind=job.kind).observe(result.duration_ms / 1000)

    async def _finish_heartbeat(self, heartbeat_task: asyncio.Task[bool]) -> bool:
        if heartbeat_task.done():
            try:
                return heartbeat_task.result()
            except asyncio.CancelledError:
                return False
            except Exception as exc:
                self._logger.exception(
                    "lease_heartbeat_failed",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                return False
        heartbeat_task.cancel()
        await self._await_cancelled(heartbeat_task)
        return True

    async def _return_job_to_pending(self, job: JobRow) -> None:
        async with self.session_factory() as session:
            await self.job_repository.return_leased_job_to_pending(session, job_id=job.id, worker_id=self.worker_id)
            await session.commit()

    async def _register_worker(self) -> None:
        async with self.session_factory() as session:
            await self.worker_repository.register_worker(
                session,
                worker_id=self.worker_id,
                queues=self.settings.get_queue_names(),
                hostname=socket.gethostname(),
            )
            await session.commit()
        WORKER_HEARTBEAT_TIMESTAMP_SECONDS.labels(worker_id=self.worker_id).set_to_current_time()

    async def _heartbeat_worker(self) -> None:
        async with self.session_factory() as session:
            updated = await self.worker_repository.heartbeat(session, worker_id=self.worker_id)
            if not updated:
                await self.worker_repository.register_worker(
                    session,
                    worker_id=self.worker_id,
                    queues=self.settings.get_queue_names(),
                    hostname=socket.gethostname(),
                )
            await session.commit()
        WORKER_HEARTBEAT_TIMESTAMP_SECONDS.labels(worker_id=self.worker_id).set_to_current_time()

    async def _purge_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                async with self.session_factory() as session:
                    await self.purge_service.purge_batch(session)
                    await session.commit()
            except Exception as exc:
                self._logger.exception(
                    "worker_purge_loop_error",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            await asyncio.sleep(max(60.0, self.settings.worker_poll_interval_seconds))

    async def _wait_for_one_inflight(self) -> None:
        if self._inflight:
            await asyncio.wait(self._inflight.keys(), return_when=asyncio.FIRST_COMPLETED)

    async def _shutdown_inflight(self) -> None:
        if not self._inflight:
            return
        done, pending = await asyncio.wait(self._inflight.keys(), timeout=self.settings.worker_shutdown_grace_seconds)
        for task in done:
            job = self._inflight.pop(task, None)
            if job is not None:
                self._consume_inflight_result(task, job)
        for task in pending:
            task.cancel()
        for task in pending:
            await self._await_cancelled(task)

    def _on_inflight_done(self, task: asyncio.Task[None]) -> None:
        job = self._inflight.pop(task, None)
        self._consume_inflight_result(task, job)

    def _consume_inflight_result(self, task: asyncio.Task[None], job: JobRow | None) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            if job is not None:
                self._logger.info("worker_job_task_cancelled", job_id=str(job.id))
        except Exception as exc:
            self._logger.exception(
                "worker_job_task_failed",
                job_id=None if job is None else str(job.id),
                error_type=type(exc).__name__,
                error_message=str(exc),
            )

    async def _await_cancelled(self, task: asyncio.Task[object]) -> None:
        try:
            await task
        except asyncio.CancelledError:
            return
        except Exception as exc:
            self._logger.exception(
                "worker_cancelled_task_failed",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
