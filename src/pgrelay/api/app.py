"""FastAPI application factory."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from pgrelay.api.routers import health, jobs, queues, stats, workers
from pgrelay.config.settings import Settings, load_settings
from pgrelay.db.session import create_engine, create_session_factory
from pgrelay.errors import ERROR_STATUS_CODES, PgRelayError
from pgrelay.observability.logging import setup_logging
from pgrelay.repositories.attempts import AttemptRepository
from pgrelay.repositories.jobs import JobRepository
from pgrelay.repositories.queues import QueueRepository
from pgrelay.repositories.stats import StatsRepository
from pgrelay.repositories.workers import WorkerRepository
from pgrelay.schemas.api_errors import ApiError, ApiErrorResponse
from pgrelay.services.enqueue import EnqueueService
from pgrelay.services.jobs import JobService
from pgrelay.services.queues import QueueService
from pgrelay.services.stats import StatsService
from pgrelay.services.workers import WorkerService


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create a configured FastAPI app."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        runtime_settings = settings or load_settings()
        runtime_settings.validate_runtime()
        setup_logging(runtime_settings.log_level)
        engine = create_engine(runtime_settings)
        session_factory = create_session_factory(engine)
        queue_repository = QueueRepository()
        job_repository = JobRepository(queue_repository)
        attempt_repository = AttemptRepository()
        worker_repository = WorkerRepository()
        stats_repository = StatsRepository()
        app.state.settings = runtime_settings
        app.state.engine = engine
        app.state.session_factory = session_factory
        app.state.enqueue_service = EnqueueService(job_repository, runtime_settings)
        app.state.job_service = JobService(job_repository, attempt_repository)
        app.state.queue_service = QueueService(queue_repository)
        app.state.worker_service = WorkerService(worker_repository)
        app.state.stats_service = StatsService(stats_repository)
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(title="PgRelay Admin API", version="0.1.0", lifespan=lifespan)
    app.add_exception_handler(PgRelayError, _pgrelay_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, _request_validation_handler)  # type: ignore[arg-type]
    app.add_exception_handler(HTTPException, _http_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(Exception, _unexpected_exception_handler)
    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(queues.router)
    app.include_router(stats.router)
    app.include_router(workers.router)
    return app


def _error_response(code: str, message: str, details: dict[str, Any] | None, status_code: int) -> JSONResponse:
    body = ApiErrorResponse(error=ApiError(code=code, message=message, details=details))
    return JSONResponse(status_code=status_code, content=jsonable_encoder(body))


async def _pgrelay_error_handler(_request: Request, exc: PgRelayError) -> JSONResponse:
    """Handle PgRelay domain errors."""
    code = exc.error_code
    return _error_response(code, str(exc), None, ERROR_STATUS_CODES.get(code, 500))


async def _request_validation_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
    """Handle FastAPI request validation errors."""
    return _error_response(
        "validation_error", "Request validation failed", {"errors": jsonable_encoder(exc.errors())}, 422
    )


async def _http_exception_handler(_request: Request, exc: HTTPException) -> JSONResponse:
    """Handle HTTP exceptions with the common envelope."""
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        return JSONResponse(status_code=exc.status_code, content=exc.detail)
    return _error_response("http_error", str(exc.detail), None, exc.status_code)


async def _unexpected_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    """Handle unexpected errors."""
    return _error_response("pgrelay_error", "Internal server error", {"type": type(exc).__name__}, 500)
