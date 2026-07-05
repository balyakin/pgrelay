"""Worker CLI command."""

import asyncio

from pgrelay.config.settings import load_settings
from pgrelay.db.session import create_engine, create_session_factory
from pgrelay.observability.logging import setup_logging
from pgrelay.worker.handlers import HandlerRegistry
from pgrelay.worker.runner import WorkerRunner
from pgrelay.worker.signals import install_signal_handlers


async def run_worker_async(handler_registry: HandlerRegistry | None = None) -> None:
    """Run the worker asynchronously."""
    settings = load_settings()
    settings.validate_runtime()
    setup_logging(settings.log_level)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    runner = WorkerRunner(settings=settings, session_factory=session_factory, handler_registry=handler_registry)
    install_signal_handlers(runner)
    try:
        await runner.run()
    finally:
        await engine.dispose()


def run_worker() -> None:
    """Run the worker process."""
    asyncio.run(run_worker_async())
