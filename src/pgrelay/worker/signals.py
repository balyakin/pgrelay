"""Worker signal handling."""

import asyncio
import signal

from pgrelay.worker.runner import WorkerRunner


def install_signal_handlers(runner: WorkerRunner) -> None:
    """Install SIGTERM and SIGINT handlers for the worker runner."""
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, runner.request_stop)
