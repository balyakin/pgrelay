"""API CLI command."""

import uvicorn

from pgrelay.api.app import create_app
from pgrelay.config.settings import load_settings
from pgrelay.observability.logging import setup_logging


def run_api() -> None:
    """Run the FastAPI admin API."""
    settings = load_settings()
    settings.validate_runtime()
    setup_logging(settings.log_level)
    uvicorn.run(create_app(settings=settings), host=settings.api_host, port=settings.api_port)
