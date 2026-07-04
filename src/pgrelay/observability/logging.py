"""Structlog configuration with redaction."""

import logging
from collections.abc import Mapping, MutableMapping
from typing import Any

import structlog

from pgrelay.utils.redaction import redact_mapping


def redact_event(_logger: Any, _method_name: str, event_dict: MutableMapping[str, Any]) -> Mapping[str, Any]:
    """Redact sensitive fields from structlog events."""
    return redact_mapping(event_dict)


def setup_logging(level: str) -> None:
    """Configure structlog for JSON logs."""
    logging.basicConfig(level=level.upper(), format="%(message)s")
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.add_log_level,
            structlog.processors.format_exc_info,
            redact_event,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
