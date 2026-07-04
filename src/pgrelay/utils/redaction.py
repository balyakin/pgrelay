"""Sensitive key redaction helpers."""

from collections.abc import Mapping
from typing import Any

from pgrelay.constants import SECRET_HEADER_NAMES

REDACTED_VALUE = "<redacted>"


def redact_headers(headers: Mapping[str, Any]) -> dict[str, Any]:
    """Return headers with secret values replaced."""
    redacted: dict[str, Any] = {}
    for key, value in headers.items():
        if key.lower() in SECRET_HEADER_NAMES:
            redacted[key] = REDACTED_VALUE
        else:
            redacted[key] = value
    return redacted


def redact_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively redact secret fields from a mapping."""
    redacted: dict[str, Any] = {}
    for key, item in value.items():
        lowered = str(key).lower()
        if lowered in SECRET_HEADER_NAMES or lowered in {"payload", "headers", "metadata"}:
            redacted[key] = REDACTED_VALUE
        elif isinstance(item, Mapping):
            redacted[key] = redact_mapping(item)
        elif isinstance(item, list):
            redacted[key] = [redact_mapping(child) if isinstance(child, Mapping) else child for child in item]
        else:
            redacted[key] = item
    return redacted
