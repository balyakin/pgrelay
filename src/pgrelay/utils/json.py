"""JSON helpers for deterministic payload handling."""

import hashlib
import json
from typing import Any


def canonical_json_dumps(value: Any) -> str:
    """Serialize a JSON-compatible value deterministically."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def json_size_bytes(value: Any) -> int:
    """Return UTF-8 byte length for canonical JSON."""
    return len(canonical_json_dumps(value).encode("utf-8"))


def sha256_json(value: Any) -> str:
    """Return SHA-256 hex digest for canonical JSON."""
    return hashlib.sha256(canonical_json_dumps(value).encode("utf-8")).hexdigest()
