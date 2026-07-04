"""Identifier helpers for traces and workers."""

import socket
import uuid


def generate_trace_id() -> str:
    """Return a new trace identifier."""
    return uuid.uuid4().hex


def generate_worker_id(prefix: str) -> str:
    """Return a worker identifier using hostname and random suffix."""
    hostname = socket.gethostname()
    suffix = uuid.uuid4().hex[:12]
    return f"{prefix}-{hostname}-{suffix}"
