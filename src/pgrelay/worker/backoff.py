"""Retry backoff calculation."""

import random

from pgrelay.config.settings import Settings


def calculate_retry_delay_seconds(attempt_count: int, settings: Settings) -> int:
    """Calculate exponential retry delay with jitter."""
    exponent = max(attempt_count - 1, 0)
    base_delay = min(settings.retry_base_seconds * (2**exponent), settings.retry_max_seconds)
    jitter = base_delay * settings.retry_jitter_ratio
    delayed = base_delay + random.uniform(-jitter, jitter)
    return max(1, min(settings.retry_max_seconds, int(delayed)))
