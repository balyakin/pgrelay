"""Minimal Python handler worker example.

RetryableJobError asks PgRelay to retry. Unexpected handler exceptions move the job to dead_letter.
"""

import asyncio
from typing import Any

import structlog

from pgrelay.cli.commands_worker import run_worker_async
from pgrelay.errors import RetryableJobError
from pgrelay.worker.handlers import HandlerRegistry

logger = structlog.get_logger(__name__)
registry = HandlerRegistry()


@registry.register("orders.recalculate_totals")
async def recalculate_order_totals(payload: dict[str, Any]) -> None:
    """Process one order totals recalculation job."""
    order_id = str(payload["order_id"])
    failure_mode = payload.get("failure_mode")
    if failure_mode == "transient":
        raise RetryableJobError("temporary downstream failure")
    if failure_mode == "permanent":
        raise RuntimeError("unexpected handler failure")
    logger.info("order_totals_recalculated", order_id=order_id)


if __name__ == "__main__":
    asyncio.run(run_worker_async(handler_registry=registry))
