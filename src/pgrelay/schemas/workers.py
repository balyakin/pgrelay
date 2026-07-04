"""Worker schemas."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class WorkerResponse(BaseModel):
    """Worker heartbeat response."""

    model_config = ConfigDict(from_attributes=True)

    worker_id: str
    queues: list[str]
    hostname: str
    started_at: datetime
    last_heartbeat_at: datetime
    alive: bool
