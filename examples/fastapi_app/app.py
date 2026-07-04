"""Example FastAPI app using PgRelay."""

from examples.fastapi_app.db import SessionFactory, settings
from examples.fastapi_app.models import WebhookEvent
from fastapi import FastAPI
from pydantic import BaseModel, Field

from pgrelay.sdk.client import PgRelayClient

app = FastAPI(title="PgRelay example")
client = PgRelayClient(settings)


class CreateEventRequest(BaseModel):
    """Example create event request."""

    event_id: str = Field(min_length=1)


@app.post("/events")
async def create_event(request: CreateEventRequest) -> dict[str, str]:
    """Create a domain event and enqueue a webhook in one transaction."""
    async with SessionFactory() as session, session.begin():
        session.add(WebhookEvent(event_id=request.event_id, status="created"))
        await client.enqueue_http(
            session=session,
            name="example.webhook",
            url="https://example.com/webhook",
            json_body={"event_id": request.event_id},
            idempotency_key=f"event:{request.event_id}",
        )
    return {"status": "created"}
