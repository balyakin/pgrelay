# PgRelay FastAPI Example

This example shows a domain row and PgRelay HTTP job created in the same `AsyncSession` transaction.

Run with the local compose database after migrations:

```bash
PGRELAY_DATABASE_URL=postgresql+asyncpg://pgrelay:pgrelay@localhost:5432/pgrelay uvicorn examples.fastapi_app.app:app --reload
```

Create an event:

```bash
curl -X POST http://localhost:8000/events -H "Content-Type: application/json" -d '{"event_id":"evt_1"}'
```

If the transaction rolls back, neither the domain row nor the outbox job is committed.
