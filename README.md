# PgRelay

PostgreSQL-backed transactional outbox and reliable background jobs for Python services.

[![CI][badge-ci]][ci]
[![PyPI][badge-pypi]][pypi]
[![Python][badge-python]][pypi]
[![PostgreSQL][badge-postgres]][production]
[![Coverage gate][badge-coverage]][ci]
[![Checks][badge-checks]][ci]
[![Docker smoke][badge-docker]][ci]
[![License: MIT][badge-license]][license]

CI runs Ruff, mypy, pytest with an 85% coverage gate across Python 3.12/3.13 and PostgreSQL 15/16/17, plus a
Docker Compose smoke test. The test suite covers enqueue, claim, leases, heartbeat, idempotency, purge, replay/cancel,
retry/DLQ, HTTP workers, process recovery, API jobs, and runner behavior.

PgRelay is for the awkward space between "just call the webhook in the request" and "we need a separate queueing
platform". It stores jobs in the same PostgreSQL database your application already commits to, then runs them from a
small asyncio worker. The SDK writes into your existing SQLAlchemy `AsyncSession`, so a domain row and the job that
publishes it can commit or roll back together.

PgRelay 0.1.0 is not a `LISTEN/NOTIFY` queue. Workers poll PostgreSQL, claim ready rows with `FOR UPDATE SKIP LOCKED`,
and use durable leases for recovery after worker crashes.

It is intentionally not an exactly-once system. PgRelay gives you at-least-once delivery, retries, leases, dead-letter
jobs, replay, and enough operator API to see what happened. Any external side effect still needs an idempotency key on
the receiving side.

![PgRelay admin console concept](docs/admin-console-concept.svg)

The image is a concept for a possible browser admin console. PgRelay currently ships the admin API and CLI; a separate
browser console is not part of this release.

## What You Get

- Transactional enqueue from a caller-owned `sqlalchemy.ext.asyncio.AsyncSession`
- HTTP jobs and Python handler jobs
- PostgreSQL queues with pause/resume and per-queue concurrency limits
- Worker leases, lease heartbeats, expired lease recovery, retries, and dead-letter state
- Idempotency keys and dedupe keys
- Replay and cancel operations for job recovery
- FastAPI admin API with health, readiness, jobs, attempts, queues, stats, and worker heartbeats
- Typer CLI for migrations, workers, replay, drain, purge, and environment checks
- Worker counters and histograms built with `prometheus-client`

## When It Fits

PgRelay is a good fit when your service already uses PostgreSQL and SQLAlchemy async, and the job is part of the same
business fact you are committing. Typical examples are webhook delivery, search indexing, billing sync, email requests,
or a small internal handler that should run after a transaction is durable.

It is not trying to replace Kafka, Temporal, Celery, SQS, or a workflow engine. If you need fan-out streams,
long-running durable workflows, cross-language workers, hosted scheduling, or exactly-once effects in another system,
use the tool built for that job.

For trade-offs against nearby alternatives, see
[Comparison with Celery, Taskiq, Procrastinate, and PgQueuer](docs/comparison.md).

For operational limits, sizing notes, polling cost, retention, vacuum, and production checks, see
[Production Readiness and Limits](docs/production.md).

## Quick Start

Requirements:

- Python 3.12 or 3.13
- PostgreSQL 15+
- Poetry 2.x for local development
- Docker Compose if you want the one-command local stack

Run the local stack:

```bash
poetry install --with dev
docker compose up --build
```

The compose file starts PostgreSQL, runs migrations, then launches the admin API and one worker. The API listens on
`http://localhost:8090`.

Check it:

```bash
curl http://localhost:8090/healthz
curl http://localhost:8090/readyz
curl -H "Authorization: Bearer dev-token-change-me" http://localhost:8090/v1/jobs
```

FastAPI's OpenAPI UI is available at:

```text
http://localhost:8090/docs
```

The default token is for local development only. Set `PGRELAY_API_AUTH_TOKENS` before running anything that is reachable
outside your machine. Use `PGRELAY_API_READ_ONLY_AUTH_TOKENS` for monitoring clients that should only read admin API
state.

## Enqueue Inside Your Transaction

The important detail is ownership: PgRelay does not call `commit()` and does not close your session. Your application
keeps the transaction boundary.

```python
from pgrelay.sdk.client import PgRelayClient

client = PgRelayClient.from_env()

async with session_factory() as session:
    async with session.begin():
        session.add(order)

        await client.enqueue_http(
            session=session,
            name="order.webhook",
            url="https://example.com/webhooks/orders",
            json_body={"order_id": str(order.id)},
            idempotency_key=f"order-webhook:{order.id}",
            trace_id=request_id,
        )
```

If the transaction rolls back, the job rolls back with it. If the transaction commits, a worker can claim the job after
commit.

Python handler jobs use the same pattern:

```python
await client.enqueue_handler(
    session=session,
    name="orders.recalculate_totals",
    payload={"order_id": str(order.id)},
    queue_name="default",
    idempotency_key=f"recalculate:{order.id}",
)
```

Python handler jobs use an explicit process-local handler registry: each worker process must register the same handler
names in its application code. HTTP jobs do not use that registry; job state, attempts, leases, and queue state live in
PostgreSQL.

Run a custom worker for Python handler jobs:

```bash
PGRELAY_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/postgres \
  python examples/handler_worker.py
```

See [examples/handler_worker.py](examples/handler_worker.py) for the matching `orders.recalculate_totals` handler.

## CLI

```bash
pgrelay migrate upgrade
pgrelay migrate downgrade REVISION
pgrelay api
pgrelay worker
pgrelay replay JOB_ID
pgrelay replay JOB_ID --force
pgrelay drain default --timeout-seconds 300
pgrelay purge
pgrelay doctor
```

The CLI reads `PGRELAY_*` settings from the environment or `.env`.

## Admin API

Unauthenticated endpoints:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Process liveness |
| `GET` | `/readyz` | Database readiness |

Authenticated endpoints require `Authorization: Bearer <token>`:

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/jobs` | Enqueue a job through the admin API |
| `GET` | `/v1/jobs` | List jobs without payload fields |
| `GET` | `/v1/jobs/{job_id}` | Read one job with sensitive fields redacted |
| `GET` | `/v1/jobs/{job_id}/attempts` | Read attempt history |
| `POST` | `/v1/jobs/{job_id}/replay` | Create a fresh pending job from an existing one |
| `POST` | `/v1/jobs/{job_id}/cancel` | Cancel a pending job |
| `GET` | `/v1/queues` | List queues |
| `PUT` | `/v1/queues/{queue_name}` | Create or update a queue |
| `POST` | `/v1/queues/{queue_name}/pause` | Pause a queue |
| `POST` | `/v1/queues/{queue_name}/resume` | Resume a queue |
| `GET` | `/v1/stats` | Read queue and job stats |
| `GET` | `/v1/workers` | Read worker heartbeat rows |

Example:

```bash
curl -H "Authorization: Bearer dev-token-change-me" \
  "http://localhost:8090/v1/jobs?status=pending&limit=25"
```

## Configuration

PgRelay uses `pydantic-settings` with the `PGRELAY_` prefix. The sample file is [.env.example](.env.example).

The settings you will usually touch first:

| Setting | Why it matters |
| --- | --- |
| `PGRELAY_DATABASE_URL` | Runtime database URL. It must use `postgresql+asyncpg://`. |
| `PGRELAY_API_AUTH_TOKENS` | Comma-separated bearer tokens with read/write admin API access. Required in production. |
| `PGRELAY_API_READ_ONLY_AUTH_TOKENS` | Comma-separated bearer tokens for read-only admin API access. |
| `PGRELAY_WORKER_QUEUES` | Comma-separated queue names a worker should claim from. |
| `PGRELAY_WORKER_CONCURRENCY` | Maximum in-flight jobs per worker process. |
| `PGRELAY_WORKER_LEASE_SECONDS` | Lease duration before another worker may recover a stuck job. |
| `PGRELAY_HTTP_ALLOWED_HOSTS` | Allowlist for HTTP job targets. Required in production. |
| `PGRELAY_BLOCK_PRIVATE_NETWORK_TARGETS` | Blocks HTTP jobs from reaching private network targets by default. |
| `PGRELAY_RETENTION_SUCCEEDED_DAYS` | How long succeeded jobs are kept before purge. |
| `PGRELAY_RETENTION_DEAD_LETTER_DAYS` | How long dead-letter jobs are kept before purge. |

In production, do not use the development token from `docker-compose.yml`. PgRelay refuses to start with that token when
`PGRELAY_ENV=prod`.

## Job Lifecycle

```text
pending -> leased -> succeeded
                  -> pending      (retryable failure before max_attempts)
                  -> dead_letter  (max attempts reached or permanent failure)
pending -> cancelled
dead_letter/cancelled -> pending  (replay creates a new job id)
```

Workers poll PostgreSQL, claim pending jobs with row locks and `SKIP LOCKED`, then heartbeat the lease while the job
runs. If a worker dies, lease recovery returns the job to `pending` or moves it to `dead_letter` when attempts are
exhausted.

## Operational Notes

- Design receivers to handle duplicate delivery. At-least-once is the contract.
- Use stable `idempotency_key` values for effects that should not be queued twice.
- Keep HTTP job hosts allowlisted. The worker follows no redirects and blocks private network targets by default.
- Admin API job details redact payloads, headers, metadata, and response body previews.
- Watch dead-letter jobs. They are usually either a receiver problem, a bad payload, or a missing idempotency rule.
- Run more worker processes for throughput, but size the database pool so each worker has room to claim, heartbeat, and
  finish jobs.
- Treat payloads as operational data. Job list endpoints omit payloads, and detail endpoints redact payloads before
  returning jobs.

For state transitions, guarantees, and failure modes, see [Architecture](docs/architecture.md).
For operational sizing, polling cost, retention, vacuum, and scaling limits, see
[Production Readiness and Limits](docs/production.md).

## Project Status

This repository is currently at `0.1.0`. The core API, worker, SDK, migrations, and local Docker stack are present, but
the project should still be treated as young. Pin versions, test against your own failure modes, and expect the edges to
be sharper than a mature hosted queue.

Planned next steps are intentionally modest: optional PostgreSQL `LISTEN/NOTIFY` wakeups on top of polling, batch
enqueue, and OpenTelemetry integration.

## License

MIT. See [LICENSE](LICENSE).

## Development

This project was developed with AI assistance and is maintained by the author.

[badge-ci]: https://github.com/balyakin/pgrelay/actions/workflows/ci.yml/badge.svg
[badge-pypi]: https://img.shields.io/pypi/v/pgrelay.svg
[badge-python]: https://img.shields.io/badge/python-3.12%20%7C%203.13-blue
[badge-postgres]: https://img.shields.io/badge/postgresql-15%20%7C%2016%20%7C%2017-blue
[badge-coverage]: https://img.shields.io/badge/coverage%20gate-85%25-brightgreen
[badge-checks]: https://img.shields.io/badge/checks-ruff%20%7C%20mypy%20%7C%20pytest-brightgreen
[badge-docker]: https://img.shields.io/badge/docker-smoke%20test-brightgreen
[badge-license]: https://img.shields.io/badge/license-MIT-green.svg
[ci]: https://github.com/balyakin/pgrelay/actions/workflows/ci.yml
[license]: LICENSE
[pypi]: https://pypi.org/project/pgrelay/
[production]: docs/production.md
