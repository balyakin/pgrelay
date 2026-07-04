# PgRelay Architecture

PgRelay stores jobs in PostgreSQL and executes them from asyncio workers. The application enqueues a job in the same
transaction as its domain write, so commit and rollback decisions stay with the caller.

## Components

- SDK: validates enqueue requests and writes `pgrelay_job` rows through the caller's `AsyncSession`.
- Admin API: exposes job, queue, worker, stats, replay, cancel, health, and readiness operations.
- Worker: claims due jobs, heartbeats leases, executes HTTP or Python handler jobs, records attempts, and purges old
  rows.
- Repository layer: owns SQL for claim, state transitions, lease recovery, purge, and queue administration.

## Job States

```text
pending -> leased -> succeeded
                  -> pending      retryable failure before max_attempts
                  -> dead_letter  final attempt or permanent failure
pending -> cancelled
dead_letter/cancelled -> pending  replay creates a new job id
```

`claim_jobs` increments `attempt_count` when a worker leases a job. If shutdown cancels an in-flight job while the
worker still owns the lease, the job returns to `pending` and the claim attempt is rolled back.

## Delivery Guarantees

PgRelay is at-least-once. A job may run more than once after worker death, lease expiry, network retries, or replay.
External side effects must be idempotent. Use `idempotency_key` for enqueue de-duplication and send stable downstream
idempotency headers for HTTP targets.

PgRelay is not exactly-once. PostgreSQL protects job state transitions, but it cannot atomically coordinate with an
external HTTP receiver or arbitrary Python handler side effects.

## Claiming and Concurrency

Workers claim jobs with PostgreSQL row locks. Queue rows are locked before free slots are computed, so concurrent
workers cannot oversubscribe a queue's `concurrency_limit`. Pending job rows use `FOR UPDATE SKIP LOCKED`, which keeps
workers from blocking each other on individual jobs.

## Leases and Recovery

Each leased job has `locked_by` and `locked_until`. While the job runs, a heartbeat task extends the lease. A
different worker can recover expired leases and move jobs back to `pending` or to `dead_letter` when attempts are
exhausted.

If a worker exits cleanly, it cancels in-flight tasks and returns owned leases to `pending`. If the process is killed,
the database lease expires and another worker recovers it.

## Failure Modes

- Worker process killed: job remains `leased` until `locked_until`, then recovery requeues or dead-letters it.
- Handler raises retryable error or HTTP target returns retryable status: job goes back to `pending` with backoff.
- Handler raises permanent error or HTTP target returns permanent status: job moves to `dead_letter`.
- Worker loses lease heartbeat: the result is not recorded; lease recovery decides the next state.
- Admin cancel: only `pending` jobs can become `cancelled`.
- Purge: removes old `succeeded`, `dead_letter`, and `cancelled` jobs in batches.
- HTTP SSRF guard: worker TCP connect resolves and validates the peer address immediately before connecting.
