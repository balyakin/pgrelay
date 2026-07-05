# Production Readiness and Limits

PgRelay is production-shaped, not production-proven at arbitrary scale. Treat the numbers below as capacity-planning
inputs, not vendor throughput claims. Benchmark on your own PostgreSQL instance, with your payload size, retention, job
duration, and worker count.

## When Not to Use PgRelay

Do not use PgRelay when:

- the queue is expected to be the central messaging backbone for many services;
- you need high-throughput fan-out streams;
- jobs may run for hours and require durable workflow state;
- workers must be written in multiple languages;
- losing PostgreSQL availability must not stop job processing;
- you need exactly-once external side effects;
- queue churn would compete with your primary OLTP workload.

## Delivery Guarantees

PgRelay provides at-least-once execution.

A job may be executed more than once when:

- a worker completes the external side effect but crashes before recording success;
- the lease expires while the handler is still running;
- the database connection is lost after the side effect has happened;
- an operator replays a job manually;
- a downstream target times out after processing the request.

Handlers and HTTP receivers must be idempotent.

## HTTP Job Security

For HTTP jobs:

- keep `PGRELAY_HTTP_ALLOWED_HOSTS` as narrow as possible;
- set `PGRELAY_HTTP_ALLOWED_HOSTS` before running with `PGRELAY_ENV=prod`;
- keep private network targets blocked unless explicitly needed;
- do not allow arbitrary user-provided URLs to become job targets;
- treat job payloads as operationally sensitive data;
- use read-only admin tokens for dashboards and monitoring;
- use receiver-side idempotency keys;
- set downstream timeouts low enough that leases do not expire during normal calls.

## Expected Throughput

PgRelay is usually bound by job duration first and PostgreSQL write churn second. A successful job creates at least:

- one `pgrelay_job` insert at enqueue time;
- one `pgrelay_job` update when the worker claims it;
- one `pgrelay_job` update when the worker records the result;
- one `pgrelay_attempt` insert for the recorded attempt.

Long-running jobs also write lease heartbeats. Retryable failures add more state updates and attempt rows.

A simple execution-capacity estimate is:

```text
execution_capacity_jobs_per_second ~= worker_processes * PGRELAY_WORKER_CONCURRENCY / average_job_seconds
```

Examples before database overhead, target throttling, retries, and network latency:

- 1 worker * concurrency 8 / 0.2s average job = at most 40 jobs/s.
- 4 workers * concurrency 16 / 0.5s average job = at most 128 jobs/s.
- 4 workers * concurrency 16 / 2.0s average job = at most 32 jobs/s.

Those examples are arithmetic capacity estimates, not measured PgRelay benchmarks. If the queue itself is your
bottleneck, measure claim latency, final-state update latency, PostgreSQL CPU, WAL volume, and autovacuum lag before
raising concurrency.

## Polling Cost

PgRelay 0.1.0 polls PostgreSQL. It does not yet wake workers with `LISTEN/NOTIFY`.

An idle worker with free slots runs roughly this every poll interval:

- update or re-register the worker heartbeat;
- recover one batch of expired leases;
- lock configured queue rows;
- claim due pending jobs;
- commit the heartbeat transaction and the claim transaction.

With the default `PGRELAY_WORKER_POLL_INTERVAL_SECONDS=1.0`, the rough idle query rate is:

```text
idle_sql_statements_per_second ~= worker_processes / poll_interval_seconds * 4
```

At 10 idle worker processes, that is roughly 40 SQL statements per second just to discover no work. At 100 idle worker
processes, it is roughly 400 statements per second plus commits. If the queue is often empty, prefer fewer idle worker
processes or a longer poll interval.

Polling latency is also the normal wakeup latency. A newly available job may wait up to one poll interval before a
worker claims it.

## Multi-Worker Behavior

Multiple workers can safely claim from the same queue. PgRelay uses short database transactions for claim and final
state updates; it does not hold a database transaction while the job executes.

Claiming works in two layers:

- queue rows are locked with `FOR UPDATE` while free slots are computed;
- pending job rows are claimed with `FOR UPDATE SKIP LOCKED`.

The queue row lock prevents workers from oversubscribing `pgrelay_queue.concurrency_limit`. `SKIP LOCKED` lets workers
skip job rows another worker already picked instead of blocking on them.

Adding workers helps when job execution is the bottleneck. It does not help when PostgreSQL is saturated by claim,
heartbeat, result, purge, or admin queries.

## Row Locks, Not Advisory Locks

PgRelay uses PostgreSQL row locks for claiming. It does not use advisory locks.

Row locks are the simpler fit here because:

- the lock target is the row being updated;
- lock lifetime is exactly the claim transaction;
- `SKIP LOCKED` is designed for queue-like consumers;
- committed job state remains visible in ordinary SQL after the lock is gone.

PgRelay uses row state for long ownership: `status`, `locked_by`, and `locked_until`. That lease is durable and can be
recovered by another worker after expiry. An advisory lock would not replace that lease row, because PgRelay still needs
visible ownership, timeout, recovery, and operator state.

## Indexes

The initial schema creates these job-related indexes:

- `idx_pgrelay_job_claim`: partial index for pending claims on `(queue_name, priority DESC, available_at, created_at)`.
- `idx_pgrelay_job_locked_expired`: partial index for expired leased jobs on `locked_until`.
- `idx_pgrelay_job_status_created`: status and creation-time index for admin filtering and recent lists.
- `idx_pgrelay_job_dedupe_key`: partial index for non-null dedupe keys.
- `uq_pgrelay_job_queue_idempotency_key`: partial unique index for idempotent enqueue per queue.
- `idx_pgrelay_job_replayed_from`: partial index for replay lineage.
- `idx_pgrelay_attempt_job_started`: attempt history by job.
- `idx_pgrelay_worker_last_heartbeat`: worker liveness ordering.

The claim path is indexed for the common case: many historical rows, a smaller active pending set, and queries by queue.
It will degrade if the pending set is huge, table statistics are stale, or final rows are kept forever.

Purge filters final jobs by `status` and `completed_at`. If you keep millions of final rows, test the purge plan. A
site-local partial index on final `(status, completed_at)` rows may be justified when purge becomes expensive.

## Row Count Rules of Thumb

Total row count matters less than active row count and bloat.

At around 100k total jobs, PgRelay should remain a normal PostgreSQL workload if:

- most old final rows are being purged;
- pending jobs are a bounded backlog, not permanent storage;
- `VACUUM` and `ANALYZE` keep up;
- admin endpoints avoid `include_total=true` on hot paths.

At around 1M total jobs, treat the database as a queue store that needs active care:

- use `GET /v1/stats?approximate=true` for stats pages;
- verify claim and purge plans with `EXPLAIN (ANALYZE, BUFFERS)`;
- lower retention or run catch-up purge before final rows dominate the table;
- monitor autovacuum and table bloat;
- consider a site-local archive or partitioning strategy if you need long history.

PgRelay is not intended to be the permanent event log. If the business requirement is to keep every event forever, copy
completed job data into an audit table or object storage before purge.

## Retention and Purge

Defaults:

- `PGRELAY_RETENTION_SUCCEEDED_DAYS=7`;
- `PGRELAY_RETENTION_DEAD_LETTER_DAYS=30`;
- `PGRELAY_PURGE_BATCH_SIZE=1000`.

The worker purge loop deletes one batch, then sleeps for at least 60 seconds. With the default batch size, one worker
process attempts at most 1,000 expired job deletes per minute. Every worker process has its own purge loop, so a large
worker fleet also adds repeated empty purge checks and possible delete contention.

Use `pgrelay purge` for catch-up cleanup. The CLI runs batches in separate transactions until the final partial batch.

Tune retention from observed write rate:

```text
retained_succeeded_rows ~= succeeded_jobs_per_day * retention_succeeded_days
retained_final_rows ~= retained_succeeded_rows + retained_dead_letter_rows + retained_cancelled_rows
```

If you complete 1M jobs/day and keep succeeded jobs for 7 days, PostgreSQL must carry roughly 7M succeeded rows before
dead letters, cancelled jobs, attempts, indexes, and bloat. That is not a small default queue table anymore.

## Vacuum and Analyze

PgRelay updates and deletes rows frequently. That creates dead tuples and changes planner statistics. PostgreSQL
autovacuum is part of the queue system, not optional background noise.

Watch the queue tables:

```sql
SELECT
    relname,
    n_live_tup,
    n_dead_tup,
    vacuum_count,
    autovacuum_count,
    analyze_count,
    autoanalyze_count
FROM pg_stat_user_tables
WHERE relname IN ('pgrelay_job', 'pgrelay_attempt');
```

If `n_dead_tup` keeps growing, claims slow down, or purge gets slower over time, tune autovacuum for the PgRelay tables
or reduce retention. For heavy churn, table-specific autovacuum settings are usually better than global changes.

## Backpressure

PgRelay has worker-side backpressure:

- `PGRELAY_WORKER_CONCURRENCY` caps in-flight jobs per worker process;
- `pgrelay_queue.concurrency_limit` caps leased jobs per queue across workers;
- queue pause/resume can stop new claims from a queue.

PgRelay 0.1.0 does not enforce producer-side backpressure. It will keep accepting enqueue writes as long as PostgreSQL
accepts the transaction.

Add application-side limits when any of these rise:

- pending depth per queue;
- oldest pending age;
- database pool wait time;
- claim latency;
- dead-letter rate;
- downstream HTTP 429/5xx rate.

Common producer policies are "reject new optional jobs", "drop duplicate low-value jobs before enqueue", "pause a
queue", or "return 503 before accepting more work that cannot meet its SLO".

## Pool Sizing

Each PgRelay process owns its own SQLAlchemy pool. Add those pools to your application's own PostgreSQL connections
when checking `max_connections`.

The worker validates:

```text
PGRELAY_DB_POOL_SIZE + PGRELAY_DB_MAX_OVERFLOW >= PGRELAY_WORKER_CONCURRENCY + 2
```

That is a minimum, not a universal sizing rule. A worker uses short sessions for claim, heartbeat, recovery, result
recording, and purge. It does not keep one database connection checked out for the whole job execution.

For HTTP jobs, also size:

```text
PGRELAY_HTTP_MAX_CONNECTIONS >= PGRELAY_WORKER_CONCURRENCY
```

For the database, start with:

```text
postgres_connections_needed >= application_pool
                              + api_processes * api_pool
                              + worker_processes * worker_pool
                              + maintenance_margin
```

Keep a margin for migrations, `pgrelay doctor`, `pgrelay purge`, psql, monitoring, and autovacuum workers.

## Operational Checklist

Before production:

- set `PGRELAY_ENV=prod`, real `PGRELAY_API_AUTH_TOKENS`, and `PGRELAY_HTTP_ALLOWED_HOSTS`;
- use `PGRELAY_API_READ_ONLY_AUTH_TOKENS` for monitoring clients;
- benchmark enqueue, claim, finish, retry, and purge on production-like PostgreSQL;
- set retention from expected jobs/day, not from defaults;
- alert on oldest pending age, dead-letter growth, worker heartbeat age, and PostgreSQL pool saturation;
- test worker kill, database restart, slow downstream HTTP, duplicate delivery, and replay;
- run `pgrelay doctor` after migrations and before rollout;
- document the idempotency key sent to every downstream side effect.

## PostgreSQL References

- [SELECT locking clause](https://www.postgresql.org/docs/15/sql-select.html#SQL-FOR-UPDATE-SHARE)
- [Explicit locking](https://www.postgresql.org/docs/15/explicit-locking.html)
- [Partial indexes](https://www.postgresql.org/docs/15/indexes-partial.html)
- [Routine vacuuming](https://www.postgresql.org/docs/15/routine-vacuuming.html)
