# Comparison with Celery, Taskiq, Procrastinate, and PgQueuer

This page is not a claim that PgRelay is universally better. It is a positioning note: when an existing tool is a
better fit, use it.

PgRelay is not the first PostgreSQL-backed queue, and it should not pretend to be. Procrastinate and PgQueuer are the
closest neighbors: both already use PostgreSQL as queue storage and both are serious projects worth evaluating first.

PgRelay is narrower. It is built around a transactional outbox shape: enqueue a durable job row inside the same
SQLAlchemy `AsyncSession` transaction as your domain write, then let a small worker execute HTTP jobs or Python handler
jobs after commit. The admin API and CLI are part of that operational surface.

<table>
  <thead>
    <tr>
      <th>Tool</th>
      <th>Use it when</th>
      <th>Use PgRelay when</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Celery</td>
      <td>
        You need the mature Python task queue ecosystem: RabbitMQ or Redis brokers, result backends, canvas workflows,
        periodic scheduling, rate limits, monitoring events, and lots of production history.
      </td>
      <td>
        You already have PostgreSQL, do not want to run a separate broker, and the job must commit or roll back with
        the database row that created it.
      </td>
    </tr>
    <tr>
      <td>Taskiq</td>
      <td>
        You want an asyncio-native task framework with pluggable brokers, typed task calls, middleware, result
        backends, and a worker model centered on Python task functions.
      </td>
      <td>
        You want the database transaction to be the enqueue boundary and prefer a small PostgreSQL-backed outbox over
        a broker-oriented task framework.
      </td>
    </tr>
    <tr>
      <td>Procrastinate</td>
      <td>
        You want a PostgreSQL-backed distributed task library with task decorators, sync and async APIs, retries,
        periodic tasks, locks, Django integration, and a broad task-processing model.
      </td>
      <td>
        You want transactional enqueue from a caller-owned SQLAlchemy <code>AsyncSession</code>, built-in HTTP job
        execution, and an admin API for jobs, queues, attempts, stats, and worker heartbeats.
      </td>
    </tr>
    <tr>
      <td>PgQueuer</td>
      <td>
        You want a minimalist high-performance PostgreSQL queue using <code>LISTEN/NOTIFY</code>,
        <code>FOR UPDATE SKIP LOCKED</code>, batch processing, and a queue-library style API.
      </td>
      <td>
        You want a transactional outbox plus operational API, leases, replay, cancellation, HTTP target safeguards,
        and direct integration with SQLAlchemy async sessions.
      </td>
    </tr>
  </tbody>
</table>

## Short version

Use Celery when the queue is core infrastructure and you are comfortable operating a broker. Use Taskiq when you want a
modern async task framework and its broker model fits your system.

Use Procrastinate or PgQueuer when you specifically want a PostgreSQL queue library. They are the closest comparisons
and may be the better choice if their task APIs, wakeup model, scheduling, or framework integrations match your service.

Use PgRelay when the important part is not "run a Python function later" but "write a domain row and the durable side
effect request in the same database transaction."

That is the core trade-off PgRelay optimizes for.

## References

- [Celery introduction](https://docs.celeryq.dev/en/stable/getting-started/introduction.html)
- [Taskiq architecture overview](https://taskiq-python.github.io/guide/architecture-overview.html)
- [Procrastinate documentation](https://procrastinate.readthedocs.io/en/stable/)
- [PgQueuer documentation](https://pgqueuer.readthedocs.io/en/latest/)
