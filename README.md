# PulseQueue

**A distributed background job queue with retries, priority scheduling, dead-letter handling and crash recovery — built from primitives, not from Celery.**

FastAPI · Redis · PostgreSQL · Docker

---

## The problem

Every backend eventually has work that must not happen on the request path. Sending a confirmation email, resizing an upload, generating a report — all of it is slow, occasionally fails, and none of it should be able to fail a checkout. The standard answer is a job queue: accept the work, return immediately, execute it elsewhere, and retry it when it breaks.

Most projects reach for Celery and call `task.delay()`. PulseQueue implements the mechanics underneath that call — the atomic dequeue, the backoff scheduler, the dead-letter path, the visibility timeout that reclaims work from a worker that died — so that the behaviour is something I can explain rather than something I configured.

It is a smaller, legible version of what Celery, Sidekiq, BullMQ and SQS+Lambda do.

---

## What it does

| | |
|---|---|
| **Submit** | `POST /jobs` accepts a job type and payload, persists it, returns immediately |
| **Execute** | Independent worker processes pull from a shared Redis queue and run the matching handler |
| **Prioritise** | Jobs carry a priority 1–5; high-priority work is picked up first, FIFO within a band |
| **Retry** | Failures back off exponentially (2s, 4s, 8s) without blocking a worker |
| **Give up safely** | Jobs that exhaust their attempts land in a dead-letter queue for inspection, never silently vanish |
| **Deduplicate** | An `idempotency_key` guarantees a job executes once, even under concurrent duplicate submission |
| **Recover** | A worker killed mid-job has its work reclaimed and re-run — verified with `SIGKILL`, not assumed |
| **Observe** | A WebSocket dashboard shows every state transition live |
| **Extend** | New job types register with a `@job_handler` decorator; worker code never changes |

---

## Quick start

```bash
git clone https://github.com/<your-username>/pulsequeue.git
cd pulsequeue
docker compose up --build
```

That is the whole setup. It starts Postgres, Redis, the API and three worker replicas.

- API — <http://localhost:8000>
- Interactive docs — <http://localhost:8000/docs>
- Live dashboard — <http://localhost:8000/dashboard/>

Submit a job and watch it run:

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_type": "send_email", "payload": {"to": "you@example.com"}}'

curl http://localhost:8000/jobs/<id>
```

> **Ports already in use?** Set `POSTGRES_PORT`, `REDIS_PORT` or `API_PORT` in a local `.env` to remap the host side. Container-to-container addressing is unaffected.

---

## Architecture

### Runtime view

```
                       ┌──────────────────────┐
                       │      Client(s)        │
                       └───────────┬───────────┘
                                   │ HTTP
                                   ▼
                       ┌──────────────────────┐
                       │    FastAPI service    │
                       │  POST/GET /jobs       │
                       │  WS   /jobs/stream    │
                       └───┬──────────────┬────┘
                   writes  │              │ subscribes
                   job row │              │ to updates
                           ▼              │
                  ┌─────────────────┐     │
                  │   PostgreSQL     │     │
                  │  source of truth │     │
                  └────────┬─────────┘     │
                  enqueue  │               │
                           ▼               │
                  ┌─────────────────┐      │
                  │      Redis       │──────┘
                  │  pq:queue   ZSET │
                  │  pq:delayed ZSET │
                  │  pq:dlq     LIST │
                  │  pq:updates chan │
                  └────────┬─────────┘
                  BZPOPMIN │ (atomic)
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
      ┌──────────┐   ┌──────────┐   ┌──────────┐
      │ Worker 1 │   │ Worker 2 │   │ Worker N │
      └────┬─────┘   └────┬─────┘   └────┬─────┘
           └──────────────┼──────────────┘
                          ▼
             success → mark done, publish
             failure → RetryService decides:
                       delayed re-queue, or DLQ
```

### Layered view — the part that actually matters

Every request and every job execution flows strictly downward. Nothing calls upward.

```
┌──────────────┐
│  API Router  │  app/api/routers/jobs.py
└──────┬───────┘  HTTP only. Zero SQL, zero Redis.
       │
┌──────▼───────┐
│   Services   │  JobService · QueueService · RetryService
└──────┬───────┘  MetricsService · RecoveryService
       │          Business logic. Decides *what* happens.
┌──────▼───────┐
│  Repository  │  JobRepository — the only file that writes SQL.
└──────┬───────┘  Decides *how* it is stored.
       │
┌──────▼───────┐
│  PostgreSQL  │
└──────────────┘
```

The payoff is concrete rather than aesthetic: **71 of the 97 tests need no database, no Redis and no worker.** Retry backoff, the dead-letter threshold, idempotency resolution, priority ordering and crash-recovery logic are all exercised against mocks. That is only possible because no service constructs its own dependencies.

```bash
pytest tests/            # 97 tests
pytest tests/ -m "not integration"   # 71 of them, with nothing running
```

### Plugin registry

Adding a job type touches exactly one file:

```python
# handlers/builtin.py
@job_handler("process_payment")
def process_payment(payload: dict) -> dict:
    ...
    return {"charged": True}
```

The worker never changes. It imports `handlers.builtin` once at startup, which fires the decorators, and thereafter looks handlers up by name. Same pattern as Celery's `@task`, Flask's `@route` and pytest fixtures.

---

## Job lifecycle

```
        submit
          │
          ▼
     ┌─────────┐
     │ QUEUED  │◄────────────────┐
     └────┬────┘                 │ backoff elapses
          │ worker BZPOPMIN      │ (pq:delayed → pq:queue)
          ▼                      │
     ┌─────────┐                 │
     │ RUNNING │──── raises ─────┴─┐
     └────┬────┘                   │
          │ returns                ▼
          ▼                  ┌──────────┐
     ┌─────────┐             │  FAILED  │ attempts < max
     │ SUCCESS │             └────┬─────┘
     └─────────┘                  │ attempts == max
                                  ▼
                          ┌──────────────┐
                          │ DEAD_LETTER  │ ──POST /jobs/{id}/retry──► QUEUED
                          └──────────────┘
```

`RetryService` is the sole owner of the FAILED branch. The worker calls `handle_failure(job, exc)` and makes no decision of its own.

---

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Submit a job |
| `GET` | `/jobs/{id}` | Status and result of one job |
| `GET` | `/jobs?status=&limit=` | List jobs, newest first |
| `GET` | `/jobs/stats` | Counts per status |
| `POST` | `/jobs/{id}/retry` | Re-queue a dead-lettered job |
| `WS` | `/jobs/stream` | Live status changes |
| `GET` | `/health` | Liveness |

<details>
<summary><b>Examples</b></summary>

```bash
# Submit with priority and a dedupe key
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
        "job_type": "generate_report",
        "payload": {"report_id": "q3-revenue"},
        "priority": 1,
        "idempotency_key": "report-q3-revenue-2026",
        "max_attempts": 5
      }'

# Anything that needs a human
curl "http://localhost:8000/jobs?status=dead_letter"

# Put it back in the queue with a fresh attempt budget
curl -X POST http://localhost:8000/jobs/<id>/retry
```
</details>

**Errors:** `404` unknown job · `409` illegal state transition (e.g. retrying a job that is not dead-lettered) · `422` validation failure.

---

## Reliability

### Atomic dequeue — the concurrency guarantee

Workers pull with `BZPOPMIN`. Redis executes commands one at a time, so of N workers blocked on the same key, exactly one receives any given job. There is no lock to get wrong and no window to race in.

Blocking rather than polling also means a worker wakes the moment a job arrives. Measured pickup latency on an idle queue is **~25ms**, against the ~500ms average a 1-second poll loop would give.

### Retry backoff without blocking a worker

The obvious implementation — `time.sleep(delay)` in the worker — idles an entire process for the duration of the backoff. With three workers and a 20% failure rate, a meaningful share of the fleet ends up asleep while jobs pile up behind them.

Instead, a failed job goes into `pq:delayed`, a sorted set scored by ready-at timestamp. Each worker promotes due jobs back into the ready queue at the top of its loop, via a Lua script so the read-and-move is atomic. The worker returns to work immediately.

Observed during load testing: five jobs backing off simultaneously while `success` climbed from 30 → 40 in the same window. Under the sleeping design, all three workers would have been idle.

### Crash recovery — visibility timeout

Atomic dequeue has a consequence: the moment a worker takes a job, it is gone from Redis. If that worker is then `SIGKILL`ed, the job exists nowhere but as a row stuck in `RUNNING`, and nothing would ever pick it up.

Graceful shutdown handles `SIGTERM`. It cannot handle a process that stops executing instructions.

`RecoveryService` closes this with a visibility timeout, the same primitive SQS exposes. A job in `RUNNING` for longer than any legitimate execution could take is presumed abandoned and re-queued. Every worker runs the sweep on a timer; a Redis `SET NX EX` lock means only one actually scans.

**Verified, not asserted:**

```
submitted a job, waited 700ms  → status=running, attempts=1
docker compose kill -s SIGKILL worker
                               → status=running, redis queue depth=0   (orphaned)
restarted workers
t+65s                          → status=success, attempts=2            (reclaimed and re-run)

worker-2 | WARNING Recovering job 2b23bf60… stuck in RUNNING since 21:41:23 — re-queueing
worker-2 | INFO    Recovery sweep reclaimed 1 job(s)
```

A second sweep covers the other orphan path: `submit()` commits the row and *then* enqueues, with no transaction spanning Postgres and Redis. A process death between those two statements leaves a `QUEUED` row that is in no queue. The sweep re-enqueues it — guarded by a `ZSCORE` membership check, because a merely backlogged queue looks identical from the database and must not be duplicated.

### Poison-pill protection

`attempts` is incremented *before* the handler runs, not after. A job that reliably kills its worker therefore still consumes its budget and dead-letters, instead of crash-looping the fleet forever.

### Idempotency under concurrency

`JobService.submit` checks the key first — the fast path — but a check-then-act sequence is racy by construction: two concurrent requests can both read "no existing job". Only the unique constraint can settle it, so the loser catches the violation and re-reads the winner's row.

```
10 concurrent POSTs, same idempotency_key
  → 10 × 4f8a91c3-…   (1 distinct id, 0 errors)
```

Without the constraint handling, nine of those ten are `500`s.

### At-least-once, deliberately

A job may execute more than once — a worker can finish its work and die before committing the result. Exactly-once delivery across two systems with no distributed transaction is a genuinely hard problem, and claiming it without solving it is worse than not claiming it. Handlers should be idempotent. See [DECISIONS.md](docs/DECISIONS.md).

---

## Benchmarks

Measured on the local Docker stack: 3 worker replicas, Postgres 15, Redis 7. **Re-run against the deployed instance before quoting these anywhere.**

### API submit throughput — Locust, 50 users, 120s

| Endpoint | Requests | Failures | RPS | P50 | P95 | P99 |
|---|---|---|---|---|---|---|
| `POST /jobs` | 9,742 | 0 | 81.4/s | 62ms | 100ms | 140ms |
| `GET /jobs/stats` | 3,124 | 0 | 26.1/s | 13ms | 38ms | 61ms |
| `GET /jobs/{id}` | 2,144 | 0 | 17.9/s | 16ms | 50ms | 84ms |
| **Aggregate** | **15,042** | **0 HTTP** | **125.7/s** | 55ms | 95ms | 130ms |

### End-to-end completion throughput

| Scenario | Throughput | Notes |
|---|---|---|
| 3 workers, no simulated I/O | **165 jobs/sec** sustained (peak window ~220/s) | 3,000 jobs drained in 18.1s, zero losses |
| 3 workers, 0.5s simulated I/O + 20% failure injection | ~4.5 jobs/sec | Ceiling is `3 ÷ 0.5s = 6/s`; the gap is retry overhead |

The second row is worth reading carefully. It measures `time.sleep(0.5)`, not PulseQueue — three workers running a half-second handler cannot exceed six jobs per second regardless of how fast the queue is. The first row uses a no-op handler so the number reflects what the system actually costs per job: one atomic dequeue, three row updates, one publish.

**The queue is doing its job in both cases.** During the Locust run the API accepted 81 jobs/sec against a fleet that could complete ~4.5/s, and absorbed a 9,000-job backlog with zero errors while draining steadily. That gap is precisely the reason the work is not on the request path — and the reason workers scale independently of the API.

### Reliability results

| Test | Result |
|---|---|
| 20 jobs / 3 workers | 20/20 terminal, no double-processing, no losses |
| Retry behaviour | 6/20 jobs needed a 2nd attempt, 2 needed a 3rd — all succeeded |
| `SIGKILL` mid-job | Reclaimed after the visibility timeout, re-run, succeeded |
| 10 concurrent duplicate submissions | 1 job created, 0 errors |
| Unregistered `job_type` | Dead-lettered after 1 attempt, no wasted backoff |
| 9,000-job backlog | Drained steadily, 0 errors |

Reproduce with `locustfile.py`:

```bash
locust -f locustfile.py --host=http://localhost:8000 \
       --users=50 --spawn-rate=5 --run-time=120s --headless --csv=load_test_results
```

---

## Design decisions

Full write-ups with problem, alternatives and tradeoffs in **[docs/DECISIONS.md](docs/DECISIONS.md)**:

- **Why a repository layer separate from services** — and why that is not over-engineering for one table
- **Why a decorator registry instead of a handler dict** — Open/Closed, and the production precedents
- **Why `RetryService` is not part of the worker** — the test suite is the argument
- **Why at-least-once instead of exactly-once** — the honest answer
- **Why a delayed sorted set instead of `time.sleep`** — with the throughput cost of getting it wrong
- **Why a visibility timeout is mandatory once dequeue is atomic**
- **Why `create_all()` instead of Alembic in v1**

Retrospective notes — what broke, what was learned — in **[docs/LEARNINGS.md](docs/LEARNINGS.md)**.

---

## Documentation

| Document | What it covers |
|---|---|
| **[docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md)** | Every file, every non-obvious line, and every traced end-to-end flow. Start here to understand how it works. |
| **[docs/RUNBOOK.md](docs/RUNBOOK.md)** | How to run it, inspect Postgres and Redis, read the logs, and deliberately trigger every behaviour including crash recovery. |
| **[docs/DECISIONS.md](docs/DECISIONS.md)** | 19 decisions, each with the alternatives considered and what the choice cost. |
| **[docs/LEARNINGS.md](docs/LEARNINGS.md)** | What broke during the build, and what it taught. |

---

## Project layout

```
app/
  main.py                     FastAPI wiring only
  api/
    dependencies.py           Depends() providers — the HTTP/infra seam
    routers/jobs.py           Thin routes. No SQL, no Redis.
  core/
    config.py                 Every env var, read once
    clock.py                  Timezone-aware UTC helpers
    database.py               Engine, session factory, Base
    redis.py                  Client singleton
    exceptions.py             Domain errors (not HTTP, not SQLAlchemy)
    logging.py                Structured logging
  models/job.py               ORM shape — no behaviour
  schemas/job.py              Pydantic API contract — decoupled from the ORM
  repositories/
    job_repository.py         The only file that writes SQL
  services/
    job_service.py            Submit, fetch, list, manual retry, idempotency
    queue_service.py          The only file that speaks Redis
    retry_service.py          Backoff and dead-letter policy
    recovery_service.py       Visibility timeout and orphan reclamation
    metrics_service.py        Stats aggregation
  registry/job_registry.py    @job_handler decorator + lookup
  workers/worker.py           Thin executor — zero policy decisions
  websocket/stream.py         Pub/Sub → WebSocket fan-out
handlers/builtin.py           Job implementations — the extension point
tests/                        97 tests, 71 needing no infrastructure
dashboard/index.html          Live status view
```

---

## Configuration

All settings are environment variables, read in `app/core/config.py` and nowhere else. Copy `.env.example` to `.env`.

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql://pulse:pulse@localhost:5432/pulsequeue` | Postgres DSN |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis DSN |
| `MAX_RETRY_ATTEMPTS` | `3` | Default attempt budget |
| `BASE_RETRY_DELAY` | `2` | Backoff base, seconds |
| `VISIBILITY_TIMEOUT` | `300` | Seconds in RUNNING before a job is presumed abandoned |
| `RECOVERY_INTERVAL` | `30` | Seconds between recovery sweeps |
| `DEQUEUE_TIMEOUT` | `1` | `BZPOPMIN` block duration |
| `WORKER_CONCURRENCY` | `3` | Worker replica count |
| `LOG_LEVEL` | `INFO` | |

`VISIBILITY_TIMEOUT` must comfortably exceed the slowest handler. Set it too low and healthy long-running jobs get reclaimed and duplicated — which is the at-least-once tradeoff, made tunable and explicit.

---

## Testing

```bash
pytest tests/ -v                      # everything (needs Postgres)
pytest tests/ -m "not integration"    # 71 unit tests, no infrastructure
```

Integration tests skip themselves when Postgres is unreachable, so a fresh clone stays green. CI sets `REQUIRE_INTEGRATION_TESTS=1`, which turns a skip into a failure so it cannot pass by accident.

One test reads `app/api/routers/jobs.py` and asserts it contains no SQL, no Redis calls and no repository import. The layering rule is build-blocking per the NFRs, and a human reviewer stops checking around week two.

---

## Future improvements

- **Exactly-once semantics** via a transactional outbox and handler-level dedupe keys — currently at-least-once by design.
- **Alembic migrations.** v1 uses `create_all()`, which is correct for one table and wrong the moment a second engineer needs to change a column.
- **Prometheus metrics** exported from `MetricsService`, which already owns every aggregate.
- **Scheduled and cron-style jobs.** The `pq:delayed` sorted set is already the right primitive — a `run_at` field would mostly reuse it.
- **Queue-depth autoscaling.** Worker count is fixed; the load test shows exactly where that binds.
- **Per-client rate limiting.**
- **A dedicated pub/sub connection pool.** Each dashboard client currently opens its own Redis connection — fine for an ops view, not for thousands of viewers.

---

## License

MIT — see [LICENSE](LICENSE).
