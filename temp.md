# PulseQueue → Resume Pack

**Prepared:** 2026-08-09
**Purpose:** Everything needed to put this project on an SDE placement resume — a proper project name, ready-to-paste bullets, a verified fact sheet of what the code actually does, an honest audit of which numbers are defensible, interview prep, and a checklist of what you still need to do.

**Source of truth:** every claim below was read out of the code at `Downloads/pulsequeue/pulsequeue`, not from the README's marketing copy. Where the README and the code disagree, this document follows the code and says so.

> This file lives *outside* the git repo on purpose, so it never gets pushed to your portfolio repository.

---

## Table of contents

1. [Project name](#1-project-name)
2. [Resume bullets — primary set](#2-resume-bullets--primary-set)
3. [Alternate lengths](#3-alternate-lengths)
4. [Verified fact sheet](#4-verified-fact-sheet)
5. [Numbers audit — what is safe to quote](#5-numbers-audit--what-is-safe-to-quote)
6. [Interview prep](#6-interview-prep)
7. [YOUR TODO LIST](#7-your-todo-list)
8. [Appendix — file-by-file inventory](#8-appendix--file-by-file-inventory)

---

## 1. Project name

`pulsequeue` is a product name, not a description. A recruiter scanning for six seconds gets nothing from it. The title should say what the system *is*.

### Recommended

> ### DistriQ — Distributed Job Orchestration Engine

**Why this one:** the codebase is not just a queue. It contains a retry engine (`RetryService`), a DAG scheduler (`DagService`), a crash-recovery subsystem (`RecoveryService`), and a metrics/observability stack. "Task queue" alone undersells the two things that are actually impressive — the DAG and the crash recovery. "Orchestration engine" covers all of it and is a term infra teams recognise.

You can keep `PulseQueue` as the repo/product name and use the descriptive subtitle on the resume — that's exactly what the header format below does.

### Alternatives

| Name | Use it if |
|---|---|
| **Fault-Tolerant Distributed Task Queue** | You want reliability engineering to be the headline. Your deepest work is here (visibility timeout, orphan sweep, idempotency race). |
| **Distributed Job Queue & DAG Scheduler** | You want the DAG feature visible in the title itself — it differentiates you from every other "I built a task queue" project. |
| **Resilient Async Job Processing Platform** | Broad backend/platform roles; slightly less specific, reads well to non-specialist recruiters. |
| **Distributed Task Queue with Crash Recovery** | Longest, but the most specific — good if you have room for one long title. |

### Resume header line

```
DistriQ — Distributed Job Orchestration Engine                          [GitHub] [Demo]
FastAPI · PostgreSQL · Redis · Docker · Prometheus/Grafana · Locust          Jun–Aug 2026
```

Adjust the date range to reality. Keep the tech list to the stack you can defend in an interview.

---

## 2. Resume bullets — primary set

Four bullets, STAR-compressed (Situation and Task are implicit in a resume; every bullet is Action + quantified Result). Paste as-is; bold is markdown, convert to your resume's emphasis style.

---

**• Architected a horizontally-scalable job-processing engine** from queue primitives (no Celery), using a **Redis sorted-set priority queue** with `BZPOPMIN` atomic dequeue and a `priority × 10¹³ + timestamp` score encoding to guarantee **strict priority ordering with FIFO tie-breaking**; sustained **165 jobs/s end-to-end** across 3 worker replicas (**3,000 jobs drained in 18.1 s, zero loss**) and **81 req/s** submit throughput at **P95 100 ms** over 15,000 requests with **0 failures**.

**• Eliminated every silent job-loss path** by building the fault-tolerance layer by hand: **non-blocking exponential backoff** (2/4/8 s) via a delayed sorted set promoted by an **atomic Lua script** — freeing workers that a `sleep()`-based design would idle — plus a dead-letter queue with manual replay and an **SQS-style visibility timeout** guarded by a `SET NX EX` distributed lock; validated by `SIGKILL`-ing a worker mid-job (**reclaimed and re-run within 65 s**) and by **10 concurrent duplicate POSTs collapsing to 1 job with 0 errors**, down from 9 HTTP 500s before database-adjudicated idempotency.

**• Extended the queue into a DAG scheduler** where jobs declare `depends_on` (PostgreSQL `UUID[]`, fan-out queried via `= ANY()`), remain `PENDING` until every upstream job succeeds, and are **rejected at submit time by an O(V+E) iterative-DFS cycle check** with cascade dead-lettering of unreachable dependents; added **hard per-job wall-clock timeouts** enforced through `ProcessPoolExecutor` (the only preemptible primitive in CPython) and a **decorator-based plugin registry** that onboards new job types with **zero changes to worker code**.

**• Made the system observable and provably correct:** exported **Prometheus multiprocess metrics** across 4 processes (claim-latency and execution-duration histograms, live queue-depth gauges) into an **8-panel Grafana dashboard** with p50/p95/p99 latency and per-type failure rates, plus a **WebSocket live view over Redis pub/sub**; enforced strict router→service→repository layering — including **a test that fails the build if a router touches SQL or Redis** — keeping **71 of 97 tests infrastructure-free**, all gated by CI that boots the full 6-service stack and runs a job end-to-end on every push.

---

### Why each bullet is built the way it is

| Bullet | The signal it sends |
|---|---|
| 1 | Systems design + concrete throughput numbers. The score-encoding detail proves you designed the ordering, not copy-pasted it. |
| 2 | Distributed-systems reasoning and *verification*. "SIGKILL-verified" and "10 → 1, 0 errors" are experiments, not claims. This is the strongest bullet. |
| 3 | Algorithms (DFS cycle detection) + language-level depth (`ProcessPoolExecutor` vs threads) + design principles (Open/Closed). Covers the CS-fundamentals box. |
| 4 | Production maturity: observability, testing discipline, CI. Most student projects have none of this. |

---

## 3. Alternate lengths

### Three-bullet version (space-constrained)

**•** Built a **distributed job queue** (FastAPI · Redis · PostgreSQL · Docker) with atomic `BZPOPMIN` dequeue and priority-band + FIFO scoring — **165 jobs/s end-to-end** on 3 workers, **81 req/s** ingest at **P95 100 ms**, 15K requests, **0 failures**.

**•** Engineered reliability from primitives — **Lua-atomic delayed-set backoff**, dead-letter queue with replay, and an **SQS-style visibility timeout** reclaiming jobs from `SIGKILL`-ed workers; **10 concurrent duplicate submits → 1 job, 0 errors** via database-adjudicated idempotency.

**•** Added a **DAG scheduler** (O(V+E) cycle rejection, cascade dead-lettering), per-job timeouts via `ProcessPoolExecutor`, and **Prometheus/Grafana** observability; layered architecture keeps **71/97 tests infrastructure-free**, verified by full-stack CI on every push.

### Two-bullet version (if the resume is packed)

**•** Built a **distributed job orchestration engine** (FastAPI · Redis · PostgreSQL · Docker) — atomic `BZPOPMIN` priority dequeue, Lua-atomic exponential backoff, dead-letter queue, DAG dependency scheduling with O(V+E) cycle rejection, and per-job `ProcessPoolExecutor` timeouts; **165 jobs/s end-to-end, 81 req/s ingest at P95 100 ms, 0 failures over 15K requests**.

**•** Guaranteed **no silent job loss** with an SQS-style visibility timeout (`SIGKILL`-verified reclaim) and DB-adjudicated idempotency (**10 concurrent duplicates → 1 job, 0 errors**); instrumented with **Prometheus/Grafana** (8 panels, p50/p95/p99) and a layered design keeping **71/97 tests infra-free** under full-stack CI.

### One-line version (for a "Projects" list on a 1-page resume)

**•** **DistriQ** — Distributed job orchestration engine in FastAPI/Redis/PostgreSQL: atomic priority dequeue, Lua-atomic retry backoff, DAG scheduling, SQS-style crash recovery, Prometheus/Grafana observability. **165 jobs/s, 0 job loss under worker SIGKILL, 97 tests.**

---

## 4. Verified fact sheet

Everything in this section was confirmed by reading the source.

### 4.1 Scale of the codebase

| Metric | Value |
|---|---|
| Application code (`app/` + `handlers/`) | **2,985 lines** |
| Test code (`tests/`) | **1,201 lines** |
| Documentation (`docs/` + README) | **~7,700 lines** across 12 documents |
| Python modules | 24 (excluding `__init__.py`) |
| Test functions | 90 defined, **~97 collected** (two `@pytest.mark.parametrize` blocks expand) |
| Docker services | 6 defined (postgres, redis, api, worker, prometheus, grafana) → **8 running containers** (worker has `replicas: 3`) |

### 4.2 Architecture — strict downward layering

```
API Router  (app/api/routers/jobs.py)     HTTP only. Zero SQL, zero Redis.
     │
Services    (6 classes)                   Business logic — decides WHAT happens
     │      JobService · QueueService · RetryService
     │      RecoveryService · DagService · MetricsService
     │
Repository  (app/repositories/)           The only file that issues SQL
     │
PostgreSQL
```

Enforcement is mechanical, not cultural: `tests/test_api.py::test_routers_contain_no_sql_or_redis_calls` reads `app/api/routers/jobs.py` as text and asserts it contains no SQL, no Redis calls and no repository import. Dependency injection is done in `app/api/dependencies.py`; no service constructs its own collaborators, which is *why* 71 of 97 tests run against `MagicMock` with nothing booted.

### 4.3 Data model — `jobs` table

| Column | Type | Notes |
|---|---|---|
| `id` | `UUID` PK | |
| `idempotency_key` | `VARCHAR(255)` **UNIQUE**, nullable, indexed | The DB is the real enforcement point |
| `job_type` | `VARCHAR(100)` | Registry lookup key |
| `payload` | `JSONB` | Binary storage, indexable later |
| `priority` | `INT` 1–5 | 1 = highest |
| `status` | `ENUM job_status` indexed | `values_callable` forces lowercase values in PG |
| `attempts` / `max_attempts` | `INT` | Incremented *before* execution (poison-pill guard) |
| `timeout_seconds` | `INT` nullable | Per-job wall-clock budget |
| `depends_on` | `ARRAY(UUID)` `server_default '{}'` | DAG edges, queried with `= ANY()` |
| `created_at` / `updated_at` | `TIMESTAMPTZ` | |
| `result` | `JSONB` nullable | Handler output, or `{error, reason, retry_in}` |

Composite indexes: `ix_jobs_status_updated_at` (serves the recovery sweep) and `ix_jobs_status_created_at` (serves `GET /jobs?status=`).

**State machine (6 states):**

```
PENDING ──all deps SUCCESS──▶ QUEUED ──worker claims──▶ RUNNING ──▶ SUCCESS
                                 ▲                          │
                                 │                          ▼
                        backoff elapses                  FAILED ──attempts exhausted──▶ DEAD_LETTER
                                 └──────────────────────────┘                                │
                                                                    POST /jobs/{id}/retry ───┘
```

### 4.4 Redis key layout

| Key | Type | Purpose |
|---|---|---|
| `pq:queue` | ZSET | Ready jobs. Score = `priority × 10¹³ + epoch_ms` |
| `pq:delayed` | ZSET | Jobs in retry backoff. Score = ready-at ms; member = `"<priority>:<job_id>"` |
| `pq:dlq` | LIST | Permanently failed job ids |
| `pq:updates` | Pub/Sub channel | Feeds the WebSocket dashboard |
| `pq:lock:*` | STRING | `SET NX EX` mutexes (recovery sweep) |

**Why `10¹³`:** an epoch-millisecond timestamp is ~1.7 × 10¹², comfortably below 10¹³, so a priority-4 job can never sort ahead of a priority-3 job regardless of age. The maximum score (~5.2 × 10¹³) stays well inside float64's exact-integer range (~9 × 10¹⁵), so no two distinct scores collide through rounding. **This is a great whiteboard answer — know it cold.**

### 4.5 The two Lua scripts

1. **`_PROMOTE_DUE_LUA`** — moves every due job from `pq:delayed` into `pq:queue`, decoding the `"<priority>:<job_id>"` member so priority survives the round trip. Must be atomic: separate `ZRANGEBYSCORE` + `ZADD` + `ZREM` calls would let two workers observe the same due job and enqueue it twice.
2. **`_RELEASE_LOCK_LUA`** — compare-and-delete, so a worker cannot release a lock that expired and was re-acquired by someone else.

Both registered via `register_script()` (EVALSHA with automatic EVAL fallback).

### 4.6 Reliability mechanisms — the full list

| Mechanism | Implementation | Failure it prevents |
|---|---|---|
| **Atomic dequeue** | `BZPOPMIN` — Redis is single-threaded, exactly one of N blocked workers gets any job | Double-processing |
| **Blocking, not polling** | Worker wakes the instant a job lands (`dequeue_timeout=1s` only bounds shutdown responsiveness) | ~500 ms average pickup latency of a 1 s poll loop |
| **Non-blocking backoff** | Failed job → `pq:delayed` ZSET; worker returns to work immediately | An entire worker process idling through a 2/4/8 s sleep |
| **Dead-letter queue** | `pq:dlq` LIST + `POST /jobs/{id}/retry` with a fresh attempt budget | Jobs vanishing silently after exhausting retries |
| **Visibility timeout** | `RecoveryService.requeue_stuck_jobs` — `RUNNING` older than cutoff is presumed abandoned | A `SIGKILL`-ed worker wedging a job forever (it is already gone from Redis) |
| **Orphan sweep** | `requeue_orphaned_jobs` — row committed but enqueue never landed | The non-transactional gap between the Postgres commit and the Redis `ZADD` |
| **Membership check** | `ZSCORE` on both ZSETs before re-enqueueing | Duplicating every job in a queue that is merely backlogged (a backlog and an orphan look identical from the DB) |
| **Distributed lock** | `SET NX EX` with worker-id token + Lua compare-and-delete release | All 3 workers scanning the same rows simultaneously |
| **Poison-pill guard** | `attempts` incremented *before* the handler runs | A job that reliably crashes its worker crash-looping the fleet forever |
| **Idempotency** | Service-layer pre-check (fast path) + `IntegrityError` catch → re-read the winner's row | 9 of 10 concurrent duplicate submits returning HTTP 500 |
| **Permanent-failure fast path** | Unregistered `job_type` → `handle_failure(permanent=True)`, dead-letters on attempt 1 | Burning three backoffs on a typo'd job type |
| **Graceful shutdown** | `SIGTERM`/`SIGINT` set a flag; loop finishes the in-flight job then exits. `exec`-form `CMD` so signals actually reach Python | Every deploy abandoning an in-flight job |

**The key insight this project earns you** (and the best thing in it): *atomic dequeue is what makes multi-worker safety true, and it is also what makes crash recovery necessary.* A correct mechanism created a new failure mode. Say exactly that in an interview.

### 4.7 DAG scheduling

- Jobs with `depends_on` are born **`PENDING`** and never touch Redis.
- After **any** job reaches `SUCCESS`, the worker calls `DagService.resolve_dependents(job_id)`, which queries `WHERE status='pending' AND :job_id = ANY(depends_on)` — a native Postgres array query, no join table.
- A candidate transitions `PENDING → QUEUED` only when **all** its dependencies are `SUCCESS`.
- If any dependency reaches `DEAD_LETTER` or does not exist, the dependent is **immediately dead-lettered** with `reason: "dependency_failure"` rather than left stuck forever.
- **Cycle detection** runs at submit time: iterative DFS over the dependency subgraph, `O(V+E)`, returning 422 with `DagCycleError`. Because cycles are impossible at runtime, the resolution loop always terminates.
- Documented tradeoff: `ARRAY(UUID)` makes fan-out ("who depends on X?") crisp, but "what are X's deps?" is O(jobs) — a join table would be needed at millions of rows.

### 4.8 Per-job timeout

Handlers with `timeout_seconds` set run in a `ProcessPoolExecutor` subprocess; `future.result(timeout=N)` gives a hard wall-clock limit. **This is the only mechanism in CPython that can actually preempt a handler** — a thread cannot be killed from outside, and `asyncio.wait_for` only works for cooperative coroutines. On timeout the subprocess is abandoned, `JobTimeoutError` is raised into the normal failure path, and metrics record `outcome="timeout"` separately from business failures. Jobs without a timeout run in-process (no pickle or fork overhead).

### 4.9 Plugin registry

`@job_handler("name")` registers at import time into a module-level dict. Duplicate registration raises `ValueError` — a silently-overwritten handler would disable a job type in production with the symptom appearing nowhere near the cause. The worker imports `handlers.builtin` once and looks up by name; it never knows what is in there. **Precedents to name in an interview:** Celery `@task`, Flask `@route`, pytest fixtures.

Four built-in handlers: `send_email` (with a **20% injected transient failure rate** to exercise retries), `resize_image` (real Pillow work), `generate_report`, `benchmark_noop` (the no-op used for honest throughput measurement).

### 4.10 Observability

**Prometheus instruments** (`app/core/metrics.py`):

| Instrument | Type | Labels |
|---|---|---|
| `pulsequeue_jobs_total` | Counter | `job_type`, `outcome` ∈ {success, failed, dead_letter, timeout} |
| `pulsequeue_job_duration_seconds` | Histogram (13 buckets, 5 ms → 60 s) | `job_type`, `status` |
| `pulsequeue_claim_latency_seconds` | Histogram | `job_type` — submit → first worker claim |
| `pulsequeue_queue_depth` / `delayed_depth` / `dead_letter_depth` | Gauges | live Redis depths |

**Multiprocess mode** is the non-obvious part: the API and the 3 workers are separate OS processes, so in-memory counters in a worker are invisible to the API's `/metrics`. Solved with `prometheus_client`'s multiprocess mode — every process writes mmap files to `PROMETHEUS_MULTIPROC_DIR`, shared across containers by a Docker volume, and `MultiProcessCollector` unions them at scrape time. A dedicated `entrypoint.sh` creates that directory with the right ownership before dropping to the non-root `pulse` user.

**Grafana** (auto-provisioned datasource + dashboard): **8 panels** across 3 rows — Queue Health (3 stat panels + depths-over-time), Throughput & Outcomes (jobs/sec by outcome, failure rate by job type), Latency (claim latency p50/p95/p99, handler execution p50/p95). Prometheus scrapes every 5 s.

**WebSocket** `/jobs/stream` — async Redis client subscribes to `pq:updates` and forwards each status change to the browser dashboard. Deliberately fire-and-forget: pub/sub has no delivery guarantee, and a dropped dashboard update must never affect job execution.

### 4.11 Testing & CI

- **97 tests** collected, **71 need no infrastructure** — they run services against `MagicMock` repositories and Redis clients. This is the concrete payoff of dependency injection.
- Integration tests (`tests/test_api.py`, module-level `pytestmark = pytest.mark.integration`) **skip themselves** when Postgres is unreachable, so a fresh clone stays green. CI sets `REQUIRE_INTEGRATION_TESTS=1`, which converts a skip into a **failure** so the suite can't pass by accident.
- **Architecture fitness test:** `test_routers_contain_no_sql_or_redis_calls` reads the router source and fails the build on a layering violation. (A human reviewer stops checking around week two; a test doesn't.)
- **GitHub Actions, two jobs:**
  1. `test` — Postgres 15 + Redis 7 service containers, Python 3.11, full `pytest -v`.
  2. `build` — `docker compose build`, `up -d --wait`, then **submits a real job over HTTP and polls until it reaches `success`**, dumping logs on failure and tearing down with `down -v`. This proves the README's Quick Start actually works on every push.

### 4.12 Configuration surface

All env-driven via `pydantic-settings`, read in exactly one file (`app/core/config.py`); no other module imports `os` or `dotenv`.

`DATABASE_URL` · `REDIS_URL` · `WORKER_CONCURRENCY=3` · `DEQUEUE_TIMEOUT=1` · `MAX_RETRY_ATTEMPTS=3` · `BASE_RETRY_DELAY=2` · `VISIBILITY_TIMEOUT=300` (compose overrides to 60) · `RECOVERY_INTERVAL=30` (compose: 15) · `SMTP_HOST`/`SMTP_PORT` · `LOG_LEVEL`

### 4.13 API surface

| Method | Path | Notes |
|---|---|---|
| `POST` | `/jobs` | 201. Accepts `job_type`, `payload`, `priority` 1–5, `idempotency_key`, `max_attempts` 1–10, `timeout_seconds` 1–3600, `depends_on[]`. 422 on cycle or missing dep. |
| `GET` | `/jobs/stats` | Counts per status. Declared *before* `/{job_id}` so the literal path wins. |
| `GET` | `/jobs?status=&limit=` | Newest first, limit 1–500 |
| `GET` | `/jobs/{id}` | 404 if unknown |
| `POST` | `/jobs/{id}/retry` | 409 unless the job is `dead_letter` |
| `GET` | `/metrics` | Prometheus text format, multiprocess-aggregated |
| `GET` | `/health` | Liveness (used by compose healthcheck + CI) |
| `WS` | `/jobs/stream` | Live status changes |
| — | `/dashboard/` | Static live dashboard |

Six domain exceptions (`DuplicateIdempotencyKey`, `JobNotFound`, `InvalidStateTransition`, `JobTimeoutError`, `DagCycleError`, `UnresolvableDependency`) keep SQLAlchemy out of the router and HTTP out of the services — the same `JobService` is called by the worker, which speaks no HTTP.

---

## 5. Numbers audit — what is safe to quote

**This section matters. Do not skip it.** An interviewer who asks "how did you measure that?" and gets a vague answer erases the benefit of having the number at all.

### Tier A — verifiable from code, zero risk

| Claim | Where it's proven |
|---|---|
| 97 tests, 71 infrastructure-free | `pytest tests/` and `pytest tests/ -m "not integration"` |
| ~3,000 lines of application code | `find app handlers -name "*.py" \| xargs wc -l` |
| 6 services, 6 job states, 5 priority levels | Source |
| 8 Grafana panels, 6 Prometheus instruments | `docker/grafana/provisioning/dashboards/pulsequeue.json`, `app/core/metrics.py` |
| 3 worker replicas, 6 compose services | `docker-compose.yml` |
| O(V+E) DFS cycle detection | `DagService.validate_no_cycle` |
| 20% injected failure rate in `send_email` | `handlers/builtin.py` |

### Tier B — documented experiments, currently unreproducible from committed artifacts

⚠️ **The load-test CSVs committed in the repo are from a ~15-second run, not the 120-second run the README describes.**

| Metric | README claims | Committed CSV actually shows |
|---|---|---|
| `POST /jobs` requests | 9,742 | **825** |
| Aggregate requests | 15,042 | **1,217** |
| Aggregate RPS | 125.7/s | **86.5/s** |
| `POST /jobs` RPS | 81.4/s | **58.7/s** |
| `POST /jobs` P95 / P99 | 100 ms / 140 ms | **100 ms / 130 ms** ✅ matches |
| Failures | 0 | **0** ✅ matches |
| Run duration | 120 s | ~15 s (12 rows in the history CSV) |

The latency and zero-failure figures hold up. The throughput and volume figures come from a longer run whose CSV was never committed. Nothing is fabricated — but right now you cannot show an interviewer the evidence.

**The 165 jobs/s end-to-end figure, the 3,000-jobs-in-18.1s figure, the 9,000-job backlog and the SIGKILL recovery timeline have no committed artifact at all.** They are recorded in the README and `docs/LEARNINGS.md` as observations from manual runs.

**Fix:** re-run the benchmarks once (see the TODO list), commit the CSVs and a short `docs/BENCHMARKS.md` with the raw output, then quote whatever the new numbers say. If they come out lower, use the lower ones — a defensible 90 jobs/s beats an indefensible 165.

### Tier C — honest framing you should keep

The README already does something genuinely impressive that you should preserve and repeat in interviews: it distinguishes **submit throughput (81/s)** from **completion throughput (~4.5/s with a 0.5 s simulated-I/O handler)** and explains that the second number measures `time.sleep`, not the queue — three workers running a half-second handler cannot exceed 6 jobs/s no matter how fast the queue is. Adding a no-op handler moved the measured figure to 165 jobs/s.

That reasoning — *"before quoting a number, work out what would have to change for it to move"* — is more impressive than the number itself. Lead with it when asked about performance.

---

## 6. Interview prep

### The six questions this project is built to earn

**1. "Your dequeue is atomic. What does that break?"**
The best question in the project. `BZPOPMIN` removes the job from Redis the instant a worker takes it — that's what guarantees no double-processing. But if that worker is then `SIGKILL`-ed, the job exists nowhere except a row stuck in `RUNNING`, and nothing will ever pick it up. Graceful `SIGTERM` handling is irrelevant; a killed process runs no handler. The fix is a visibility timeout — the same primitive SQS exposes. **Atomicity is what makes multi-worker safety true and what makes crash recovery necessary; those are the same property viewed from two sides.**

**2. "How do you retry without blocking a worker?"**
`time.sleep(delay)` in the worker idles an entire process for the backoff duration. With 3 workers and a 20% failure rate a meaningful share of the fleet ends up asleep while jobs pile up. Instead a failed job goes into `pq:delayed`, a ZSET scored by ready-at time; each worker promotes due jobs back at the top of its loop via a Lua script (atomic read-and-move, so no job is promoted twice) and returns to work immediately.

**3. "Why a repository layer for one table? Isn't that over-engineering?"**
Not because the abstraction pays for itself at one table — because it is why **71 of 97 tests need no database, no Redis and no worker**. That's a number, not an opinion. If `QueueService` did `self._r = get_redis_client()` instead of taking the client by injection, every one of its tests would need a live Redis.

**4. "How do you handle duplicate submissions?"**
Two layers, deliberately. A service-layer pre-check on `idempotency_key` is the fast path for a client retrying seconds later. But check-then-act is racy by construction — two concurrent requests can both read "no existing job" — so the unique constraint is the real adjudicator, and the loser catches `IntegrityError` and re-reads the winner's row. Tested with 10 concurrent POSTs: 1 job, 0 errors. Before the constraint handling, 9 of those 10 were 500s. (I reasoned in the abstract that the pre-check was enough. It wasn't. Running it settled the argument in 30 seconds.)

**5. "Exactly-once delivery?"**
No — **at-least-once, deliberately**. A worker can finish its work and die before committing the result. Exactly-once across two systems with no distributed transaction is a genuinely hard problem, and claiming it without solving it is worse than not claiming it. Handlers should be idempotent. The path to exactly-once would be a transactional outbox plus handler-level dedupe keys.

**6. "What's your throughput?"**
Two numbers, and the gap between them is the point. Submit: 81 req/s at P95 100 ms. End-to-end completion with a no-op handler: 165 jobs/s across 3 workers. With a 0.5 s simulated-I/O handler it drops to ~4.5/s — but that measures `time.sleep`, not the queue; the ceiling is 3 ÷ 0.5 = 6/s by arithmetic. During the load test the API accepted 81 jobs/s against a fleet completing 4.5/s and absorbed a 9,000-job backlog with zero errors while draining steadily. **That 20× gap is precisely why the work isn't on the request path, and why workers scale independently of the API.**

### Other things worth having ready

- **Why the `10¹³` priority band** (see §4.4) — a crisp, specific answer that shows you designed the ordering.
- **Why `ProcessPoolExecutor` for timeouts** — the only preemptible primitive in CPython.
- **Two enum/routing bugs you fixed** (from `docs/LEARNINGS.md`): SQLAlchemy `Enum` storing member *names* so `psql` showed `QUEUED` while the API showed `queued`; and `/jobs/stats` needing to be declared before `/jobs/{job_id}` because FastAPI matches in declaration order.
- **Why `exec`-form `CMD` in the Dockerfile** — shell form runs the process under `/bin/sh`, which doesn't forward `SIGTERM`, so graceful shutdown would silently never fire and every deploy would abandon an in-flight job. A bug that produces no error message anywhere.
- **Known limitations** (have these ready — volunteering them reads as maturity): at-least-once not exactly-once; `create_all()` instead of Alembic; each dashboard client opens its own Redis connection; fixed worker count with no queue-depth autoscaling; no per-client rate limiting; a job stays `FAILED` while in the delayed set, so the `queued` stat undercounts pending work.

---

## 7. YOUR TODO LIST

Ordered by impact per unit of effort. Items 1–4 are the ones that actually matter before you submit the resume.

### 🔴 Priority 1 — do these before the resume goes out

- [ ] **Re-run the load test and commit the artifacts.** This is the single most important item — it converts your headline numbers from "claimed" to "evidenced."
  ```bash
  cd pulsequeue
  docker compose up -d --wait
  locust -f locustfile.py --host=http://localhost:8000 \
         --users=50 --spawn-rate=5 --run-time=120s --headless --csv=load_test_results
  ```
  Then update the README's benchmark table with whatever the new CSVs say, and **change the resume bullets to match**. If the numbers come out lower, use the lower ones.

- [ ] **Re-run the end-to-end throughput measurement** with the `benchmark_noop` handler and record the exact procedure. The 165 jobs/s and "3,000 jobs in 18.1 s" figures currently have no committed artifact. Write down the commands used so it's reproducible.

- [ ] **Fix the README's stale "Future improvements" section.** It still lists **Prometheus metrics** and **scheduled jobs** as future work — Prometheus is fully implemented (multiprocess mode, Grafana dashboard, 6 instruments), and the DAG scheduler isn't mentioned in the feature table at all. A recruiter who reads "future: Prometheus metrics" right after your resume says "Prometheus/Grafana observability" will assume the resume is inflated.

- [ ] **Fix the placeholder clone URL** in README line 38: `https://github.com/<your-username>/pulsequeue.git`. Also update the README title/description if you adopt the new project name.

- [ ] **Push to a public GitHub repo** with a clean commit history, and confirm the CI badge is green. The `build` job proves the Quick Start works — that green check is worth a lot on a portfolio repo.

### 🟠 Priority 2 — high value, low effort

- [ ] **Fix the broken local venv.** `pulsequeue/venv/` points at `C:\Users\Hitansh\AppData\Local\Programs\Python\Python311\python.exe`, which no longer exists, so you cannot run the test suite locally. Recreate it:
  ```bash
  cd pulsequeue
  rm -rf venv && python -m venv venv
  ./venv/Scripts/python.exe -m pip install -r requirements.txt
  ./venv/Scripts/python.exe -m pytest tests/ -v
  ```
  **Confirm the "97 tests / 71 infra-free" numbers yourself** before quoting them — this document trusts the README on that split.

- [ ] **Add `venv/`, `.pytest_cache/`, `__pycache__/` and `.env` to `.gitignore`** and verify none of them are tracked. A committed `venv/` in a portfolio repo is an instant credibility hit — and `.env` may contain secrets.
  ```bash
  git ls-files | grep -E "venv/|__pycache__|\.pytest_cache|^\.env$"
  ```

- [ ] **Add a `docs/BENCHMARKS.md`** with the raw Locust output, the exact commands, the machine spec (CPU/RAM), and the caveat that the stack shares a CPU with the workers. One page. It is the document you open during an interview when someone asks "how did you measure that?"

- [ ] **Take 3–4 screenshots** — the Grafana dashboard under load, the live WebSocket dashboard, the Locust results page, and the terminal showing the SIGKILL recovery log lines. Put them in `docs/images/` and embed them in the README. Recruiters skim; a Grafana screenshot does more than three paragraphs.

### 🟡 Priority 3 — makes the project stronger if you have time

- [ ] **Deploy it somewhere public** (Railway, Render, Fly.io — all handle Postgres + Redis + multi-container). A live `/dashboard/` link on a resume gets clicked. Re-run the benchmarks against the deployed instance afterwards; `docs/LEARNINGS.md` already flags that local numbers share a CPU with the workers.

- [ ] **Kill a worker *during* a sustained load run**, not in isolation. Crash recovery is currently verified standalone; verifying it under load is a materially stronger claim and `docs/LEARNINGS.md` lists it as still owed.

- [ ] **Record a 60–90 second demo GIF/video**: submit a job → watch it in the dashboard → kill a worker → watch it get reclaimed. Embed at the top of the README.

- [ ] **Add Alembic migrations.** `create_all()` is correct for one table and wrong the moment a second engineer changes a column. It's also a common interview follow-up ("how do you handle schema changes?").

- [ ] **Add an architecture diagram as an image** (excalidraw/mermaid). The ASCII diagrams are excellent in a terminal and weak in a browser.

### 🟢 Priority 4 — genuinely optional

- [ ] Queue-depth autoscaling (the load test shows exactly where the fixed worker count binds).
- [ ] Transactional outbox for exactly-once semantics.
- [ ] Scheduled/cron jobs — `pq:delayed` is already the right primitive; a `run_at` field mostly reuses it.
- [ ] A shared pub/sub connection pool for the dashboard (currently one Redis connection per viewer).
- [ ] Per-client rate limiting.

### ✅ Decisions for you to make

- [ ] **Pick the project name** — recommendation is `DistriQ — Distributed Job Orchestration Engine`; alternatives in §1. Whatever you choose, use the same name on the resume, the GitHub repo, and the README title.
- [ ] **Pick the bullet count** — 4 bullets (§2) if the project is your resume centrepiece, 3 (§3) if it shares space with internships and other projects.
- [ ] **Set the date range** on the resume header line.
- [ ] **Decide whether to claim the DAG scheduler in the title.** It's a genuine differentiator — most "I built a task queue" projects stop at retries.

---

## 8. Appendix — file-by-file inventory

```
pulsequeue/
├── app/
│   ├── main.py                    FastAPI wiring only — lifespan, routers, dashboard mount, WS
│   ├── api/
│   │   ├── dependencies.py        Depends() providers — the HTTP/infra seam tests override
│   │   └── routers/jobs.py        Thin routes + /metrics. Zero SQL, zero Redis (test-enforced)
│   ├── core/
│   │   ├── config.py              pydantic-settings; the only module that reads the environment
│   │   ├── clock.py               Timezone-aware UTC helpers (utcnow, epoch_ms)
│   │   ├── database.py            Engine, SessionLocal, Base, init_db
│   │   ├── redis.py               Client singleton
│   │   ├── exceptions.py          6 domain errors — not HTTP, not SQLAlchemy
│   │   ├── logging.py             Structured logging config
│   │   └── metrics.py             6 Prometheus instruments + multiprocess registry
│   ├── models/job.py              ORM shape only — no behaviour. 6-state enum, 2 composite indexes
│   ├── schemas/job.py             Pydantic wire contract, decoupled from the ORM
│   ├── repositories/
│   │   └── job_repository.py      The ONLY file that issues SQL. 13 methods incl. DAG queries
│   ├── services/
│   │   ├── job_service.py         Submit, fetch, list, manual retry, idempotency, dep validation
│   │   ├── queue_service.py       The ONLY file that speaks Redis. 2 Lua scripts, 5 key types
│   │   ├── retry_service.py       Backoff maths + dead-letter decision (pure, unit-testable)
│   │   ├── recovery_service.py    Visibility timeout + orphan reclamation
│   │   ├── dag_service.py         PENDING→QUEUED resolution + O(V+E) cycle detection
│   │   └── metrics_service.py     Read-only status aggregation (one GROUP BY, not 6 COUNTs)
│   ├── registry/job_registry.py   @job_handler decorator + lookup; duplicate registration raises
│   ├── workers/worker.py          Thin executor — ZERO policy decisions. ProcessPoolExecutor timeouts
│   └── websocket/stream.py        Redis pub/sub → WebSocket fan-out (async client)
├── handlers/builtin.py            4 handlers: send_email (20% fail), resize_image, generate_report, benchmark_noop
├── tests/                         97 tests; 71 infra-free; includes the layering fitness test
├── dashboard/index.html           Live WebSocket status view
├── docker/
│   ├── entrypoint.sh              Creates PROMETHEUS_MULTIPROC_DIR before dropping to non-root
│   ├── prometheus.yml             5s scrape config
│   └── grafana/provisioning/      Auto-provisioned datasource + 8-panel dashboard
├── docker-compose.yml             6 services, healthcheck-gated startup ordering, overridable ports
├── Dockerfile.api / .worker       python:3.11-slim, non-root uid 1000, exec-form CMD
├── .github/workflows/ci.yml       2 jobs: unit+integration tests, and a full-stack e2e build
├── locustfile.py                  2 user classes: JobSubmitter (weight 4), JobLifecycleUser (weight 1)
└── docs/                          12 documents, ~7,700 lines
    ├── DECISIONS.md               19 design decisions with alternatives + costs  ← best interview prep
    ├── LEARNINGS.md               Per-milestone retrospective: what broke and why ← read before interviews
    ├── IMPLEMENTATION.md          Every file, every non-obvious line, traced end-to-end flows
    ├── RUNBOOK.md                 How to run, inspect, and deliberately trigger every behaviour
    ├── INTERVIEW_GUIDE.md         Pre-written Q&A
    ├── ARCHITECTURE.md · PRD.md · SPECIFICATIONS.md · PROJECT.md
    └── ENGINEER_NOTEBOOK.md · implementation-plan.md · 02_PulseQueue_Final_Implementation_Plan.md
```

**The three things in this project worth more than the rest combined** (from `docs/LEARNINGS.md`, and I agree after reading the code):

1. **The layered architecture** — because it's why most of the tests need nothing running.
2. **The delayed-set backoff** — because it's where an implementation detail was quietly dominating a benchmark.
3. **The visibility timeout** — because it's where a correct mechanism turned out to have created the failure mode it was protecting against.

Lead with #3 in interviews. It's the one that sounds like production experience.
