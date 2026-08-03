# PulseQueue — Implementation Walkthrough

Everything that was built, in the order it was built, with the reasoning behind
each piece. This is the document to read if you want to be able to defend any
line of this codebase.

**Companion documents**
- [DECISIONS.md](DECISIONS.md) — the *why* for every significant choice, with alternatives and costs
- [RUNBOOK.md](RUNBOOK.md) — how to run it, watch it, and inspect every layer
- [LEARNINGS.md](LEARNINGS.md) — what broke during the build

---

## Table of contents

1. [What was built, in numbers](#1-what-was-built-in-numbers)
2. [Environment setup](#2-environment-setup)
3. [How to read this codebase](#3-how-to-read-this-codebase)
4. [The core layer](#4-the-core-layer--infrastructure-with-no-business-logic)
5. [The data layer](#5-the-data-layer--model-and-schemas)
6. [The Redis layer](#6-the-redis-layer--queueservice)
7. [The repository layer](#7-the-repository-layer--jobrepository)
8. [The service layer](#8-the-service-layer)
9. [The registry and handlers](#9-the-registry-and-handlers)
10. [The worker](#10-the-worker)
11. [The API layer](#11-the-api-layer)
12. [The real-time layer](#12-the-real-time-layer)
13. [Traced end-to-end flows](#13-traced-end-to-end-flows)
14. [The test suite](#14-the-test-suite)
15. [Packaging](#15-packaging--docker-and-ci)
16. [Load testing](#16-load-testing)
17. [Bugs found and fixed](#17-bugs-found-in-the-original-spec-and-fixed)
18. [Deviations from the spec](#18-deviations-from-the-spec)

---

## 1. What was built, in numbers

| | |
|---|---|
| Application code | ~1,527 lines across 21 modules |
| Tests | 97 tests, ~824 lines |
| Tests needing zero infrastructure | 71 of 97 |
| Redis data structures | 4 (+ a lock namespace) |
| Database tables | 1 |
| Services | 5 |
| Docker services | 5 containers (API, 3 workers, Postgres, Redis) |
| API endpoints | 6 HTTP + 1 WebSocket |

Test distribution:

| File | Tests | Needs infrastructure? |
|---|---|---|
| `tests/test_api.py` | 26 | Yes — real Postgres |
| `tests/test_queue_service.py` | 22 | No |
| `tests/test_recovery_service.py` | 15 | No |
| `tests/test_job_service.py` | 14 | No |
| `tests/test_retry_service.py` | 13 | No |
| `tests/test_registry.py` | 7 | No |

---

## 2. Environment setup

### 2.1 Why Python 3.11 and not the system Python

The machine had Python **3.14.5** installed. The project's pinned dependency set
could not install on it:

- `pydantic 2.7.1` publishes no cp314 wheels
- `psycopg2-binary 2.9.9` publishes no cp314 wheels

Both would have needed to build from source, which on Windows means a C
compiler and the Postgres client headers. That is a lot of setup cost to run a
dependency set that was never tested on 3.14 anyway.

The Dockerfiles and the CI runner both use `python:3.11`. So the decision was to
make the local environment match them exactly:

```powershell
& "C:\Program Files\PyManager\pymanager.exe" install 3.11
# -> installs Python 3.11.9
```

**Why this matters:** local, Docker and CI now resolve every dependency to the
same wheel. Version skew between environments produces bugs that reproduce in
one place and not another, which are the most expensive kind to chase.

> Note: on this machine `py` on the PATH resolves to `python.exe`, not the
> PyManager launcher. `pymanager.exe` is the real entry point.

### 2.2 Creating the virtual environment

```powershell
& "$env:LOCALAPPDATA\Python\bin\python3.11.exe" -m venv venv
.\venv\Scripts\python.exe -m pip install --upgrade pip
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

**What a venv is and why:** a self-contained directory with its own Python
interpreter and its own `site-packages`. Without it, `pip install` writes into
the global interpreter, so two projects wanting different versions of the same
library conflict, and there is no way to reproduce which versions a project
actually needs. `venv/` is gitignored — it is derived from `requirements.txt`
and never committed.

### 2.3 The dependency set

```
fastapi==0.111.0            # async web framework, auto OpenAPI docs
uvicorn[standard]==0.30.1   # ASGI server that runs FastAPI
sqlalchemy==2.0.30          # ORM — the only thing that generates SQL
psycopg2-binary==2.9.9      # PostgreSQL driver SQLAlchemy sits on
redis==5.0.4                # Redis client (sync + asyncio)
pydantic==2.7.1             # validation and serialisation
pydantic-settings==2.2.1    # environment variables -> typed settings object
python-dotenv==1.0.1        # loads .env
pytest==8.2.0               # test runner
pytest-asyncio==0.23.7      # async test support
httpx==0.27.0               # HTTP client; required by FastAPI's TestClient
locust==2.29.0              # load testing
websockets==12.0            # WebSocket protocol support for uvicorn
```

Every version is pinned. `fastapi>=0.111` would mean two people cloning the
repo a month apart get different code, and a bug in the newer release becomes
indistinguishable from a bug in this project.

### 2.4 Configuration files

**`.env.example`** — committed. Documents every variable the app reads.
Contains no secrets.

**`.env`** — gitignored. The actual local values. On this machine it also
carries host-port overrides, because other containers already held 5432, 6379
and 8000:

```
POSTGRES_PORT=5433
REDIS_PORT=6380
API_PORT=8080
DATABASE_URL=postgresql://pulse:pulse@localhost:5433/pulsequeue
REDIS_URL=redis://localhost:6380/0
```

Two things read this file, for different purposes:
- **Docker Compose** substitutes `${POSTGRES_PORT}` into the port mappings
- **pydantic-settings** reads `DATABASE_URL` etc. when the app runs on the host

**`.gitignore`** — keeps `venv/`, `__pycache__/`, `.env`, `.pytest_cache/` and
load-test CSVs out of the repository.

**`.dockerignore`** — keeps `venv/`, `.git/`, `tests/` and `docs/` out of the
build context. Without it, Docker uploads the entire venv to the daemon on
every build, which is slow, and bakes host-platform binaries into a Linux
image, which is wrong.

---

## 3. How to read this codebase

### 3.1 The dependency rule

There is exactly one architectural rule, and everything else follows from it:

```
    API Router  ──calls──▶  Service  ──calls──▶  Repository  ──▶  Database
        │                      │
        │                      └──uses──▶  QueueService  ──▶  Redis
        │
        └── never touches SQL or Redis directly
```

**Dependencies only flow downward.** Concretely:

| Layer | May not |
|---|---|
| Router | contain SQL, call Redis, or import a repository |
| Service | build an HTTP response or import FastAPI |
| Repository | touch Redis, or know what HTTP is |
| Model | contain behaviour of any kind |

This is enforced by a test, not by discipline —
`test_routers_contain_no_sql_or_redis_calls` reads
[`app/api/routers/jobs.py`](../app/api/routers/jobs.py) and asserts it contains
no `self._db`, no `zadd`, no `SELECT`, and no repository import.

### 3.2 Reading order

Build order was bottom-up, and that is also the best reading order — each file
only depends on ones you have already seen:

```
1. core/config.py        settings
2. core/clock.py         time
3. core/exceptions.py    domain errors
4. core/database.py      engine, session factory, Base
5. core/redis.py         client singleton
6. models/job.py         the jobs table
7. schemas/job.py        the API contract
8. services/queue_service.py    all Redis operations
9. repositories/job_repository.py   all SQL
10. services/retry_service.py       retry policy
11. services/recovery_service.py    crash recovery
12. services/metrics_service.py     stats
13. services/job_service.py         submit/fetch/list/manual-retry
14. registry/job_registry.py        plugin lookup
15. handlers/builtin.py             the job implementations
16. workers/worker.py               the executor
17. api/dependencies.py             DI wiring
18. api/routers/jobs.py             HTTP
19. websocket/stream.py             live updates
20. main.py                         assembly
```

### 3.3 The two processes

This is the single most important structural fact, and it is easy to miss:

**The API and the worker are separate operating-system processes that share no
memory.** They communicate only through Postgres and Redis.

```
┌────────────────────────┐          ┌────────────────────────┐
│   API process          │          │   Worker process ×3    │
│   uvicorn app.main:app │          │   python -m app.workers│
├────────────────────────┤          ├────────────────────────┤
│ routers/jobs.py        │          │ workers/worker.py      │
│ JobService             │          │ RetryService           │
│ MetricsService         │          │ RecoveryService        │
│ QueueService           │          │ QueueService           │
│ JobRepository          │          │ JobRepository          │
│ websocket/stream.py    │          │ handlers/builtin.py    │
└───────┬────────────┬───┘          └────┬──────────────┬────┘
        │            │                   │              │
        ▼            ▼                   ▼              ▼
   ┌─────────┐  ┌────────┐         ┌────────┐    ┌─────────┐
   │Postgres │  │ Redis  │         │ Redis  │    │Postgres │
   └─────────┘  └────────┘         └────────┘    └─────────┘
```

Both import the same modules, but each has its **own** instance of everything —
its own connection pool, its own registry dict, its own settings object. The
API never calls a worker function; it puts a job id in Redis and a worker picks
it up.

---

## 4. The core layer — infrastructure with no business logic

### 4.1 `app/core/config.py`

**Why it exists.** Every environment-derived setting is read here and nowhere
else. No other module in the codebase imports `os` or `dotenv`. When a setting
needs changing, there is exactly one file to open.

**How it works.** `pydantic-settings` reads each field from the environment
(case-insensitively) or from `.env`, coerces it to the annotated type, and fails
loudly at startup if a value cannot be coerced. A typo'd `BASE_RETRY_DELAY=two`
is a crash at boot, not a `TypeError` in a worker three hours later.

**The settings and what each one does:**

| Setting | Default | Effect |
|---|---|---|
| `database_url` | `postgresql://pulse:pulse@localhost:5432/pulsequeue` | Postgres DSN |
| `redis_url` | `redis://localhost:6379/0` | Redis DSN |
| `worker_concurrency` | 3 | Documented replica count |
| `dequeue_timeout` | 1 | Seconds `BZPOPMIN` blocks per loop |
| `max_retry_attempts` | 3 | Default attempt budget when a client omits it |
| `base_retry_delay` | 2 | Backoff base in seconds |
| `visibility_timeout` | 300 | Seconds in RUNNING before a job is presumed abandoned |
| `recovery_interval` | 30 | Seconds between recovery sweeps |
| `log_level` | INFO | Root logger level |

`settings` is a module-level singleton, imported as
`from app.core.config import settings`. Constructed once at import, so the
environment is read once per process.

Defaults point at localhost so that `pytest` works on a fresh clone without a
`.env` file at all.

### 4.2 `app/core/clock.py`

**Why it exists.** Three layers need "what time is it in UTC" — the ORM model
for row timestamps, `QueueService` for sorted-set scores, and
`RecoveryService` for visibility-timeout cutoffs. Centralising it keeps the
representation consistent and gives tests one place to control the clock.

```python
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def epoch_ms() -> int:
    return int(utcnow().timestamp() * 1000)
```

**Why not `datetime.utcnow()`,** which the original spec used: it is deprecated
as of Python 3.12, and it returns a **naive** datetime that merely happens to
hold UTC. Comparing a naive datetime to an aware one raises `TypeError` — and
`RecoveryService` compares `job.updated_at` against a computed cutoff. With
naive timestamps that comparison is a latent crash in the one code path that
only runs *after* something else has already gone wrong. That is the worst
possible place for a crash to hide.

`epoch_ms` returns an integer because integer sorted-set scores stay exactly
representable in a float64 and keep ordering intuitive.

### 4.3 `app/core/exceptions.py`

**Why it exists.** The service layer must leak neither persistence details
upward nor HTTP details downward.

- If `JobService` raised `sqlalchemy.exc.IntegrityError`, the router would need
  to import SQLAlchemy to catch it — a dependency pointing the wrong way.
- If it raised `fastapi.HTTPException`, the service would be unusable from the
  worker, which speaks no HTTP.

So the repository translates persistence errors into domain errors, services
raise them, and the router is the single place that maps them to status codes.

```python
class PulseQueueError(Exception): ...
class DuplicateIdempotencyKey(PulseQueueError): ...   # -> resolved internally
class JobNotFound(PulseQueueError): ...               # -> HTTP 404
class InvalidStateTransition(PulseQueueError): ...    # -> HTTP 409
```

**Why not `ValueError`,** which the original spec used: `ValueError` is also
what a bad `int()` raises. The router cannot distinguish "illegal state
transition" from "a bug in my code", so both become a 400 and a genuine bug is
reported to the client as their fault.

### 4.4 `app/core/logging.py`

**Why it exists.** Both entry points need identical, predictable formatting, and
neither should configure handlers inline.

```python
def configure_logging(component: str) -> None:
```

The `component` tag ("api" or "worker") is embedded in every line, so
interleaved `docker compose logs` output stays readable. Output goes to stdout
because that is where container logs are read from.

It is idempotent (guarded by a module flag) so a uvicorn reload or a test that
imports the app twice does not stack duplicate handlers. It also silences
`uvicorn.access` and `sqlalchemy.engine`, which at INFO drown out the job
lifecycle events you actually want to watch.

Every other module does `log = logging.getLogger(__name__)` and never touches
levels or handlers.

### 4.5 `app/core/database.py`

**Why it exists.** Connection plumbing only. No models, no queries, no
business logic. Lowest layer in the dependency chain: everything points at it,
it points at nothing.

```python
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    future=True,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Base(DeclarativeBase): ...
```

**Line by line:**

- **`create_engine`** builds a *connection pool*, not a connection. Opening a
  TCP connection and authenticating costs milliseconds; doing it per request
  would dominate the latency budget.
- **`pool_pre_ping=True`** issues a cheap `SELECT 1` before handing out a pooled
  connection. A connection killed by a container restart or an idle timeout is
  transparently replaced instead of surfacing as a random `OperationalError`
  mid-request. This is what makes `docker compose restart postgres` survivable.
- **`pool_recycle=1800`** proactively discards connections older than 30
  minutes — shorter than the idle timeout of every managed Postgres provider
  this might deploy to.
- **`autocommit=False, autoflush=False`** means the repository decides
  explicitly when to commit. With autoflush on, a read could silently flush
  half-built objects to the database.
- **`SessionLocal`** is a *factory*. Each call returns a new session. A session
  is a unit of work and an identity map; sharing one across requests or threads
  is a correctness bug.

**`init_db()`** creates missing tables, wrapped in a retry loop:

```python
def init_db(retries: int = 15, delay: float = 2.0) -> None:
    from app.models import job  # noqa: F401
    ...
```

- The import is **inside the function** because `app/models/job.py` imports
  `Base` from this module — a top-level import would be circular. Importing it
  is what registers the table on `Base.metadata`.
- The **retry loop** exists because application containers can beat Postgres to
  readiness even behind a healthcheck, and crash-looping on startup is a worse
  failure mode than waiting.

**Why `create_all()` and not Alembic:** one table. Alembic would add a
migrations directory, a revision chain, and an `alembic upgrade head` step to
every deployment, to manage a schema that fits on a screen. Documented as a
tradeoff in [DECISIONS.md D10](DECISIONS.md), listed under Future Improvements.

### 4.6 `app/core/redis.py`

```python
_client = redis.from_url(
    settings.redis_url,
    decode_responses=True,
    socket_keepalive=True,
    health_check_interval=30,
)

def get_redis_client() -> redis.Redis:
    return _client
```

**Why a singleton.** redis-py's client is a thin handle over a connection pool
and is thread-safe. One per process is correct; one per request would leak
pools.

- **`decode_responses=True`** — we only ever store UTF-8 job ids and JSON
  strings, so decoding at the client boundary keeps every consumer free of
  bytes handling. Without it, `dequeue()` returns `b"uuid"` and every
  comparison silently fails.
- **`health_check_interval=30`** — pings idle connections so a stale one is
  replaced rather than failing the next command.

**Why this is separate from `QueueService`:** `QueueService` receives a client
by injection and never constructs one. That is the entire reason all 22 of its
tests can run against a `MagicMock` with no Redis installed.

---

## 5. The data layer — model and schemas

### 5.1 `app/models/job.py`

**Why it exists.** A data *shape*, nothing more. No methods, no validation, no
behaviour. Putting `def retry(self)` on this class would smear business logic
into the persistence layer.

**The state machine, as an enum:**

```python
class JobStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"
```

Inheriting from `str` means these serialise to their lowercase values in JSON
and compare equal to plain strings, so the API contract stays readable with no
translation layer.

**The table:**

| Column | Type | Why |
|---|---|---|
| `id` | UUID PK | Generated client-side, so the id is known before the INSERT and there is no sequence to collide across instances |
| `idempotency_key` | VARCHAR(255) UNIQUE NULL, indexed | The **database** is the real enforcement point for idempotency |
| `job_type` | VARCHAR(100) NOT NULL | Registry lookup key |
| `payload` | JSONB NOT NULL | Arbitrary handler input |
| `priority` | INT NOT NULL, default 3 | 1 = highest, 5 = lowest |
| `status` | ENUM `job_status`, indexed | Current state |
| `attempts` | INT NOT NULL, default 0 | Executions **started** |
| `max_attempts` | INT NOT NULL, default 3 | Dead-letter threshold |
| `created_at` | TIMESTAMPTZ | |
| `updated_at` | TIMESTAMPTZ, `onupdate` | Doubles as the visibility-timeout heartbeat |
| `result` | JSONB NULL | Handler output, or error detail |

**Details worth understanding:**

- **`JSONB` not `JSON`.** Binary storage, and it can be indexed later if payload
  querying ever becomes a requirement.

- **`values_callable` on the enum:**
  ```python
  status = Column(SAEnum(JobStatus, name="job_status",
      values_callable=lambda e: [m.value for m in e]), ...)
  ```
  SQLAlchemy's default is to store the member **name** (`'QUEUED'`). Without
  this, Postgres holds uppercase while every API response says lowercase.
  Everything works — the ORM translates both ways — but a `psql` session shows
  something different from the API, which is a miserable thing to debug.

- **`updated_at` is load-bearing.** It is not just an audit column. It is the
  heartbeat `RecoveryService` reads to decide whether a RUNNING job has been
  abandoned. That is why `JobRepository.touch()` exists.

- **The composite indexes:**
  ```python
  Index("ix_jobs_status_updated_at", "status", "updated_at")   # recovery sweep
  Index("ix_jobs_status_created_at", "status", "created_at")   # GET /jobs?status=
  ```
  Each serves one hot query. Without the first, every recovery sweep is a full
  table scan on a growing table.

### 5.2 `app/schemas/job.py`

**Why it exists.** These define the wire format. They are deliberately **not**
the ORM model, so the database can gain a column without it appearing in the
API, and the API can change without a migration.

**`JobCreate`** — the `POST /jobs` body, with validation as declaration:

```python
job_type: str = Field(..., min_length=1, max_length=100)
payload: dict[str, Any] = Field(default_factory=dict)
priority: int = Field(default=3, ge=1, le=5)
idempotency_key: Optional[str] = Field(default=None, max_length=255)
max_attempts: Optional[int] = Field(default=None, ge=1, le=10)
```

- `ge=1, le=5` on priority means a `priority: 99` request is rejected with a 422
  by the framework. No hand-written validation in the router.
- **`default_factory=dict`, not `= {}`.** A bare dict literal as a default would
  be a single shared mutable object across every instance — a classic Python
  bug where one request's payload mutation leaks into the next.
- **`max_attempts` defaults to `None`, not 3.** `None` means "the client did not
  express a preference", and `JobService` then applies the server's configured
  `MAX_RETRY_ATTEMPTS`. Hardcoding 3 in the schema would make the
  `MAX_RETRY_ATTEMPTS` setting dead configuration — which it was in the
  original spec.

**`JobResponse`** — carries `model_config = {"from_attributes": True}`, which
lets FastAPI construct it directly from a SQLAlchemy row.

**`StatsResponse`** — five integers, one per status.

---

## 6. The Redis layer — QueueService

[`app/services/queue_service.py`](../app/services/queue_service.py) — 219 lines,
and the most interesting file in the project.

**Why it exists.** Every key name, every score calculation, and all pub/sub
plumbing live here. If Redis were swapped for SQS, this is the one file that
changes.

### 6.1 The key layout

```
pq:queue     ZSET   ready jobs, score = priority band + enqueue time
pq:delayed   ZSET   jobs waiting out a retry backoff, score = ready-at time
pq:dlq       LIST   permanently failed job ids, newest first
pq:updates   chan   pub/sub feed the WebSocket endpoint consumes
pq:lock:*    STRING short-lived mutexes (SET NX EX)
```

### 6.2 Priority scoring — the arithmetic

```python
PRIORITY_BAND = 10**13

def _job_score(self, priority: int) -> float:
    return float(priority * PRIORITY_BAND + epoch_ms())
```

A sorted set orders by score; `ZPOPMIN`/`BZPOPMIN` pop the **lowest**. So lower
score must mean "run me first".

**How the two-level sort works.** The score packs two values into one number:

```
score = priority × 10¹³  +  milliseconds-since-epoch
        └─── band ────┘     └──── tiebreaker ────┘
```

An epoch-millisecond timestamp is around 1.7 × 10¹², comfortably below 10¹³.
So:

| Job | Priority | Score |
|---|---|---|
| A | 1 | 1.0000017×10¹³ |
| B | 2 | 2.0000017×10¹³ |

Band 1 can never overlap band 2, no matter how old a job gets. **Priority
always dominates; age only breaks ties.** Maximum score is about 5.2 × 10¹³,
well inside the ~9 × 10¹⁵ range a float64 represents exactly, so no two
distinct scores can collide through rounding.

**Why not the original spec's tiebreaker.** It used
`uuid.UUID(job_id).int % 1_000_000 / 1e9`. Its docstring claimed FIFO ordering,
but a UUID is random, so the tiebreaker was random — a job submitted an hour
ago could sit behind one submitted a second ago. Using the enqueue timestamp
makes the documented behaviour actually true.

Two tests lock this down: `test_priority_dominates_age` guards the band width,
and `test_equal_priority_is_fifo` guards the ordering.

### 6.3 Dequeue

```python
def dequeue(self, timeout: Optional[int] = None) -> Optional[str]:
    if timeout is None:
        return self.dequeue_nowait()
    result = self._r.bzpopmin(QUEUE_KEY, timeout=timeout)
    if not result:
        return None
    _key, job_id, _score = result
    return job_id
```

**The atomicity guarantee — this is the concurrency claim.** Redis executes
commands one at a time on a single thread. Of N workers blocked on the same
key, exactly one receives any given job. There is no lock to acquire, no lock
to leak, and no window to race in.

That is what makes "3+ workers, zero double-processing" *true* rather than
merely claimed — and it is why the answer to "how do you prevent two workers
taking the same job?" is "I don't; Redis does, structurally".

**Why blocking and not polling.** `BZPOPMIN` blocks until a job exists or the
timeout expires. The alternative — `ZPOPMIN` then `sleep(1)` — adds up to a
second of latency to every job on an idle queue. Measured pickup latency with
`BZPOPMIN` is **~25ms**; the poll loop would average ~500ms.

The 1-second timeout is **not a poll interval.** A job arriving mid-block
returns immediately. The timeout only bounds how long shutdown and the recovery
timer wait.

`dequeue_nowait()` is kept alongside it because unit tests want the
non-blocking form.

### 6.4 The delayed set — non-blocking backoff

```python
def enqueue_delayed(self, job_id, priority, delay_seconds) -> None:
    ready_at = epoch_ms() + int(delay_seconds * 1000)
    self._r.zadd(DELAYED_KEY, {f"{priority}:{job_id}": ready_at})
```

**Why the member is `"<priority>:<job_id>"`.** The delayed set is ordered by
*time*, so the score is the deadline. The job's priority has nowhere else to
live until it is promoted back into the ready queue, so it rides along in the
member string.

**Why this exists at all.** The obvious implementation of backoff is
`time.sleep(delay)` in the worker. That idles an entire worker process for the
whole backoff. With three workers, a 20% failure rate, and delays of 2/4/8s, a
large fraction of the fleet ends up asleep while jobs pile up behind it.

Measured during load testing: **five jobs backing off simultaneously while
`success` climbed from 30 to 40 in the same window.** Under the sleeping
design, all three workers would have been frozen through that period.

### 6.5 Promotion — and why it needs Lua

```lua
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due == 0 then return 0 end
local now  = tonumber(ARGV[3])
local band = tonumber(ARGV[4])
for _, member in ipairs(due) do
    local sep = string.find(member, ':')
    if sep then
        local priority = tonumber(string.sub(member, 1, sep - 1))
        local job_id   = string.sub(member, sep + 1)
        if priority and job_id ~= '' then
            redis.call('ZADD', KEYS[2], priority * band + now, job_id)
        end
    end
    redis.call('ZREM', KEYS[1], member)
end
return #due
```

**Why a script and not three commands.** The read ("which jobs are due?") and
the write ("move them") must be one indivisible step. Done as separate
`ZRANGEBYSCORE` + `ZADD` + `ZREM` calls, two workers could both observe the same
due job and enqueue it twice. Redis runs a script as a single atomic unit, so
the race cannot occur.

**Why not a lock instead.** A lock has to be acquired, held, released, and
given a TTL in case the holder dies. The script needs none of that — Redis
already serialises execution. Fewer moving parts, no failure mode.

Registered with `r.register_script(...)`, which uses `EVALSHA` with an automatic
`EVAL` fallback if the script is not yet cached server-side.

Every worker calls `promote_due()` at the top of its loop. Safe to run
concurrently from the whole fleet.

### 6.6 Membership — the check that prevents duplication

```python
def contains(self, job_id: str, priority: int) -> bool:
    if self._r.zscore(QUEUE_KEY, job_id) is not None:
        return True
    return self._r.zscore(DELAYED_KEY, f"{priority}:{job_id}") is not None
```

Used by `RecoveryService` before re-enqueueing something that looks orphaned.

**Why it is essential.** From the database alone, a genuinely orphaned job and a
job sitting in a deep backlog are **indistinguishable** — both are old rows in
QUEUED that nothing has touched. Only Redis can tell them apart. Without this
check, a queue that merely got behind would have every job in it enqueued a
second time.

Note it must check the delayed set with the *prefixed* member form, or every
job waiting out a backoff would look absent.

### 6.7 The distributed lock

```python
def acquire_lock(self, name, ttl_seconds, token) -> bool:
    return bool(self._r.set(f"{LOCK_PREFIX}{name}", token, nx=True, ex=ttl_seconds))
```

`SET key value NX EX ttl` is atomic — exactly one caller succeeds. The TTL is a
deadlock guard: a worker that dies holding the lock releases it implicitly on
expiry.

**Release is owner-checked, via Lua:**

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
```

**Why not a plain `DEL`.** Worker A takes the lock. A stalls; the lock expires;
worker B acquires it. A wakes up and `DEL`s — deleting *B's* lock. The
compare-and-delete makes that impossible, and it must be atomic for the same
reason.

Used so only one worker in the fleet runs the recovery sweep.

### 6.8 Pub/sub

```python
def publish_update(self, job_id: str, status: str) -> None:
    status_value = getattr(status, "value", status)
    self._r.publish(CHANNEL, json.dumps({"job_id": job_id, "status": status_value}))
```

Fire-and-forget by design. The dashboard is an observability nicety and a
dropped update must never affect job execution. Redis pub/sub has no delivery
guarantee, and that is the correct tradeoff — the dashboard reconciles against
`GET /jobs/stats` on every event.

The `getattr(status, "value", status)` normalises a `JobStatus` member to its
lowercase string, so the wire format is identical whether callers pass an enum
or a string.

---

## 7. The repository layer — JobRepository

[`app/repositories/job_repository.py`](../app/repositories/job_repository.py) —
the only file in the codebase that issues SQL.

### 7.1 Writes

**`create()`** — and the exception translation that matters:

```python
self._db.add(job)
try:
    self._db.commit()
except IntegrityError as exc:
    self._db.rollback()
    raise DuplicateIdempotencyKey(...) from exc
self._db.refresh(job)
```

- **The `rollback()` is mandatory.** A session is poisoned after a failed flush;
  it must be rolled back before it can be used again to look up the winning row.
  Skip it and the subsequent query raises `PendingRollbackError`.
- **The translation** means `JobService` catches a domain exception, not a
  SQLAlchemy one, so nothing above this layer imports SQLAlchemy.
- **`refresh()`** re-reads the row so server-side defaults are populated on the
  returned object.

**`increment_attempts()`** — called **before** the handler runs, not after:

```python
job.attempts += 1
```

**Why the ordering matters enormously.** If the counter moved after execution, a
job that crashes its worker would never record an attempt — so recovery hands it
to another worker, which also dies, forever. Incrementing first makes a
poison-pill job burn its budget and dead-letter after `max_attempts` crashes.

The counter therefore reads as "executions **started**", which is exactly the
number the dead-letter threshold wants.

**`update_status(job, status, result=None)`** — `result=None` means "leave the
existing result alone", not "clear it". A job moving FAILED → QUEUED for a retry
should keep the error explaining why.

**`reset_attempts()`** — exists because of a real bug in the original spec.
`JobService.manual_retry` did `job.attempts = 0` in Python after the commit, so
the reset was **never persisted**. Routing it through the repository is what
makes it actually happen.

**`touch()`** — bumps `updated_at` only. This is the heartbeat a long-running
job would use to hold off the visibility timeout.

### 7.2 Reads

**`list_stuck_running(cutoff, limit)`** — jobs still marked RUNNING but untouched
since `cutoff`. These are jobs whose worker died. They are no longer in Redis
(atomically popped), so nothing will ever pick them up unless something goes
looking.

**`list_stale_pending(cutoff, limit)`** — jobs in QUEUED or FAILED untouched
since `cutoff`. QUEUED means it *should* be in the ready queue; FAILED means it
*should* be in the delayed set. If the row is stale and the id is in neither,
the enqueue was lost.

**`count_by_status()`** — one `GROUP BY`, not five `COUNT` queries:

```python
rows = self._db.query(Job.status, func.count(Job.id)).group_by(Job.status).all()
counts = {member: 0 for member in JobStatus}
```

Pre-seeding all five keys with zero means a status with no rows still appears,
so `MetricsService` never has to handle a missing key.

---

## 8. The service layer

### 8.1 `JobService` — submit, fetch, list, manual retry

**Idempotency, enforced twice on purpose.** This is the most subtle logic in the
project:

```python
def submit(self, job_in: JobCreate) -> Job:
    if job_in.idempotency_key:
        existing = self._repo.get_by_idempotency_key(job_in.idempotency_key)
        if existing:
            return existing                    # (1) fast path

    try:
        job = self._repo.create(...)
    except DuplicateIdempotencyKey:
        winner = self._repo.get_by_idempotency_key(job_in.idempotency_key)
        if winner is None:
            raise
        return winner                          # (2) race resolution

    self._queue.enqueue(str(job.id), job.priority)
    self._queue.publish_update(str(job.id), JobStatus.QUEUED)
    return job
```

**(1) The pre-check** handles the ordinary case — a client retrying seconds or
minutes later — without attempting a doomed INSERT.

**(2) The constraint violation** handles what the pre-check structurally cannot.
The pre-check is a *check-then-act* sequence, and it is racy by construction:
two concurrent requests with the same key can **both** read "no existing job"
before either inserts. Only the database can adjudicate that, so we let it, and
the loser re-reads the winner's row.

**Measured:** 10 concurrent POSTs with one key → **1 distinct id, 0 errors**.
Without step 2, nine of those ten are HTTP 500s. This is exactly what the
original spec's code did.

**Why the enqueue comes last.** Commit first, enqueue second. If the process
dies in between, the job exists in Postgres as QUEUED and is recoverable. The
reverse ordering would put an id on the queue that no worker could ever resolve
to a row. (The remaining window is what the orphan sweep closes — see §8.3.)

**`manual_retry()`** — guards the state machine:

```python
if job.status != JobStatus.DEAD_LETTER:
    raise InvalidStateTransition(...)
job = self._repo.reset_attempts(job)
job = self._repo.update_status(job, JobStatus.QUEUED, result=None)
self._queue.remove_dead_letter(str(job.id))
self._queue.enqueue(str(job.id), job.priority)
```

Only DEAD_LETTER is a legal source state. Re-queueing a RUNNING job would
double-process it; re-queueing a SUCCESS job would re-fire a side effect that
already happened.

The `remove_dead_letter` call is also a bug fix over the original spec — without
it, the DLQ accumulates ids of jobs that have since been revived, and its depth
stops meaning "things needing human attention".

### 8.2 `RetryService` — the single owner of retry policy

**The backoff maths:**

```python
def compute_delay(self, attempt: int) -> float:
    return float(settings.base_retry_delay * (2**attempt))
```

Base 2s → attempt 0 gives 2s, 1 gives 4s, 2 gives 8s.

**Why exponential.** A transient dependency failure — a database failover, a
rate limit, a restarting service — typically resolves on a timescale of seconds
to tens of seconds. Backing off geometrically gives it room to recover, whereas
retrying every 2s indefinitely keeps the failing service pinned under load from
the very clients waiting for it to come back.

**The threshold:**

```python
def should_dead_letter(self, job: Job) -> bool:
    return job.attempts >= job.max_attempts
```

`>=` not `==`, so a miscount can never produce an infinite loop.

**The decision:**

```python
def handle_failure(self, job, exc, permanent: bool = False) -> JobStatus:
    if permanent or self.should_dead_letter(job):
        ... update_status(DEAD_LETTER) ; enqueue_dead_letter ; publish
        return JobStatus.DEAD_LETTER

    delay = self.compute_delay(max(job.attempts - 1, 0))
    ... update_status(FAILED, result={"error", "attempt", "retry_in"}) ; publish
    self._queue.enqueue_delayed(str(job.id), job.priority, delay)
    return JobStatus.FAILED
```

**The `attempts - 1` is a fix for an off-by-one in the original spec.**
`attempts` was already incremented for the execution that just failed, so the
first failure has `attempts == 1`. Passing that straight to `compute_delay`
gives `2 × 2¹ = 4s` for the first retry — a rung too high, and contradicting
the spec's own documented "2s, 4s, 8s".

**The `permanent` flag** is used for one case: no handler is registered for the
job type. No amount of backoff conjures a handler into existence; retrying
three times delays the real error by 14 seconds and buries it under retry
noise. Because retryable-versus-permanent is a *policy* distinction, it lives
here rather than as a special case in the worker.

**Note on the resulting status.** The job stays FAILED while it waits in the
delayed set, rather than flipping straight back to QUEUED. This is honest —
"failed, retrying in 4s" carries more information — but it means the `queued`
stat undercounts pending work. Documented as accepted debt.

**What `handle_failure` must never do:** block. There is a test that patches
`time.sleep` to raise, so reintroducing a sleep here fails the suite:

```python
def test_handle_failure_never_sleeps(svc, monkeypatch):
    def explode(_seconds):
        raise AssertionError("RetryService must not block the worker")
    monkeypatch.setattr("time.sleep", explode)
```

### 8.3 `RecoveryService` — two orphan paths

**Why this service exists at all.** Atomic dequeue removes the job from Redis
the instant a worker takes it. That atomicity is what prevents
double-processing — and it is also what creates a failure mode. A worker
`SIGKILL`ed mid-job leaves the job existing **nowhere** but as a row stuck in
RUNNING. Nothing will ever pick it up.

Graceful `SIGTERM` handling does not help. A process that stops executing
instructions runs no handler.

**This is the most instructive thing in the project: a correct mechanism created
a new failure mode.** Atomicity makes multi-worker safety true, and it makes
crash recovery necessary. Same property, two consequences.

**Path 1 — `requeue_stuck_jobs()`.** The visibility timeout, the same primitive
SQS exposes:

```python
cutoff = utcnow() - timedelta(seconds=timeout)
stuck = self._repo.list_stuck_running(cutoff, limit=limit)
for job in stuck:
    if job.attempts >= job.max_attempts:
        ... DEAD_LETTER ...          # poison-pill guard
    else:
        ... QUEUED + enqueue ...
```

A job in RUNNING for longer than any legitimate execution could take is presumed
abandoned. Because the attempt was consumed before the handler ran, a job that
reliably kills its worker still exhausts its budget and dead-letters instead of
crash-looping the fleet.

**Path 2 — `requeue_orphaned_jobs()`.** `submit()` commits the row and *then*
enqueues. There is no transaction spanning Postgres and Redis, so a process
death between those two statements leaves a QUEUED row in no queue.

This was **found empirically, not theorised**: after a test run, two jobs sat
QUEUED forever while `ZCARD pq:queue` was 0.

```python
for job in self._repo.list_stale_pending(cutoff, limit=limit):
    if self._queue.contains(str(job.id), job.priority):
        continue                     # genuinely waiting its turn
    if job.status != JobStatus.QUEUED:
        self._repo.update_status(job, JobStatus.QUEUED)
    self._queue.enqueue(str(job.id), job.priority)
```

The `contains` guard is the whole safety argument — see §6.6.

**The critical tunable.** `VISIBILITY_TIMEOUT` must comfortably exceed the
slowest legitimate handler. Set it too low and healthy long-running jobs get
reclaimed and duplicated — which *is* the at-least-once tradeoff, now explicit
and tunable rather than implicit.

### 8.4 `MetricsService`

Thin by design. Wraps `count_by_status()` and returns a `StatsResponse`.

**Why it exists rather than the router calling the repository.** Two consumers
(REST now, Prometheus later) must never end up with two subtly different
definitions of "how many jobs are running".

It reports **Postgres** state, not Redis queue depth, because Postgres is the
source of truth. A job can be QUEUED in Postgres while sitting in the delayed
set rather than the ready queue; the database view answers "what is the system
actually holding".

---

## 9. The registry and handlers

### 9.1 `app/registry/job_registry.py`

```python
_registry: dict[str, Callable[[dict], dict]] = {}

def job_handler(job_type: str) -> Callable:
    def decorator(fn):
        existing = _registry.get(job_type)
        if existing is not None and existing is not fn:
            raise ValueError(f"job_type '{job_type}' is already handled by ...")
        _registry[job_type] = fn
        return fn
    return decorator
```

**How a decorator achieves this.** `@job_handler("send_email")` is evaluated at
**import time**. `job_handler("send_email")` returns `decorator`; Python calls
`decorator(send_email)`; that stores the function in `_registry` and returns it
unchanged. So merely importing the module populates the registry — the worker
never needs a list of what exists.

**Why the duplicate check.** Silently letting the second registration win means
a copy-pasted decorator quietly disables a job type in production, and the
symptom appears nowhere near the cause.

**Why `get_handler` lists what is available on failure:**

```python
raise KeyError(f"No handler registered for job_type '{job_type}'. "
               f"Available: {sorted(_registry)}")
```

The overwhelmingly likely cause is a typo or a handler module that was never
imported. The error should name itself.

**The tradeoff.** Registration is an import side effect. If nothing imports
`handlers.builtin`, the registry is silently empty. Mitigated two ways: the
worker imports it explicitly at startup and **logs what it found**, and the
lookup error is self-diagnosing.

**Name the precedent.** Celery's `@task`, Flask's `@route`, pytest fixtures,
Django signals. This reframes the choice from "something I invented" to "the
standard solution".

### 9.2 `handlers/builtin.py`

The extension point. Adding a job type touches only this file.

**The handler contract:**
- takes the job's `payload` dict
- returns a JSON-serialisable dict, stored as `result`
- raises on failure; the message is stored and `RetryService` takes over

| Handler | Behaviour | Purpose |
|---|---|---|
| `send_email` | sleeps 0.5s, fails ~20% | Exercises retry/DLQ under ordinary traffic |
| `resize_image` | sleeps 1.0s, never fails | Deterministic success path |
| `generate_report` | sleeps 2.0s | Slow job that justifies priorities |
| `benchmark_noop` | returns immediately | Load testing (see §16) |

`EMAIL_FAILURE_RATE = 0.2` is a module constant so it is obvious this is a demo
affordance, and so tests can patch it to 0 for determinism.

---

## 10. The worker

[`app/workers/worker.py`](../app/workers/worker.py) — 214 lines, and the number
that matters is **zero**: the count of policy decisions in this file.

### 10.1 The main loop

```python
while not self._shutdown_requested:
    try:
        self._queue.promote_due()
        self._maybe_run_recovery()
        job_id = self._queue.dequeue(timeout=settings.dequeue_timeout)
        if job_id:
            self.process_job(job_id)
    except Exception as exc:
        log.exception(...)
        time.sleep(1)
```

**Ordering is deliberate.** Promotion runs *before* the blocking dequeue, so a
due retry is never left waiting behind an idle `BZPOPMIN`.

**The blanket `except` is intentional.** The loop must never die. A Redis blip
must degrade throughput, not take the worker down. The `sleep(1)` prevents a
tight error loop from burning CPU.

### 10.2 Graceful shutdown

```python
def request_shutdown(signum, _frame):
    self._shutdown_requested = True
```

The handler **only sets a flag.** The loop finishes the job in flight and then
exits, so a deploy or a `docker compose down` never abandons work mid-execution.

Both `SIGTERM` (what Docker sends) and `SIGINT` (Ctrl+C) are registered, each
wrapped in try/except because **Windows does not deliver a real `SIGTERM`**.

`stop_grace_period: 30s` in compose gives the handler time before Docker
escalates to `SIGKILL`. The default 10s is shorter than the slowest handler plus
its commit.

**Related and easy to get wrong:** the Dockerfile uses `CMD` in **exec form**.
Shell form (`CMD python -m ...`) runs the process under `/bin/sh`, which does
**not** forward `SIGTERM` to its child — graceful shutdown would silently never
fire, and every deploy would abandon an in-flight job with no error message
anywhere.

### 10.3 Processing one job

```python
db = SessionLocal()
repo = JobRepository(db)
retry = RetryService(repo, self._queue)
job = None
try:
    job_uuid = uuid.UUID(job_id)          # malformed id -> discard
    job = repo.get_by_id(job_uuid)
    if job is None: return                # no row -> nothing to do

    if job.status not in (JobStatus.QUEUED, JobStatus.FAILED):
        return                            # duplicate delivery -> no-op

    repo.increment_attempts(job)
    repo.update_status(job, JobStatus.RUNNING)
    self._queue.publish_update(job_id, JobStatus.RUNNING)

    try:
        handler = get_handler(job.job_type)
    except KeyError as exc:
        retry.handle_failure(job, exc, permanent=True)
        return

    result = handler(job.payload)
    repo.update_status(job, JobStatus.SUCCESS, result=result)
    self._queue.publish_update(job_id, JobStatus.SUCCESS)

except Exception as exc:
    if job is None:
        log.exception(...)                # nothing to transition
        return
    retry.handle_failure(job, exc)
finally:
    db.close()
```

**Details that matter:**

- **A fresh session per job, closed in `finally`.** Sessions are the unit of
  work; holding one open across an idle poll loop would pin a pool connection
  for the worker's lifetime.

- **`job = None` before the `try`.** The original spec's code referenced `job`
  in its `except` block without this, so if `repo.get_by_id` itself threw, the
  error handler raised `UnboundLocalError` — masking the real exception.

- **The status guard is the at-least-once contract in code.** A job can
  legitimately reach a worker twice — recovered by the visibility timeout while
  the original worker was merely slow, for instance. Duplicate delivery is
  *expected*, not exceptional, and this cheap check turns most duplicates into
  no-ops.

- **The nested `try` around `get_handler`** is what routes an unregistered job
  type to the permanent-failure path.

- **The inner try/except around `handle_failure`** (in the real file) means that
  if even the failure path fails — database gone — the job is left in RUNNING
  and `RecoveryService` reclaims it later. Which is precisely why that service
  exists.

### 10.4 The recovery timer

```python
if now - self._last_recovery_sweep < settings.recovery_interval:
    return
self._last_recovery_sweep = now

if not self._queue.acquire_lock(RECOVERY_LOCK, ttl_seconds=..., token=self._worker_id):
    return                      # another worker is handling it
```

Every worker runs its own timer, but the lock means only one actually scans. The
lock TTL matches the interval, so a worker that dies mid-sweep simply lets the
next one pick it up.

`self._worker_id` is `hostname-pid-random6`, used both as the log identifier and
as the lock's ownership token.

---

## 11. The API layer

### 11.1 `app/api/dependencies.py`

FastAPI's `Depends()` providers, in one place. Routers import from here; they
never import `core/database.py` or `core/redis.py` directly.

```python
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
```

**How `yield` works here.** FastAPI runs the code up to `yield` before the
request, injects the yielded value, then runs the code after `yield` once the
response is sent. The `finally` is what stops a raised exception from leaking a
connection.

The providers compose:

```
get_redis → get_queue_service ┐
                              ├→ get_job_service
get_db ───────────────────────┘
get_db → get_metrics_service
```

So a route function receives a fully assembled service and its body is one
delegating call. **This is also the seam tests override** —
`app.dependency_overrides[get_redis] = lambda: fake_redis`.

### 11.2 `app/api/routers/jobs.py`

82 lines for six endpoints. Note what is absent: no SQL, no Redis, no retry
logic, no status arithmetic.

| Route | Body |
|---|---|
| `POST /jobs` | `return svc.submit(job_in)` |
| `GET /jobs/stats` | `return svc.get_stats()` |
| `GET /jobs` | `return svc.list(status, limit)` |
| `GET /jobs/{job_id}` | fetch, 404 if missing |
| `POST /jobs/{job_id}/retry` | delegate, map domain errors to 404/409 |

**Route ordering is load-bearing.** `/jobs/stats` is declared **before**
`/jobs/{job_id}` because FastAPI matches in declaration order. `job_id:
uuid.UUID` would reject `"stats"` with a 422 anyway, but relying on a validation
failure to route correctly is a trap for whoever later loosens that type to
`str`.

**Path is `""` not `"/"`.** With `prefix="/jobs"`, `@router.post("/")` produces
`/jobs/`, so `curl -X POST /jobs` gets a 307 redirect. `""` gives the documented
path directly.

**Exception mapping is this layer's only real logic:**

```python
except JobNotFound as exc:
    raise HTTPException(status_code=404, detail=str(exc)) from exc
except InvalidStateTransition as exc:
    raise HTTPException(status_code=409, detail=str(exc)) from exc
```

409 Conflict rather than 400 Bad Request: the request is well-formed, it just
conflicts with the resource's current state.

### 11.3 `app/main.py`

Assembly only.

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging("api")
    init_db()
    yield
```

**Why schema creation is in the lifespan and not at module scope.** In the
original spec, `Base.metadata.create_all(bind=engine)` ran at import. That meant
importing `app.main` — which every test does — required a live database, so the
entire suite needed docker-compose just to **collect**. Import-time side effects
that reach the network are worth hunting down.

```python
DASHBOARD_DIR = Path(__file__).resolve().parent.parent / "dashboard"
app.mount("/dashboard", StaticFiles(directory=str(DASHBOARD_DIR), html=True))
```

Resolved from `__file__`, not the working directory, so `uvicorn app.main:app`
behaves the same launched from the repo root, from a container's `/app`, or from
a test runner. `html=True` serves `index.html` for the directory root.

---

## 12. The real-time layer

### 12.1 `app/websocket/stream.py`

```python
await websocket.accept()
client = aioredis.from_url(settings.redis_url, decode_responses=True)
pubsub = client.pubsub()
await pubsub.subscribe(CHANNEL)

while True:
    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
    if message is None:
        continue
    await websocket.send_text(message["data"])
```

**Why an async Redis client.** This runs on the event loop. The synchronous
singleton would block it, stalling every other request the process is serving.

**Why `get_message(timeout=...)` and not `async for ... in pubsub.listen()`.**
`listen()` blocks indefinitely, so a client that vanished without a close frame
is never noticed and cancellation is not honoured promptly. The 1-second timeout
makes the loop come up for air.

**The exception handling:**

```python
except (WebSocketDisconnect, asyncio.CancelledError):
    pass
except Exception as exc:
    log.debug("Dashboard stream closed: %s", exc)
finally:
    await pubsub.unsubscribe(CHANNEL)
    await pubsub.aclose()
    await client.aclose()
```

A dead socket surfaces as a `RuntimeError` from Starlette rather than
`WebSocketDisconnect`. Either way the connection is over, so it unwinds cleanly
rather than escaping as a 500.

**Known limitation:** one Redis connection per dashboard client. Fine for an ops
view, wrong for a public page with thousands of viewers. Listed in Future
Improvements.

### 12.2 `dashboard/index.html`

A single static file — no build step, no framework, no `node_modules`.

**The reconciliation pattern, which is the interesting part:**

```javascript
ws.onmessage = (event) => {
  const { job_id, status } = JSON.parse(event.data);
  appendEvent(job_id, status);
  scheduleStatsRefresh();       // debounced fetch of /jobs/stats
};
```

The WebSocket feed is the low-latency signal; `GET /jobs/stats` is the
authoritative one. Redis pub/sub has no delivery guarantee, so a dropped message
must not leave the counters permanently wrong — every event triggers a
reconciling fetch.

**That fetch is debounced to 300ms**, because under load the feed can deliver
hundreds of events per second and one HTTP request each would put more load on
the API than the jobs themselves.

**Two fixes over the original spec:**

```javascript
const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
```
A hardcoded `ws://` is blocked as mixed content by every browser the moment this
is deployed behind HTTPS. This was a latent "works locally, broken in
production" bug.

```javascript
entry.appendChild(document.createTextNode(`... job ${jobId.slice(0,8)}… → ${status}`));
```
`textContent`/`createTextNode` rather than `innerHTML`. `job_id` and `status`
arrive over the network, and building markup from network data is how XSS
happens.

Plus exponential-backoff reconnect capped at 15s, so an API restart does not
leave a recruiter looking at a dead page.

---

## 13. Traced end-to-end flows

### 13.1 Submitting a job

```
1. POST /jobs {"job_type":"send_email","payload":{"to":"a@b.com"}}
2. FastAPI validates the body against JobCreate
      -> priority out of 1..5? 422, never reaches your code
3. Depends() builds JobService(db=Session, queue=QueueService(redis))
4. JobService.submit()
   a. idempotency_key present? -> JobRepository.get_by_idempotency_key()
   b. JobRepository.create()
         INSERT INTO jobs (...) VALUES (...)
         COMMIT
      -> IntegrityError? rollback, re-read, return the winner
   c. QueueService.enqueue(job_id, priority)
         ZADD pq:queue <priority*1e13 + now_ms> <job_id>
   d. QueueService.publish_update(job_id, "queued")
         PUBLISH pq:updates {"job_id":"...","status":"queued"}
5. FastAPI serialises the ORM row through JobResponse
6. HTTP 201 {"id":"...","status":"queued","attempts":0,...}
```

Elapsed: ~60ms P50 under load. The client is now free; nothing has executed yet.

### 13.2 Executing it (happy path)

```
1. Worker loop: QueueService.promote_due()
      EVALSHA <lua> pq:delayed pq:queue ...        -> 0 due
2. Worker loop: _maybe_run_recovery()              -> interval not elapsed
3. QueueService.dequeue(timeout=1)
      BZPOPMIN pq:queue 1
      -> returns instantly (~25ms after submit); exactly ONE worker gets it
4. process_job(job_id)
   a. SessionLocal() -> fresh session
   b. JobRepository.get_by_id()      SELECT * FROM jobs WHERE id = ...
   c. status in (QUEUED, FAILED)?    yes
   d. increment_attempts()           UPDATE jobs SET attempts=1, updated_at=now
   e. update_status(RUNNING)         UPDATE jobs SET status='running', ...
   f. publish_update("running")      PUBLISH pq:updates
   g. get_handler("send_email")      registry dict lookup
   h. handler(payload)               time.sleep(0.5); returns {"sent":true,...}
   i. update_status(SUCCESS, result) UPDATE jobs SET status='success', result=...
   j. publish_update("success")      PUBLISH pq:updates
   k. db.close()                     connection returns to the pool
```

The dashboard receives three messages (`running`, then `success`) and refreshes
its counters.

### 13.3 Executing it (failure → retry → success)

```
attempt 1: attempts=1, RUNNING, handler raises ConnectionError
  RetryService.handle_failure(job, exc)
    should_dead_letter? 1 >= 3 -> no
    delay = compute_delay(max(1-1,0)) = 2 * 2^0 = 2.0s
    update_status(FAILED, result={"error":..., "attempt":1, "retry_in":2.0})
    publish_update("failed")
    enqueue_delayed(job_id, priority, 2.0)
        ZADD pq:delayed <now_ms + 2000> "3:<job_id>"
    RETURNS IMMEDIATELY  <-- the worker is now free for other jobs

~2s later, on some worker's next loop:
  promote_due()
    ZRANGEBYSCORE pq:delayed -inf <now>   -> ["3:<job_id>"]
    ZADD pq:queue <3*1e13 + now> <job_id>
    ZREM pq:delayed "3:<job_id>"
    (all inside one atomic Lua script)

attempt 2: BZPOPMIN picks it up
  status is FAILED -> passes the guard (QUEUED or FAILED)
  attempts=2, RUNNING, handler succeeds
  update_status(SUCCESS)
```

If attempt 3 also failed: `should_dead_letter?` `3 >= 3` → yes →
`DEAD_LETTER`, `LPUSH pq:dlq`, publish. Exactly three executions.

### 13.4 A worker dies mid-job

```
t=0     BZPOPMIN pops job -> it is now GONE from Redis
t=0.1   attempts=1, status=RUNNING, updated_at=t
t=0.7   SIGKILL. No handler runs. Nothing is written.

        STATE: row says RUNNING. Redis has nothing. Nothing will pick it up.

t=60+   Some worker's recovery timer fires
        acquire_lock("recovery")            SET pq:lock:recovery <id> NX EX 15
        -> won the lock
        list_stuck_running(cutoff = now - 60s)
            SELECT * FROM jobs
             WHERE status='running' AND updated_at < cutoff
        -> [our job]
        attempts(1) >= max_attempts(3)?  no
        update_status(QUEUED, result={"error":"Worker lost...", ...})
        enqueue(job_id, priority)           ZADD pq:queue ...
        publish_update("queued")
        release_lock("recovery")            owner-checked DEL

t=65    A worker picks it up. attempts=2. Succeeds.
```

**This was verified by actually doing it** — `docker compose kill -s SIGKILL
worker`, confirming `status=running` with `ZCARD pq:queue == 0`, then watching
the reclaim at t+65s:

```
worker-2 | WARNING Recovering job 2b23bf60… stuck in RUNNING since 21:41:23 — re-queueing
worker-2 | INFO    Recovery sweep reclaimed 1 job(s)
```

### 13.5 The dashboard

```
Browser opens /dashboard/
  -> StaticFiles serves dashboard/index.html
  JS: fetch('/jobs/stats')             -> initial counters
  JS: new WebSocket('ws://host/jobs/stream')
       -> stream_job_updates()
          websocket.accept()
          aioredis SUBSCRIBE pq:updates

Any worker publishes on pq:updates
  -> Redis fans out to every subscriber
  -> get_message() returns it
  -> websocket.send_text(raw JSON)
  -> browser appends to the feed, schedules a debounced /jobs/stats refresh
```

---

## 14. The test suite

### 14.1 The split, and why it matters

```bash
pytest tests/                        # 97 tests (needs Postgres)
pytest tests/ -m "not integration"   # 71 tests, nothing running
```

**71 of 97 tests need no database, no Redis and no worker.** That is not a
stylistic achievement — it is the *measurable return* on the layered
architecture, and it is the honest answer to "isn't this over-engineered for a
solo project?"

It is only possible because no service constructs its own dependencies. Had
`QueueService` done `self._r = get_redis_client()`, all 22 of its tests would
need a live Redis.

### 14.2 `tests/conftest.py`

**`_database_available()`** attempts `SELECT 1`. Integration tests **skip** when
Postgres is unreachable, so a fresh clone runs `pytest` and gets green.

**But CI must not silently skip:**

```python
if os.getenv("REQUIRE_INTEGRATION_TESTS") == "1":
    pytest.fail(message)
pytest.skip(message, allow_module_level=True)
```

The CI workflow sets that variable, so a skip becomes a failure. Without this,
a broken database configuration in CI produces a green checkmark.

**The `fake_redis` fixture:**

```python
mock.register_script.return_value = MagicMock(return_value=0)
```

`QueueService` calls `register_script` at construction and then *calls what it
returns*, so the return value must itself be callable. A plain `MagicMock()`
would work by accident; being explicit documents the contract.

**The `client` fixture** uses `with TestClient(app)` — as a context manager,
because **that is what runs the lifespan.** Plain `TestClient(app)` with direct
`.get()` calls does not, so `init_db()` never fires and the table would not
exist. This is a real trap in the original spec's test file.

### 14.3 What each test file locks down

**`test_queue_service.py` (22)** — score arithmetic (priority dominates age,
equal priority is FIFO), `BZPOPMIN` tuple unwrapping, delayed-set member
encoding, DLQ operations, pub/sub wire format, lock semantics
(`SET NX EX`, owner-checked release).

**`test_retry_service.py` (13)** — backoff doubling, configured base honoured,
`>=` threshold, first retry uses the base delay (the off-by-one), delayed set
used rather than the ready queue, permanent failures skip retries, and
`test_handle_failure_never_sleeps` which patches `time.sleep` to raise.

**`test_job_service.py` (14)** — persist-then-enqueue ordering, server-side
default `max_attempts`, idempotent hit does **not** re-enqueue, the concurrent
race resolution, `reset_attempts` goes through the repository, and a
parametrised check that all four non-DEAD_LETTER states are rejected for manual
retry.

**`test_recovery_service.py` (15)** — abandoned jobs re-queued, poison pills
dead-lettered, cutoff computed from the configured timeout, and critically
`test_backlogged_job_still_in_redis_is_left_alone` — the test that stops the
orphan sweep from duplicating a slow queue.

**`test_registry.py` (7)** — registration and lookup, duplicate rejection, the
error message naming available handlers, and that importing
`handlers.builtin` is what populates the registry.

**`test_api.py` (26, integration)** — the full HTTP surface against real
Postgres. Includes `test_idempotent_resubmission_does_not_re_enqueue`, because
FR6 is about *execution*, not just about the response body — returning the same
id while quietly enqueueing a second copy would still run the work twice.

And the architectural guard:

```python
def test_routers_contain_no_sql_or_redis_calls():
    router_src = (repo_root / "app/api/routers/jobs.py").read_text()
    for forbidden in ("session.query", "self._db", "SELECT ", "zadd", "lpush"):
        assert forbidden not in router_src
```

The Maintainability NFR says a layering violation is build-blocking. A human
reviewer stops checking around week two; this does not.

---

## 15. Packaging — Docker and CI

### 15.1 The Dockerfiles

```dockerfile
FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt .              # <- before the source
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN useradd --create-home --uid 1000 pulse && chown -R pulse:pulse /app
USER pulse
```

- **`PYTHONUNBUFFERED=1`** — stdout reaches `docker logs` immediately instead of
  sitting in a buffer. Matters enormously when what you are debugging is why a
  container appears to hang.
- **`COPY requirements.txt` before `COPY . .`** — Docker caches layers by
  content. Editing a Python file re-runs only the final `COPY`, not the whole
  `pip install`. Reversed, every code change reinstalls every dependency.
- **Non-root user, added after the install** — a process that does not need root
  should not have it, and adding the user last keeps the pip layer cached.
- **`CMD` in exec form** — see §10.2. This one is a silent-failure trap.

Two images rather than one with different commands, because they are separate
deployables with separate scaling characteristics: workers scale with queue
depth, the API with request rate.

### 15.2 `docker-compose.yml`

**Healthcheck gating:**

```yaml
depends_on:
  postgres: { condition: service_healthy }
  redis:    { condition: service_healthy }
  api:      { condition: service_healthy }
```

`depends_on` alone only waits for the container to *start*, not to be *ready*.
With `condition: service_healthy`, dependents wait for the healthcheck.

**Why workers wait on the API specifically.** The API owns schema creation. Four
processes racing to call `create_all()` is a real conflict — this turns the race
into an ordering.

**Port overrides:**

```yaml
ports:
  - "${POSTGRES_PORT:-5432}:5432"
```

Committed defaults are the documented ones, so a fresh clone works. A machine
that already has something on 5432 sets `POSTGRES_PORT` in a local `.env`.
Container-to-container addressing is unaffected — the API still reaches
`postgres:5432`. Port collisions are the most common reason a stranger's
`docker compose up` fails.

**`deploy.replicas: 3`** — three independent worker processes competing for the
same queue. This is the concurrency claim, physically instantiated.

No top-level `version:` key — obsolete since Compose v2 and only produces a
warning.

### 15.3 `.github/workflows/ci.yml`

**Two jobs.**

`test` — spins up Postgres 15 and Redis 7 as service containers, installs on
Python 3.11 (matching Docker and local), runs `pytest` with
`REQUIRE_INTEGRATION_TESTS=1`.

`build` — the more valuable one. A unit-test-only pipeline goes green while the
compose file is broken. This job:

```yaml
- run: docker compose build
- run: docker compose up -d --wait --wait-timeout 180
- run: |
    JOB_ID=$(curl -sf -X POST .../jobs -d '{"job_type":"resize_image",...}' | ...)
    for _ in $(seq 1 30); do
      STATUS=$(curl -sf .../jobs/$JOB_ID | ...)
      [ "$STATUS" = "success" ] && exit 0
      sleep 2
    done
    exit 1
```

It executes the README's Quick Start on every push. `resize_image` never fails,
so it *must* reach SUCCESS. Polling rather than sleeping keeps the job fast when
it is fast. `docker compose logs` on failure, teardown always.

> **Status: written, never run.** There is no GitHub remote yet. Treat the green
> checkmark as unverified until it exists.

---

## 16. Load testing

### 16.1 `locustfile.py` — two user classes

**`JobSubmitter`** (weight 4) — measures API throughput. Posts jobs at random
priorities and occasionally reads `/jobs/stats`.

**`JobLifecycleUser`** (weight 1) — measures *end-to-end completion* latency.
Submits, then polls until terminal, then reports through a custom Locust event:

```python
events.request.fire(
    request_type="JOB", name=f"completion ({status})",
    response_time=elapsed_ms, ...)
```

**Why both.** Reporting only submit throughput would be misleading: accepting
500 requests/sec means nothing if the workers are hours behind.

It uses `resize_image`, not `send_email`, deliberately — `send_email` fails 20%
of the time by design, and a job spending 2–8s in backoff would swamp the
latency distribution with the failure-injection rate rather than the system's
processing time.

### 16.2 The results, and how to read them

**API submit throughput** — Locust, 50 users, 120s:

| Endpoint | Requests | Failures | RPS | P50 | P95 | P99 |
|---|---|---|---|---|---|---|
| `POST /jobs` | 9,742 | 0 | 81.4/s | 62ms | 100ms | 140ms |
| `GET /jobs/stats` | 3,124 | 0 | 26.1/s | 13ms | 38ms | 61ms |
| `GET /jobs/{id}` | 2,144 | 0 | 17.9/s | 16ms | 50ms | 84ms |
| **Aggregate** | **15,042** | **0** | **125.7/s** | 55ms | 95ms | 130ms |

**Completion throughput:**

| Scenario | Result |
|---|---|
| 3 workers, `benchmark_noop` | **165 jobs/sec** sustained; peak 5s window ~220/s |
| 3 workers, `send_email` (0.5s + 20% failure) | ~4.5 jobs/sec |

### 16.3 Why `benchmark_noop` was added

**The first load test measured `time.sleep`, not PulseQueue.** Three workers
running a 0.5s handler cannot exceed `3 ÷ 0.5 = 6` jobs/sec regardless of how
fast the queue is. Reporting "4.5 jobs/sec" would have been a true statement
about the demo handler and a meaningless one about the system.

`benchmark_noop` returns immediately, so the measured figure reflects what
PulseQueue actually costs per job: one atomic dequeue, three row updates, one
publish. That moved the number from 4.5/s to **165/s**.

**The general lesson: before quoting a number, work out what would have to
change for it to move.** If the answer is "a `sleep` constant", it is not
measuring the system.

### 16.4 The finding that looked like a bug

Every `JobLifecycleUser` probe timed out. Initially read as a defect. It is the
architecture working as designed:

The API accepted **81 jobs/sec** against a fleet completing **~4.5/s**, building
a **9,000-job backlog** — which it absorbed with **zero errors** and drained
steadily.

That 20× gap is precisely why the work is not on the request path, and precisely
why workers scale independently of the API. The right response was to report
both numbers and explain the gap, not to tune the test until it looked clean.

---

## 17. Bugs found in the original spec, and fixed

Every one of these was a real defect, verified by reproducing it.

| # | Original code | Consequence |
|---|---|---|
| 1 | `manual_retry` did `job.attempts = 0` after the commit | Never persisted — the reset silently did nothing |
| 2 | `compute_delay(job.attempts)` | Off-by-one: first backoff 4s, contradicting the documented 2s |
| 3 | `except: retry.handle_failure(job, ...)` with no `job = None` | `UnboundLocalError` masking the real exception if the job load threw |
| 4 | Idempotency pre-check only | 9 of 10 concurrent duplicate POSTs returned HTTP 500 |
| 5 | Tiebreaker `uuid.int % 1e6 / 1e9` | Docstring claimed FIFO; behaviour was random |
| 6 | `Base.metadata.create_all()` at module scope | Whole test suite needed a live DB just to *collect* |
| 7 | `client = TestClient(app)` at module level | Lifespan never runs → tables never created |
| 8 | `SAEnum(JobStatus)` with no `values_callable` | Postgres stored `'QUEUED'` while the API said `'queued'` |
| 9 | `manual_retry` left the job on the DLQ list | DLQ depth stops meaning "needs attention" |
| 10 | Dashboard hardcoded `ws://` | Blocked as mixed content once deployed behind HTTPS |
| 11 | `@router.post("/")` under `prefix="/jobs"` | `POST /jobs` gets a 307 redirect |
| 12 | `MAX_RETRY_ATTEMPTS` setting | Dead configuration — never read anywhere |
| 13 | `datetime.utcnow()` | Deprecated, and naive/aware mixing is a latent `TypeError` in the recovery path |

---

## 18. Deviations from the spec

### 18.1 Approved architectural changes

| Change | Why | Verified |
|---|---|---|
| Delayed set instead of `time.sleep` | A sleeping worker is an idle worker | 5 jobs backing off while `success` rose 30→40 |
| `RecoveryService` | The PRD claimed crash safety the code did not deliver | `SIGKILL` mid-job → reclaimed, re-ran, succeeded |
| `BZPOPMIN` instead of poll | ~25ms vs ~500ms pickup latency | Measured |
| Python 3.11 locally | Pinned deps have no 3.14 wheels | Installed |
| Orphan sweep | `COMMIT`-then-`ENQUEUE` has no transaction | 2 real stuck rows detected and re-enqueued |

### 18.2 Files added beyond the spec's structure

| File | Why |
|---|---|
| `app/core/clock.py` | Naive `utcnow()` would make the recovery datetime comparison a latent crash |
| `app/core/exceptions.py` | So the service layer imports neither SQLAlchemy nor FastAPI |
| `app/services/recovery_service.py` | Crash recovery (approved) |
| `tests/conftest.py` | Shared fixtures, skip-vs-fail logic |
| `tests/test_job_service.py` | Idempotency logic had no unit tests |
| `tests/test_recovery_service.py` | New service needs tests |
| `pytest.ini` | Marker registration, `--strict-markers` |
| `.dockerignore` | Without it the venv is uploaded on every build |

### 18.3 Smaller improvements

- Composite indexes serving the recovery sweep and status filtering
- `benchmark_noop` handler so throughput measures the system, not `sleep`
- Overridable compose host ports
- API healthcheck, so workers can wait for schema creation
- `stop_grace_period: 30s`, so graceful shutdown has time to finish
- Non-root container user
- CI `build` job that submits a real job end-to-end
- Duplicate-registration guard in the registry
- Dashboard: debounced stats, XSS-safe rendering, backoff reconnect
- Domain-error → 409 mapping (rather than 400) for illegal state transitions

### 18.4 Kept exactly as specified

- The layered architecture and every file's responsibility
- The Redis key names (`pq:queue`, `pq:dlq`, `pq:updates`)
- The pub/sub message shape `{"job_id", "status"}` — the spec's test asserts it byte-for-byte
- `compute_delay(0) == 2`, `(1) == 4`, `(2) == 8`
- All five statuses and the state machine
- Every endpoint path and method
- The three built-in handlers and their timings
- `create_all()` rather than Alembic
- All pinned dependency versions
- `POST /jobs` returning 201 even on an idempotent hit
