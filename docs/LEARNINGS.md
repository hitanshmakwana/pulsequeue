# Learnings

Retrospective notes per milestone. What worked, what broke, what is still owed.
Written as they happened, so the record is useful rather than tidy.

---

## M1 — Core submit-and-process loop

### What worked

**Building bottom-up.** Repository, then service, then router. Each layer had
something real to depend on when it was written, so nothing was stubbed and
nothing needed rework once the layer below existed.

**Injecting dependencies rather than importing singletons.** `QueueService`
takes a Redis client; `RetryService` takes a repository and a queue. This felt
like ceremony for the first hour and then paid for itself permanently — 71 of
97 tests need no infrastructure, and that is a direct consequence of no service
constructing its own collaborators. Had `QueueService` done
`self._r = get_redis_client()`, every one of its tests would need a live Redis.

**Moving `create_all()` into the FastAPI lifespan.** Originally it ran at module
import. That meant importing `app.main` — which every test does — required a
live database, so the entire suite needed docker-compose just to *collect*.
Import-time side effects that reach the network are worth hunting down.

### What broke

**Enum storage.** SQLAlchemy's `Enum` stores member *names* by default, so
`JobStatus.QUEUED` became `'QUEUED'` in Postgres while every API response said
`'queued'`. Everything worked — the ORM translated both ways — but a `psql`
session showed something different from the API, which is a miserable thing to
debug at 2am. Fixed with `values_callable`.

**Route ordering.** `/jobs/stats` has to be declared before `/jobs/{job_id}`,
because FastAPI matches in declaration order. `job_id: uuid.UUID` would have
rejected `"stats"` with a 422 anyway, but relying on a validation failure to
route correctly is a trap for whoever later loosens that type to `str`.

**Trailing slashes.** `@router.post("/")` under `prefix="/jobs"` produces
`/jobs/`, so `curl -X POST /jobs` gets a 307 redirect. Using `""` gives the
documented path directly.

### Technical debt taken on

- `POST /jobs` returns 201 even on an idempotent hit, where 200 would be more
  accurate. Left as-is because the spec's checkpoint expects 201 and the
  response body is identical either way.

### Interview questions this earns

- *"Why is a repository worth it for one table?"* — Not because the abstraction
  pays for itself at one table. Because it is what makes 71 of the 97 tests in
  the suite infrastructure-free. That is a number, not an opinion.
- *"How do you test business logic that touches a database?"* — You arrange for
  it not to. The service holds a repository interface; the test passes a mock.

---

## M2 — Reliability and extensibility

### What worked

**Writing the failing test first for the idempotency race.** Ten concurrent
POSTs with one key returned nine 500s before the `IntegrityError` handling
existed. Reasoning about the race in the abstract had already produced the
conclusion "the pre-check is enough" — which was wrong. Running it settled it in
30 seconds.

**Deleting `time.sleep` from the retry path.** Watching the delayed set under
load made the cost concrete: five jobs backing off simultaneously while
`success` climbed from 30 to 40. With `time.sleep`, all three workers would have
been frozen through that window. This is now the clearest example I have of an
implementation detail dominating a benchmark.

**Lua for the promotion step.** The read-then-move had to be atomic, and
reaching for a script rather than a lock was the right instinct — Redis already
serialises script execution, so there is no lock to acquire, hold or leak.

### What broke

**The crash-recovery gap, which the original design did not have.** Atomic
dequeue removes the job from Redis instantly. A `SIGKILL`ed worker therefore
leaves the job in `RUNNING` with nothing anywhere pointing at it. Graceful
`SIGTERM` handling — which was implemented and works — is irrelevant here,
because a killed process runs no handler.

This is the most instructive thing in the project: **a correct mechanism
created a new failure mode.** Atomicity is what makes multi-worker safety true,
and it is also what makes crash recovery necessary. Those are the same property
viewed from two sides.

**And then a second orphan path, found by accident.** After a test run, two jobs
sat `QUEUED` forever while `ZCARD pq:queue` was 0. Cause: `submit()` commits the
row and then enqueues, with no transaction spanning Postgres and Redis. The
integration tests mock Redis, so the enqueue never landed — but the same window
exists in production if the API dies between the two statements.

The lesson is about noticing. The stuck rows were visible in the stats output
for several test runs and read as "leftover test data" until the numbers were
actually reconciled against Redis. **Two systems, no shared transaction, means
a window — and windows need a sweep, not a comment saying "future cron task".**

**The membership check nearly got skipped.** The obvious orphan sweep is "any
old QUEUED row, re-enqueue it". That duplicates every job in a queue that is
merely backlogged, because a backlog and an orphan look identical from the
database. Only Redis distinguishes them.

### Technical debt taken on

- Each dashboard WebSocket client opens its own Redis connection. Fine for an
  ops view, wrong for a public page with many viewers.
- A job stays `FAILED` while waiting in the delayed set rather than flipping
  back to `QUEUED`. Defensible — "failed, retrying at X" carries more
  information — but it means the `queued` stat undercounts pending work.

### Interview questions this earns

- *"What happens if a worker is killed mid-job?"* — The good version of this
  answer requires having built the recovery, not just describing it.
- *"How do you retry without blocking a worker?"*
- *"Your dequeue is atomic. What does that break?"* — The best question in the
  project, and the one M2 is really about.

---

## M3 — Packaging and measurement

### What worked

**Making host ports overridable in compose.** The dev machine already had
Redis on 6379 and something on 8000. `${REDIS_PORT:-6379}` keeps a fresh clone
working on the documented ports while letting a busy machine remap without
editing a tracked file. Port collisions are the most common reason a stranger's
`docker compose up` fails.

**Gating workers behind the API's healthcheck.** Four processes racing to call
`create_all()` is a real conflict. Making the API the one that owns schema
creation, and having workers wait for it to report healthy, turns a race into
an ordering.

**Adding a build job to CI that actually submits a job.** A unit-test-only
pipeline goes green while the compose file is broken. The build job runs
`docker compose up --wait`, submits a job and polls until it succeeds — which
is the README's Quick Start, executed on every push.

**`exec` form in the Dockerfile `CMD`.** Shell form runs the process under
`/bin/sh`, which does not forward `SIGTERM` to its child. Graceful shutdown
would have silently never fired, and every deploy would have abandoned an
in-flight job — a bug that produces no error message anywhere.

### What broke

**The first load test measured `time.sleep`.** Three workers running a 0.5s
handler cannot exceed 6 jobs/sec regardless of how fast the queue is. Reporting
"4.5 jobs/sec" would have been a true statement about the demo handler and a
meaningless one about PulseQueue. Adding a no-op handler moved the measured
figure to **165 jobs/sec sustained** — the difference between benchmarking the
system and benchmarking the fixture.

The general lesson: **before quoting a number, work out what would have to
change for it to move.** If the answer is "a `sleep` constant", it is not
measuring the system.

**All lifecycle probes timed out, and that was the finding.** The API accepted
81 jobs/sec against a fleet completing ~4.5/s, building a 9,000-job backlog.
Initially read as a bug. It is the architecture working: the queue absorbed the
burst with zero errors and drained steadily. The right response was to report
both numbers and explain the gap, not to tune the test until it looked clean.

### Technical debt taken on

- Benchmarks are from the local Docker stack, competing with the workers for
  the same CPU. They need re-running against the deployed instance before being
  quoted anywhere permanent.

### Interview questions this earns

- *"What's your throughput?"* — The answer that lands is the one that
  distinguishes submit throughput from completion throughput and explains why
  they differ by 20x.
- *"Why is your P95 completion latency worse than your P95 submit latency?"*

---

## Still owed

| Item | Why it matters |
|---|---|
| Re-run benchmarks against the deployed instance | Local numbers share a CPU with the workers |
| Architecture diagram as an image | The ASCII diagrams are fine in a terminal, weak in a browser |
| Kill a worker *during* a load run, not in isolation | Crash recovery is verified standalone; under sustained load is a stronger claim |

---

## The one-sentence version

Three things in this project are worth more than the rest combined: **the
layered architecture, because it is why most of the tests need nothing running;
the delayed-set backoff, because it is where an implementation detail was
quietly dominating a benchmark; and the visibility timeout, because it is where
a correct mechanism turned out to have created the failure mode it was
protecting against.**
