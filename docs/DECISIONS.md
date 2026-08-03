# Engineering Decision Log

Real decisions made while building PulseQueue, in the order they came up. Each
one records what the alternatives were and what the choice cost, because a
decision without a stated cost is usually not a decision — it is a preference
that has not been examined.

---

## D1 — A repository layer, separate from services

**Problem.** Something has to own SQL. If routers query the database directly,
HTTP concerns and data access end up in the same function, and changing a query
means reading route handlers to find it.

**Options.**
1. Query from routers. Fewest files.
2. Query from services. One layer of separation, no extra class.
3. A dedicated repository that services call by name.

**Chosen.** Option 3. The rule is mechanical: if it is a SQL query it belongs in
`JobRepository`; if it is a rule about jobs it belongs in a service.

**Reason.** The rule is mechanical, which matters more than it sounds. "Put
things in sensible places" erodes under deadline pressure; "no `self._db`
outside `job_repository.py`" is checkable, and is in fact checked by a test.
It also gives one place to translate `IntegrityError` into a domain exception,
so nothing above the repository imports SQLAlchemy.

**Tradeoff.** One more file and a layer of indirection for a single-table
schema. The honest counter to "isn't this over-engineered for a solo project?"
is not that the abstraction pays for itself in one table — it is that it is
what makes 71 of 97 tests runnable with no database. That is a measurable
return, not a stylistic one.

**Future.** A second entity would want a generic base repository. With one, that
would be abstraction for its own sake.

---

## D2 — A decorator-based plugin registry, not a handler dict

**Problem.** The worker needs to map a `job_type` string to a function. A
hardcoded `HANDLERS = {"send_email": send_email}` means every new job type
requires editing worker internals — the module least safe to touch, because a
mistake there stops every job type at once.

**Options.**
1. Module-level dict, documented as the place to add entries.
2. Config-file-driven registration with dotted-path imports.
3. `@job_handler("name")` decorator populating a registry at import time.

**Chosen.** Option 3.

**Reason.** Open/Closed: the worker is open for extension, closed for
modification. Registration is co-located with the function it registers, so
there is no second list to forget to update. Option 2 moves the coupling into a
config file and adds a string-to-import indirection that fails at runtime
rather than import time. The decorator is also the pattern Celery uses for
`@task`, Flask for `@route` and pytest for fixtures — being able to name the
precedent matters, because it reframes the choice from "something I invented"
to "the standard solution."

**Tradeoff.** Registration is an import side effect. If nothing imports
`handlers.builtin`, the registry is silently empty. Mitigated two ways: the
worker imports it explicitly at startup and logs what it found, and
`get_handler` raises with the list of what *is* registered, so a typo names
itself.

**Future.** Entry-point discovery so handlers can ship in a separate package
without the worker importing them by name.

---

## D3 — RetryService owns retry policy, not the worker

**Problem.** Backoff calculation, the attempt-count check and the dead-letter
decision have to live somewhere. Inline in the worker is the obvious place.

**Options.**
1. Inline in the worker's `except` block.
2. A `RetryService` the worker delegates to.

**Chosen.** Option 2.

**Reason.** Inline, the worker has two jobs — execute work and decide policy —
and testing the backoff maths requires booting a worker, a Redis and a
Postgres. Extracted, `compute_delay` and `should_dead_letter` are pure
functions and `handle_failure` needs two mocks. `tests/test_retry_service.py`
is 13 tests that run in milliseconds with nothing installed.

The heuristic that produced this: **if you can test it separately, it should be
a separate component.**

**Tradeoff.** One more class, and a reader has to follow one more hop to see
what happens on failure. Worth it — the worker's `except` block is now three
lines and contains no arithmetic.

**Future.** Per-job-type retry policy (payment jobs might want ten attempts and
a longer base delay). The service is already the right seam for it.

---

## D4 — At-least-once delivery, not exactly-once

**Problem.** A worker can finish its work and die before committing the result.
On recovery the job runs again. Should the system prevent that?

**Options.**
1. Claim exactly-once and hope nobody probes it.
2. Attempt exactly-once with a distributed transaction across Postgres and
   Redis, or a two-phase commit.
3. Target at-least-once, document it, and require idempotent handlers.

**Chosen.** Option 3.

**Reason.** Exactly-once across two systems without a shared transaction log is
one of the genuinely hard problems in distributed systems. What is usually sold
as exactly-once is at-least-once delivery plus idempotent processing — which is
exactly what this is, stated accurately. Option 1 is a credibility risk: an
interviewer who asks "what happens if the worker dies after the side effect but
before the commit?" gets an answer that contradicts the claim.

**Tradeoff.** Handlers must tolerate re-execution. `send_email` could send twice.
That constraint is documented in the handler contract rather than hidden.

**Future.** A transactional outbox, plus a handler-level dedupe key stored in
the same transaction as the side effect. That is the real solution, and it is
correctly scoped as future work rather than pretended at.

---

## D5 — A delayed sorted set for backoff, not `time.sleep`

**Problem.** A failed job must wait before its next attempt. The direct
implementation is `time.sleep(delay)` in the worker.

**Options.**
1. `time.sleep(delay)` inline.
2. `pq:delayed`, a sorted set scored by ready-at timestamp, promoted into the
   ready queue by workers at the top of their loop.

**Chosen.** Option 2.

**Reason.** Option 1 idles an entire worker process for the whole backoff. With
three workers, a 20% failure rate and delays of 2/4/8s, a large fraction of the
fleet ends up asleep while the queue grows behind it — and the throughput
number that goes on a resume is depressed by an implementation detail that has
nothing to do with the work.

Observed under load: five jobs backing off simultaneously while `success` rose
from 30 to 40 in the same window. Under option 1, all three workers would have
been blocked for that period.

Promotion runs as a Lua script so the read (which jobs are due) and the write
(move them) are one atomic step. Split into separate `ZRANGEBYSCORE` + `ZADD` +
`ZREM` calls, two workers could both observe the same due job and enqueue it
twice.

This is Sidekiq's scheduled set and BullMQ's delayed set.

**Tradeoff.** A second Redis key, a Lua script, and a promotion step in the
worker loop. A due retry can also wait up to one `DEQUEUE_TIMEOUT` (1s) before
promotion. Acceptable against a backoff measured in seconds.

**Note.** The delayed set encodes members as `"<priority>:<job_id>"` — the set
is ordered by time, so priority has nowhere else to live until promotion.

**Future.** The same primitive supports `run_at` scheduling almost unchanged.

---

## D6 — BZPOPMIN instead of ZPOPMIN plus a poll loop

**Problem.** A worker with an empty queue must not spin.

**Options.**
1. `ZPOPMIN`, then `time.sleep(1)` when empty.
2. `BZPOPMIN` with a 1s timeout.

**Chosen.** Option 2.

**Reason.** Identical atomicity — still a single atomic pop, so the
"no double-processing across N workers" guarantee is unchanged — but the worker
wakes the instant a job arrives instead of up to a poll interval later.
Measured pickup latency on an idle queue is ~25ms against an expected ~500ms
average for option 1. On a metric being quoted as P95 end-to-end latency, that
is not a rounding error.

The 1s timeout is not a poll interval. It is the upper bound on how long
shutdown and the recovery timer wait, since a job arriving mid-block returns
immediately.

**Tradeoff.** Each idle worker holds a blocked connection. Irrelevant at this
scale; would matter at hundreds of workers against one Redis.

`dequeue_nowait()` is kept alongside it — the non-blocking form is what unit
tests use.

---

## D7 — A visibility timeout, because atomic dequeue requires one

**Problem.** `BZPOPMIN` removes a job from Redis the moment a worker takes it.
That atomicity is what prevents double-processing. It also means a worker
`SIGKILL`ed mid-job leaves the job nowhere but as a row stuck in `RUNNING`,
which nothing will ever pick up.

Graceful `SIGTERM` handling does not help. A process that stops executing
instructions runs no handler.

**Options.**
1. Ship without it and soften the reliability claim.
2. A second Redis structure tracking in-flight jobs, cleaned up on completion.
3. A visibility timeout: sweep Postgres for jobs in `RUNNING` past a deadline
   and re-queue them.

**Chosen.** Option 3.

**Reason.** Option 1 leaves the PRD's "no job silently lost on worker crash"
unbacked, and the load test's kill-a-worker step would simply wedge jobs.
Option 2 duplicates state that Postgres already holds — `status` and
`updated_at` are exactly the in-flight record, and a separate structure would
need its own crash-recovery story. Option 3 uses the source of truth directly.
It is the primitive SQS exposes.

Every worker runs the sweep on a timer; a `SET NX EX` lock means only one
actually scans. The lock TTL is the deadlock guard — a worker that dies holding
it releases it on expiry.

**Verified rather than assumed:** `SIGKILL` mid-job, confirmed the job was
orphaned in `RUNNING` with `ZCARD pq:queue == 0`, restarted the fleet, watched
it be reclaimed and complete with `attempts=2`.

**Tradeoff.** `VISIBILITY_TIMEOUT` must exceed the slowest legitimate handler.
Set too low, a healthy long-running job is reclaimed and duplicated — which
*is* the at-least-once tradeoff (D4), now explicit and tunable rather than
implicit.

---

## D8 — Increment `attempts` before running, not after

**Problem.** Where in the execution path should the attempt counter move?

**Chosen.** Before the handler runs.

**Reason.** This is what makes the visibility timeout (D7) safe. If the counter
moved *after* execution, a job that reliably crashes its worker would never
record an attempt — so `RecoveryService` would re-queue it, the next worker would
also die, and the cycle would continue forever, taking down the fleet one worker
at a time. Incrementing first means a job that kills three workers has consumed
three attempts and dead-letters. That is the poison-pill guard.

The counter therefore reads as "executions **started**", which is precisely the
number the dead-letter threshold wants.

**Tradeoff.** A job that fails for an infrastructure reason (Postgres briefly
unreachable) also consumes an attempt. Correct: from the system's position
those are indistinguishable, and the safe assumption is that an attempt
happened.

---

## D9 — Idempotency enforced twice, on purpose

**Problem.** `submit()` checks whether the key exists, then inserts. That is
check-then-act, and it is racy: two concurrent requests with the same key can
both read "no existing job".

**Options.**
1. Pre-check only. Fails under concurrency.
2. Unique constraint only, let violations surface. Every ordinary duplicate
   becomes an error the client has to interpret.
3. Both — pre-check as the fast path, constraint violation caught and resolved.

**Chosen.** Option 3.

**Reason.** The pre-check handles the common case (a client retrying seconds
later) without a doomed INSERT. The constraint handles the case only the
database can adjudicate. The loser of the race re-reads the winner's row and
returns it, so every caller gets the same job.

**Measured:** 10 concurrent POSTs with one key → 1 distinct id, 0 errors.
Without the constraint handling, nine of those are `500`s.

**Tradeoff.** Two code paths for one rule. Both are tested, and the second is
unreachable in single-threaded use — which is precisely why it needed an
explicit concurrency test rather than eyeballing.

---

## D10 — `create_all()` in v1, Alembic deferred

**Problem.** Something must create the schema.

**Chosen.** `Base.metadata.create_all()` on startup.

**Reason.** One table. Alembic would add a migrations directory, a revision
chain and an `alembic upgrade head` step to every deployment, to manage a
schema that fits on a screen. Migration tooling earns its cost when a schema
changes under a team; neither is true yet.

**Tradeoff.** No column can be altered without manual intervention, and no
schema history exists. This is a real limitation, listed in Future
Improvements, not an oversight.

**Note.** `create_all` is wrapped in a retry loop — application containers can
beat Postgres to readiness even behind a healthcheck, and crash-looping on
startup is a worse failure mode than waiting.

---

## D11 — Domain exceptions instead of `ValueError` or `HTTPException`

**Problem.** `manual_retry` on a non-dead-lettered job must fail. How does that
reach the client as a 409?

**Options.**
1. Raise `HTTPException` from the service.
2. Raise `ValueError`, catch it in the router.
3. Domain exceptions in `core/exceptions.py`, mapped to status codes by the
   router.

**Chosen.** Option 3.

**Reason.** Option 1 makes `JobService` unusable from the worker, which speaks
no HTTP — a dependency pointing the wrong way. Option 2 works but is
imprecise: `ValueError` is also what a bad `int()` raises, so the router cannot
distinguish "illegal transition" from "a bug", and both become a 400.
`InvalidStateTransition` and `JobNotFound` name what happened, map cleanly to
409 and 404, and let the worker catch exactly what it means to handle.

The same reasoning applies downward: `JobRepository` translates
`IntegrityError` into `DuplicateIdempotencyKey` so nothing above it imports
SQLAlchemy.

**Tradeoff.** One more small module. It is the thing that keeps the dependency
direction honest in both directions.

---

## D12 — Unregistered `job_type` dead-letters immediately

**Problem.** A job arrives naming a handler that does not exist.

**Chosen.** Dead-letter on the first attempt, via a `permanent=True` flag on
`handle_failure`.

**Reason.** No amount of backoff conjures a handler into existence. Retrying
three times delays the real error by 14 seconds and buries it under retry
noise. The distinction — retryable versus permanent failure — is a policy
decision, so it lives in `RetryService` rather than as a special case in the
worker.

**Tradeoff.** A job type could legitimately be *about* to be deployed, and a
brief window of unknown-type failures during a rolling deploy will now
dead-letter rather than ride out the rollout. `POST /jobs/{id}/retry` exists for
exactly that recovery, and the alternative — silently absorbing typos for 14
seconds — is worse day to day.

---

## D13 — Timezone-aware UTC everywhere

**Problem.** `datetime.utcnow()` is deprecated in Python 3.12 and returns a
*naive* datetime that merely happens to hold UTC.

**Chosen.** `datetime.now(timezone.utc)` throughout, via `app/core/clock.py`,
with `DateTime(timezone=True)` columns.

**Reason.** Not just the deprecation. Comparing a naive datetime to an aware one
raises `TypeError` — and `RecoveryService` compares `updated_at` against a
computed cutoff. With naive timestamps that comparison is a latent crash in the
one code path that only runs after something else has already gone wrong,
which is the worst possible place for it.

Centralising in `clock.py` also gives tests one seam to control time through.

---

## D14 — Orphan sweep for lost enqueues

**Problem.** `JobService.submit` commits the row and *then* enqueues the id.
There is no transaction spanning Postgres and Redis. A process death between
those two statements — or a Redis flush, or a failover to a lagging replica —
leaves a row in QUEUED that is in no queue. The row is not lost, but it is
inert, which from the caller's point of view is the same thing.

This was **found empirically, not theorised.** After a test run, two jobs sat
QUEUED forever while `ZCARD pq:queue` was 0.

**Options.**
1. Leave it. Both the original spec's comment and mine described it as "a future
   cron task".
2. Eliminate the window with a transactional outbox — write the enqueue intent
   into the same Postgres transaction as the row, and have a relay drain it.
3. Notice and repair: sweep for stale pending rows absent from Redis, and
   re-enqueue them.

**Chosen.** Option 3, reusing the sweep and lock already built for D7.

**Reason.** Option 1 leaves an acknowledged TODO in the reliability story of the
flagship project. Option 2 is the *correct* fix and is genuinely more work — an
outbox table, a relay process, and its own failure modes — so it is listed as
future work rather than half-built. Option 3 costs ~20 lines against
infrastructure that already exists.

**The safety argument is the whole decision.** A genuinely orphaned job and a job
sitting in a deep backlog are **indistinguishable from the database** — both are
old rows in QUEUED that nothing has touched recently. Re-enqueueing on age alone
would duplicate every job in a queue that merely got behind. So
`QueueService.contains` checks `ZSCORE` on both the ready queue *and* the delayed
set before anything is re-enqueued, and `test_backlogged_job_still_in_redis_is_left_alone`
locks that in.

**Tradeoff.** Two Redis round-trips per candidate row per sweep. Bounded by the
`limit` parameter and gated behind the visibility timeout, so it only touches
rows that are already stale.

---

## D15 — A no-op handler purely for benchmarking

**Problem.** The first load test reported ~4.5 jobs/sec completion throughput.
That number is wrong to quote — not inaccurate, but not measuring PulseQueue.
Three workers running a handler that sleeps 0.5s cannot exceed `3 ÷ 0.5 = 6`
jobs/sec regardless of how fast the queue is. The figure describes
`time.sleep`.

**Options.**
1. Report 4.5/s. Honest but meaningless as a system metric.
2. Remove the sleeps from the demo handlers. Destroys their purpose — the sleeps
   are what make retry, priority and backlog behaviour observable.
3. Add a handler that does nothing, and report both numbers with the distinction
   stated.

**Chosen.** Option 3. `benchmark_noop` returns `{"ok": True}` immediately.

**Reason.** With the simulated I/O removed, the measurement isolates what the
system actually costs per job: one atomic dequeue, three row updates, one
publish. That moved the figure from 4.5/s to **165 jobs/sec sustained**.

Reporting both, adjacent, with the explanation, is what makes either one
trustworthy. Reporting only the second would hide that real handlers dominate;
reporting only the first would understate the system by 35×.

**The generalisable heuristic:** before quoting a number, work out what would
have to change for it to move. If the answer is "a `sleep` constant", it is not
measuring the system.

**Tradeoff.** A handler in `builtin.py` that is not a real job type. Its
docstring says so explicitly, and it is genuinely useful for smoke-testing a
deployment.

---

## D16 — Overridable host ports in compose

**Problem.** The development machine already had containers bound to 5432, 6379
and 8000. `docker compose up` failed with
`Bind for 0.0.0.0:6379 failed: port is already allocated`.

**Options.**
1. Change the committed ports. Breaks the documented Quick Start for everyone
   else.
2. Tell people to edit `docker-compose.yml` locally. Guarantees an accidental
   commit of machine-specific values.
3. `"${REDIS_PORT:-6379}:6379"` — committed default, local override.

**Chosen.** Option 3.

**Reason.** A fresh clone gets the documented ports with no configuration. A busy
machine overrides them in a gitignored `.env` without touching a tracked file.
Container-to-container addressing is unaffected — the API still reaches
`postgres:5432` — because only the host side of the mapping changes.

Port collisions are the single most common reason a stranger's
`docker compose up` fails, which matters when Checkpoint 12 is "a stranger can
clone the repo and submit a job in under 5 minutes".

**Tradeoff.** The `.env` now serves two consumers — Compose variable
substitution and pydantic-settings — which is slightly surprising. Documented in
both `.env.example` and the runbook.

---

## D17 — Integration tests skip locally, fail in CI

**Problem.** 26 of the 97 tests need a live Postgres. If they hard-fail without
one, `pytest` on a fresh clone is red and the suite looks broken. If they skip
silently, a genuinely broken database configuration in CI produces a green
checkmark.

**Options.**
1. Always fail. Honest, but a fresh clone cannot run the tests.
2. Always skip. Convenient, but CI can pass while nothing was actually verified.
3. Skip by default; fail when `REQUIRE_INTEGRATION_TESTS=1`.

**Chosen.** Option 3. The CI workflow sets the variable.

**Reason.** Both audiences get the correct behaviour. A newcomer runs `pytest`,
gets 71 passed and 26 skipped with an actionable message
("`docker compose up -d postgres redis`"). CI, where the database is guaranteed
present, treats a skip as the failure it would actually be.

**Tradeoff.** Two behaviours for one condition, which must be documented or it
looks like a bug. It is, in the conftest docstring and the runbook.

---

## D18 — A CI job that submits a real job

**Problem.** A unit-test-only pipeline goes green while the compose file is
broken, the Dockerfile does not build, or the API cannot reach Postgres. The
README promises `docker compose up` works; nothing was checking that promise.

**Chosen.** A second CI job that runs `docker compose up -d --wait`, submits a
`resize_image` job over HTTP, and polls until it reaches `success`.

**Reason.** It executes the README's Quick Start on every push. `resize_image`
never fails, so reaching SUCCESS is deterministic — it exercises the API,
Postgres, Redis, the registry, a worker, and the full state machine in one
assertion. Polling rather than sleeping keeps it fast when it is fast.

The most common broken promise in a portfolio repo is a Quick Start that does not
work. This makes that specific failure impossible to merge.

**Tradeoff.** CI takes longer and needs Docker on the runner. Worth it — this job
catches a whole class of failure the unit tests structurally cannot.

**Status.** Written, never executed. There is no GitHub remote yet, so the green
checkmark is unverified.

---

## D19 — The worker is a class, and opens a session per job

**Problem.** The worker needs to hold a shutdown flag, a recovery timer, a worker
identity and a Redis handle. The original spec used module-level functions with a
`nonlocal` flag.

**Chosen.** A `Worker` class, with `run_worker()` kept as the module-level entry
point so `python -m app.workers.worker` is unchanged.

**Reason.** Four pieces of per-process state, two of which (the recovery timer
and the lock token) did not exist in the original design. `nonlocal` across
nested functions gets unreadable quickly, and instance attributes make the
lifecycle obvious.

**The session-per-job decision is the more important half.** `process_job` opens
`SessionLocal()` and closes it in `finally`. A session is a unit of work and an
identity map; holding one open across the idle poll loop would pin a pool
connection for the worker's entire lifetime, and its identity map would serve
stale objects across jobs. Opening one per job also means a poisoned session
from a failed flush cannot leak into the next job.

**Tradeoff.** A connection checkout per job. Negligible — the pool makes that
microseconds, and at 165 jobs/sec it did not appear in the profile.
