"""QueueService — the only code in the system that speaks Redis.

Why this file exists:
    Every key name, every score calculation, and all pub/sub plumbing live
    here. If Redis were ever swapped for SQS or NATS, this is the one file that
    changes. Because it takes its client by injection, all of its logic is unit
    testable against a ``MagicMock`` with no live Redis.

Who owns this:
    ``QueueService`` owns enqueue, dequeue, delayed retries, the dead-letter
    queue, pub/sub, and the distributed lock. The worker calls it. ``JobService``,
    ``RetryService`` and ``RecoveryService`` call it. Nothing else touches a
    Redis key.

Key layout
----------
    pq:queue          ZSET   ready jobs, score = priority band + enqueue time
    pq:delayed        ZSET   jobs waiting out a retry backoff, score = ready-at
    pq:dlq            LIST   permanently failed job ids, newest first
    pq:updates        chan   pub/sub feed consumed by the WebSocket endpoint
    pq:lock:*         STRING short-lived mutexes (SET NX EX)
"""

import json
import logging
from typing import Optional

import redis

from app.core.clock import epoch_ms

# --- Key naming convention: defined once, here ---------------------------
QUEUE_KEY = "pq:queue"  # Main sorted set (priority queue)
DELAYED_KEY = "pq:delayed"  # Retry backoff set (scored by ready-at time)
DLQ_KEY = "pq:dlq"  # Dead-letter queue (simple list)
CHANNEL = "pq:updates"  # Pub/Sub channel for the live dashboard
LOCK_PREFIX = "pq:lock:"  # Namespace for distributed locks

# Width of one priority band, in milliseconds. Any epoch-millisecond timestamp
# is ~1.7e12, comfortably below 1e13, so `priority * 1e13 + now_ms` can never
# let a priority-4 job sort ahead of a priority-3 job no matter how old it is.
# The whole score stays under 5.2e13 — well inside the ~9e15 range a float64
# represents exactly, so no two distinct scores ever collide through rounding.
PRIORITY_BAND = 10**13

# Atomically move every due job from the delayed set into the ready queue.
#
# Why Lua: the read (which jobs are due?) and the write (move them) must be one
# indivisible step. Doing it with separate ZRANGEBYSCORE + ZADD + ZREM calls
# would let two workers both observe the same due job and enqueue it twice.
# Redis executes a script as a single atomic unit, which removes the race.
_PROMOTE_DUE_LUA = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due == 0 then
    return 0
end
local now = tonumber(ARGV[3])
local band = tonumber(ARGV[4])
local moved = 0
for _, member in ipairs(due) do
    -- member is encoded "<priority>:<job_id>" so the original priority
    -- survives the trip through the delayed set.
    local sep = string.find(member, ':')
    if sep then
        local priority = tonumber(string.sub(member, 1, sep - 1))
        local job_id = string.sub(member, sep + 1)
        if priority and job_id ~= '' then
            redis.call('ZADD', KEYS[2], priority * band + now, job_id)
        end
    end
    redis.call('ZREM', KEYS[1], member)
    moved = moved + 1
end
return moved
"""

# Release a lock only if we still hold it. Comparing-then-deleting in two
# round-trips could delete a lock that expired and was re-acquired by another
# worker in between.
_RELEASE_LOCK_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

log = logging.getLogger(__name__)


class QueueService:
    def __init__(self, r: redis.Redis):
        self._r = r
        # register_script returns a callable that uses EVALSHA with an
        # automatic EVAL fallback if the script isn't cached server-side.
        self._promote_due = r.register_script(_PROMOTE_DUE_LUA)
        self._release_lock = r.register_script(_RELEASE_LOCK_LUA)

    # -- scoring -----------------------------------------------------------

    def _job_score(self, priority: int) -> float:
        """Sorted-set score. Lower score is popped first.

        Priority is the primary sort key; enqueue time is the tiebreaker, so
        two jobs of equal priority are processed first-in-first-out. (A
        uuid-derived tiebreaker would be arbitrary, not FIFO — it would let a
        job submitted an hour ago sit behind one submitted a second ago.)
        """
        return float(priority * PRIORITY_BAND + epoch_ms())

    # -- ready queue -------------------------------------------------------

    def enqueue(self, job_id: str, priority: int) -> None:
        """Add a job to the ready queue."""
        self._r.zadd(QUEUE_KEY, {job_id: self._job_score(priority)})

    def dequeue(self, timeout: Optional[int] = None) -> Optional[str]:
        """Atomically pop the highest-priority ready job, blocking if empty.

        BZPOPMIN blocks on the key until a member exists or ``timeout`` seconds
        elapse, then pops the member with the LOWEST score — our highest
        priority. The pop is atomic: Redis is single-threaded, so of N workers
        blocked on the same key, exactly one receives any given job. That
        atomicity is the entire basis of the "3+ workers, zero double
        processing" guarantee.

        Blocking rather than polling means a worker wakes the instant a job
        lands instead of up to one poll interval later, which is the difference
        between sub-millisecond and ~500ms average pickup latency on an
        otherwise idle queue.

        Args:
            timeout: Seconds to block. 0 blocks forever. ``None`` is treated as
                a non-blocking pop (see ``dequeue_nowait``).

        Returns:
            The job id, or ``None`` if the timeout expired with an empty queue.
        """
        if timeout is None:
            return self.dequeue_nowait()

        result = self._r.bzpopmin(QUEUE_KEY, timeout=timeout)
        if not result:
            return None
        _key, job_id, _score = result
        return job_id

    def dequeue_nowait(self) -> Optional[str]:
        """Non-blocking atomic pop. Returns ``None`` immediately if empty."""
        result = self._r.zpopmin(QUEUE_KEY, count=1)
        if not result:
            return None
        job_id, _score = result[0]
        return job_id

    def queue_depth(self) -> int:
        """Number of jobs waiting to be picked up."""
        return int(self._r.zcard(QUEUE_KEY))

    # -- delayed retries ---------------------------------------------------

    def enqueue_delayed(self, job_id: str, priority: int, delay_seconds: float) -> None:
        """Schedule a job to become ready after ``delay_seconds``.

        This is how retry backoff is implemented. The alternative — having the
        worker ``time.sleep(delay)`` — would idle an entire worker process for
        the duration of the backoff while other jobs queue up behind it. Under
        load with a non-trivial failure rate, that alone can dominate
        throughput.

        The member is encoded ``"<priority>:<job_id>"`` because the delayed set
        is ordered by *time*, so the job's priority has nowhere else to live
        until it is promoted back into the ready queue.
        """
        ready_at = epoch_ms() + int(delay_seconds * 1000)
        self._r.zadd(DELAYED_KEY, {f"{priority}:{job_id}": ready_at})

    def promote_due(self, limit: int = 100) -> int:
        """Move every job whose backoff has elapsed into the ready queue.

        Called by each worker at the top of its loop. Safe to run concurrently
        from every worker in the fleet — the Lua script makes the whole
        read-and-move sequence atomic, so a job cannot be promoted twice.

        Returns:
            How many jobs were promoted.
        """
        return int(
            self._promote_due(
                keys=[DELAYED_KEY, QUEUE_KEY],
                args=[epoch_ms(), limit, epoch_ms(), PRIORITY_BAND],
            )
        )

    def delayed_depth(self) -> int:
        """Number of jobs currently waiting out a retry backoff."""
        return int(self._r.zcard(DELAYED_KEY))

    # -- membership --------------------------------------------------------

    def contains(self, job_id: str, priority: int) -> bool:
        """Is this job already sitting in the ready queue or the delayed set?

        ``RecoveryService`` uses this before re-enqueueing a job that looks
        orphaned. A deep backlog and an orphaned job are indistinguishable from
        the database alone — both are rows in QUEUED that nothing has touched
        recently — so without this check, a queue that simply got behind would
        have every job in it enqueued a second time.

        The delayed set stores members as ``"<priority>:<job_id>"``, which is
        why the priority has to be supplied to look one up.
        """
        if self._r.zscore(QUEUE_KEY, job_id) is not None:
            return True
        return self._r.zscore(DELAYED_KEY, f"{priority}:{job_id}") is not None

    # -- dead-letter queue -------------------------------------------------

    def enqueue_dead_letter(self, job_id: str) -> None:
        """Push a permanently failed job onto the dead-letter queue."""
        self._r.lpush(DLQ_KEY, job_id)

    def remove_dead_letter(self, job_id: str) -> int:
        """Remove a job from the dead-letter list.

        Called when a dead-lettered job is manually retried — otherwise the DLQ
        would accumulate ids of jobs that have since been revived, and its
        depth would stop meaning "things needing human attention".
        """
        return int(self._r.lrem(DLQ_KEY, 0, job_id))

    def dead_letter_depth(self) -> int:
        return int(self._r.llen(DLQ_KEY))

    # -- pub/sub -----------------------------------------------------------

    def publish_update(self, job_id: str, status: str) -> None:
        """Broadcast a status change to the live dashboard.

        Fire-and-forget by design: the dashboard is an observability nicety,
        and a dropped update must never affect job execution. Redis pub/sub has
        no delivery guarantee, and that is the correct tradeoff here — the
        dashboard reconciles against ``GET /jobs/stats`` on every event.
        """
        # `status` arrives as either a JobStatus member or a plain string;
        # normalise so the wire format is always the lowercase value.
        status_value = getattr(status, "value", status)
        message = json.dumps({"job_id": job_id, "status": status_value})
        self._r.publish(CHANNEL, message)

    # -- distributed lock --------------------------------------------------

    def acquire_lock(self, name: str, ttl_seconds: int, token: str) -> bool:
        """Try to take a mutex shared across all worker processes.

        ``SET key value NX EX ttl`` is atomic: exactly one caller succeeds. The
        TTL is a deadlock guard — a worker that dies holding the lock releases
        it implicitly when the key expires.

        Used so that only one worker in the fleet performs the recovery sweep,
        rather than all of them scanning the same rows simultaneously.
        """
        return bool(
            self._r.set(f"{LOCK_PREFIX}{name}", token, nx=True, ex=ttl_seconds)
        )

    def release_lock(self, name: str, token: str) -> bool:
        """Release a lock, but only if this caller still owns it."""
        return bool(
            self._release_lock(keys=[f"{LOCK_PREFIX}{name}"], args=[token])
        )
