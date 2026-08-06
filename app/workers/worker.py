"""The worker process — a thin executor.

Why this file exists:
    Its entire job is: promote due retries, dequeue, look up a handler, run it,
    report the outcome, repeat. Every *decision* belongs to a service:

        what to do on failure      -> RetryService
        what to do about a job     -> RecoveryService
          abandoned by a dead worker
        how to talk to the queue   -> QueueService
        how to persist state       -> JobRepository
        which deps to unblock      -> DagService

    Count the policy decisions in this file: there are none. That is what makes
    the retry maths unit-testable without booting a worker, and it is the thing
    to point at when someone asks what "separation of concerns" bought here.

Per-job timeout
---------------
    Handlers run inside a ``ProcessPoolExecutor`` subprocess when
    ``job.timeout_seconds`` is set. This is the only Python mechanism that can
    actually preempt a handler — a thread cannot be killed from the outside, and
    ``asyncio.wait_for`` only works for cooperative coroutines. A process can be
    signalled, so ``future.result(timeout=N)`` gives a hard wall-clock limit.

    When the timeout fires, the subprocess is abandoned (not cleanly shut down;
    that is acceptable — the job was already going to be retried or dead-lettered
    anyway), and a ``JobTimeoutError`` is raised into the normal failure path.

Run it with::

    python -m app.workers.worker
"""

import logging
import os
import signal
import socket
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FuturesTimeoutError
from types import FrameType
from typing import Any, Optional

# Importing this module is what runs the @job_handler decorators and populates
# the registry. The worker deliberately does not know what is in it.
import handlers.builtin  # noqa: F401
from app.core.clock import utcnow
from app.core.config import settings
from app.core.database import SessionLocal, init_db
from app.core.exceptions import JobTimeoutError
from app.core.logging import configure_logging
from app.core.metrics import (
    claim_latency_seconds,
    dead_letter_depth,
    delayed_depth,
    job_duration_seconds,
    jobs_total,
    queue_depth,
)
from app.core.redis import get_redis_client
from app.models.job import Job, JobStatus
from app.registry.job_registry import get_handler, list_registered
from app.repositories.job_repository import JobRepository
from app.services.dag_service import DagService
from app.services.queue_service import QueueService
from app.services.recovery_service import RecoveryService
from app.services.retry_service import RetryService

log = logging.getLogger(__name__)

RECOVERY_LOCK = "recovery"


def _run_handler(job_type: str, payload: dict[str, Any]) -> dict:
    """Top-level function executed in a subprocess for timed jobs.

    Must be a module-level function (not a lambda or nested function) because
    ``ProcessPoolExecutor`` uses ``pickle`` to send it to the child process,
    and pickle cannot serialise closures.

    The child process re-imports handler modules via the normal import path, so
    the registry is populated from scratch — which is exactly what happens when
    you run the worker normally.
    """
    import handlers.builtin  # noqa: F401 — populates registry in child process
    from app.registry.job_registry import get_handler as _get_handler

    handler = _get_handler(job_type)
    return handler(payload)


class Worker:
    """One worker process. Scale by running more of them."""

    def __init__(self) -> None:
        self._redis = get_redis_client()
        self._queue = QueueService(self._redis)
        self._shutdown_requested = False
        # Identifies this process in logs and as the recovery lock's owner
        # token, so the lock can only be released by the holder.
        self._worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self._last_recovery_sweep = 0.0

    # -- signals -----------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        """Request shutdown on SIGTERM/SIGINT instead of dying immediately.

        The handler only sets a flag. The loop finishes the job in flight and
        then exits, so a deploy or a `docker compose down` never abandons work
        mid-execution. Killing the process outright is still possible, which is
        exactly the case ``RecoveryService`` exists to clean up after.
        """

        def request_shutdown(signum: int, _frame: Optional[FrameType]) -> None:
            log.info(
                "Signal %s received — finishing current job, then shutting down",
                signal.Signals(signum).name,
            )
            self._shutdown_requested = True

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, request_shutdown)
            except (ValueError, OSError):  # pragma: no cover - platform specific
                # Windows does not deliver a real SIGTERM; SIGINT (Ctrl+C) works.
                log.debug("Could not install handler for %s on this platform", sig)

    # -- one job -----------------------------------------------------------

    def process_job(self, job_id: str) -> None:
        """Execute a single job.

        A fresh session per job, closed in ``finally``. Sessions are the unit of
        work, and holding one open across an idle poll loop would pin a
        connection from the pool for the lifetime of the worker.
        """
        db = SessionLocal()
        repo = JobRepository(db)
        retry = RetryService(repo, self._queue)
        dag = DagService(repo, self._queue)
        job = None

        try:
            try:
                job_uuid = uuid.UUID(job_id)
            except ValueError:
                log.error("Dequeued malformed job id %r — discarding", job_id)
                return

            job = repo.get_by_id(job_uuid)
            if job is None:
                # The queue holds an id with no row behind it. Possible if the
                # database was reset while Redis kept its state. Nothing to do.
                log.warning("Job %s not found in the database — skipping", job_id)
                return

            # Guard against re-processing. A job can legitimately reach a worker
            # twice — recovered by the visibility timeout while the original
            # worker was merely slow, for instance. At-least-once delivery means
            # this is expected, not exceptional; the cheap status check turns
            # most duplicates into no-ops.
            if job.status not in (JobStatus.QUEUED, JobStatus.FAILED):
                log.info(
                    "Job %s is already %s — skipping", job_id, job.status.value
                )
                return

            # --- Claim latency metric -------------------------------------
            # created_at is the moment the job was submitted. The delta from
            # there to now is the end-to-end queue wait time — the number that
            # answers "how quickly does a job get picked up?".
            claim_wait = (utcnow() - job.created_at).total_seconds()
            claim_latency_seconds.labels(job_type=job.job_type).observe(claim_wait)

            repo.increment_attempts(job)
            repo.update_status(job, JobStatus.RUNNING)
            self._queue.publish_update(job_id, JobStatus.RUNNING)
            log.info(
                "Processing %s job %s (attempt %d/%d)",
                job.job_type,
                job_id,
                job.attempts,
                job.max_attempts,
            )

            started = time.monotonic()
            try:
                handler = get_handler(job.job_type)
            except KeyError as exc:
                # No amount of backoff will conjure a handler into existence.
                # Dead-letter immediately rather than burning the attempt budget
                # and burying the real cause under retry noise.
                log.error(
                    "Unknown job_type '%s' (registered: %s)",
                    job.job_type,
                    list_registered(),
                )
                retry.handle_failure(job, exc, permanent=True)
                jobs_total.labels(job_type=job.job_type, outcome="dead_letter").inc()
                return

            # --- Execute (with optional timeout) --------------------------
            result = self._execute_handler(job, handler)

            duration = time.monotonic() - started
            repo.update_status(job, JobStatus.SUCCESS, result=result)
            self._queue.publish_update(job_id, JobStatus.SUCCESS)

            # Record metrics for a successful job.
            job_duration_seconds.labels(
                job_type=job.job_type, status="success"
            ).observe(duration)
            jobs_total.labels(job_type=job.job_type, outcome="success").inc()

            log.info("Job %s succeeded in %.3fs", job_id, duration)

            # --- DAG fan-out: unblock downstream dependents ---------------
            # After any SUCCESS, check if downstream PENDING jobs are now
            # fully unblocked. This is O(downstream_count × dep_count) and
            # runs synchronously — acceptable at this scale.
            unblocked = dag.resolve_dependents(job_uuid)
            if unblocked:
                log.info(
                    "Job %s unblocked %d downstream jobs: %s",
                    job_id,
                    len(unblocked),
                    unblocked,
                )

        except JobTimeoutError as exc:
            # Timeout is its own metric label so it is visible separately from
            # ordinary handler failures in the Prometheus dashboard.
            if job is not None:
                duration = time.monotonic() - started if "started" in dir() else 0.0
                job_duration_seconds.labels(
                    job_type=job.job_type, status="timeout"
                ).observe(duration)
                jobs_total.labels(job_type=job.job_type, outcome="timeout").inc()
                log.warning(
                    "Job %s timed out after %ds", job_id, job.timeout_seconds
                )
                retry.handle_failure(job, exc)

        except Exception as exc:
            if job is None:
                # The failure happened before the job was loaded — a database
                # blip, most likely. There is no row to transition, and the job
                # id has already left the queue, so the visibility timeout is
                # not applicable either. Log loudly; the row (if any) stays
                # QUEUED and stays visible in the API.
                log.exception("Failed before loading job %s: %s", job_id, exc)
                return
            log.warning("Job %s raised %s: %s", job_id, type(exc).__name__, exc)

            duration = time.monotonic() - (started if "started" in dir() else time.monotonic())
            job_duration_seconds.labels(
                job_type=job.job_type, status="failed"
            ).observe(duration)

            try:
                outcome = retry.handle_failure(job, exc)
                outcome_label = (
                    "dead_letter" if outcome == JobStatus.DEAD_LETTER else "failed"
                )
                jobs_total.labels(job_type=job.job_type, outcome=outcome_label).inc()
            except Exception:  # pragma: no cover - defensive
                # If even the failure path fails (database gone), leave the job
                # in RUNNING. RecoveryService reclaims it once the visibility
                # timeout elapses — which is precisely why that service exists.
                log.exception("Could not record failure for job %s", job_id)
        finally:
            db.close()

    def _execute_handler(self, job: Job, handler) -> dict:
        """Run the handler, enforcing timeout_seconds if set.

        When a timeout is configured, the handler runs in a subprocess via
        ``ProcessPoolExecutor``. This is the only reliable way to enforce a
        wall-clock limit in Python — threads cannot be preempted from outside,
        and asyncio only helps for cooperative code.

        Without a timeout, the handler runs directly in the worker process
        (cheaper — no pickle overhead, no subprocess fork).

        Raises:
            JobTimeoutError: the handler did not finish within ``timeout_seconds``.
            Any exception the handler itself raises (propagated unchanged).
        """
        if not job.timeout_seconds:
            return handler(job.payload)

        with ProcessPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_handler, job.job_type, job.payload)
            try:
                return future.result(timeout=job.timeout_seconds)
            except FuturesTimeoutError:
                # Cancel the future (best-effort; the subprocess may still run
                # briefly before the executor shuts down on context exit).
                future.cancel()
                raise JobTimeoutError(
                    f"Job {job.id} ({job.job_type}) exceeded timeout of "
                    f"{job.timeout_seconds}s"
                )

    # -- gauge refresh -----------------------------------------------------

    def _refresh_gauges(self) -> None:
        """Update queue-depth gauges so Prometheus always sees current values.

        Prometheus scrapes these values at scrape time; they are not event-
        driven. Refreshing them in the worker loop (before every BZPOPMIN) means
        they lag by at most one loop iteration, which is acceptable.
        """
        try:
            queue_depth.set(self._queue.queue_depth())
            delayed_depth.set(self._queue.delayed_depth())
            dead_letter_depth.set(self._queue.dead_letter_depth())
        except Exception:  # pragma: no cover - Redis blip
            pass  # stale gauges are better than a crashed worker

    # -- periodic recovery -------------------------------------------------

    def _maybe_run_recovery(self) -> None:
        """Run the abandoned-job sweep, at most once per interval, fleet-wide.

        All workers call this on their own timer, but the Redis lock means only
        one actually scans. The lock TTL matches the interval, so if the holder
        dies mid-sweep the next worker simply picks it up.
        """
        now = time.monotonic()
        if now - self._last_recovery_sweep < settings.recovery_interval:
            return
        self._last_recovery_sweep = now

        if not self._queue.acquire_lock(
            RECOVERY_LOCK, ttl_seconds=settings.recovery_interval, token=self._worker_id
        ):
            return  # another worker is handling it

        db = SessionLocal()
        try:
            recovery = RecoveryService(JobRepository(db), self._queue)
            # Two distinct orphan paths, both invisible without a sweep:
            #   stuck    — worker died mid-execution, job left in RUNNING
            #   orphaned — row committed but the enqueue never landed in Redis
            recovery.requeue_stuck_jobs()
            recovery.requeue_orphaned_jobs()
        except Exception:  # pragma: no cover - defensive
            log.exception("Recovery sweep failed")
        finally:
            db.close()
            self._queue.release_lock(RECOVERY_LOCK, self._worker_id)

    # -- main loop ---------------------------------------------------------

    def run(self) -> None:
        self._install_signal_handlers()
        init_db()
        log.info(
            "Worker %s started at %s — handlers: %s",
            self._worker_id,
            utcnow().isoformat(),
            list_registered(),
        )

        while not self._shutdown_requested:
            try:
                # Return any retries whose backoff has elapsed to the ready
                # queue before blocking on it, so a due retry is never waiting
                # behind an idle BZPOPMIN.
                self._queue.promote_due()
                self._maybe_run_recovery()
                self._refresh_gauges()

                # Blocks until a job arrives or the timeout expires. The
                # timeout is what bounds how long shutdown and the recovery
                # timer have to wait; it is not a poll interval, because a job
                # arriving mid-block wakes this immediately.
                job_id = self._queue.dequeue(timeout=settings.dequeue_timeout)
                if job_id:
                    self.process_job(job_id)
            except Exception as exc:
                # Never let the loop die. A Redis blip must degrade throughput,
                # not take the worker down.
                log.exception("Unexpected error in the worker loop: %s", exc)
                time.sleep(1)

        log.info("Worker %s shut down cleanly", self._worker_id)


def run_worker() -> None:
    """Process entry point."""
    configure_logging("worker")
    Worker().run()


if __name__ == "__main__":
    run_worker()
