"""The worker process — a thin executor.

Why this file exists:
    Its entire job is: promote due retries, dequeue, look up a handler, run it,
    report the outcome, repeat. Every *decision* belongs to a service:

        what to do on failure      -> RetryService
        what to do about a job     -> RecoveryService
          abandoned by a dead worker
        how to talk to the queue   -> QueueService
        how to persist state       -> JobRepository

    Count the policy decisions in this file: there are none. That is what makes
    the retry maths unit-testable without booting a worker, and it is the thing
    to point at when someone asks what "separation of concerns" bought here.

Run it with::

    python -m app.workers.worker
"""

import logging
import os
import signal
import socket
import time
import uuid
from types import FrameType
from typing import Optional

# Importing this module is what runs the @job_handler decorators and populates
# the registry. The worker deliberately does not know what is in it.
import handlers.builtin  # noqa: F401
from app.core.clock import utcnow
from app.core.config import settings
from app.core.database import SessionLocal, init_db
from app.core.logging import configure_logging
from app.core.redis import get_redis_client
from app.models.job import JobStatus
from app.registry.job_registry import get_handler, list_registered
from app.repositories.job_repository import JobRepository
from app.services.queue_service import QueueService
from app.services.recovery_service import RecoveryService
from app.services.retry_service import RetryService

log = logging.getLogger(__name__)

RECOVERY_LOCK = "recovery"


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
                return

            result = handler(job.payload)
            duration = time.monotonic() - started

            repo.update_status(job, JobStatus.SUCCESS, result=result)
            self._queue.publish_update(job_id, JobStatus.SUCCESS)
            log.info("Job %s succeeded in %.3fs", job_id, duration)

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
            try:
                retry.handle_failure(job, exc)
            except Exception:  # pragma: no cover - defensive
                # If even the failure path fails (database gone), leave the job
                # in RUNNING. RecoveryService reclaims it once the visibility
                # timeout elapses — which is precisely why that service exists.
                log.exception("Could not record failure for job %s", job_id)
        finally:
            db.close()

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
