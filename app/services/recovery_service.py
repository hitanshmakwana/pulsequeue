"""RecoveryService — reclaims jobs abandoned by dead workers.

Why this file exists:
    ``ZPOPMIN``/``BZPOPMIN`` remove a job from Redis the instant a worker takes
    it. That atomicity is what prevents double-processing, but it has a
    consequence: if the worker is then hard-killed (SIGKILL, OOM, node loss)
    before reporting an outcome, the job exists nowhere except as a row stuck
    in RUNNING. Nothing will ever pick it up again.

    Graceful shutdown handles SIGTERM. It cannot handle a process that stops
    executing instructions. Without this service, "no job is silently lost on
    worker crash" would be a claim the code does not back up — and the load
    test's kill-a-worker step would simply leave jobs wedged.

    The mechanism is a visibility timeout, the same primitive SQS exposes: a
    job checked out for longer than any legitimate execution could take is
    presumed abandoned and returned to the queue.

Who owns this:
    ``RecoveryService`` owns the abandoned-job decision. The worker's only
    involvement is calling ``requeue_stuck_jobs()`` on a timer under a lock.
"""

import logging
import uuid
from datetime import timedelta
from typing import Optional

from app.core.clock import utcnow
from app.core.config import settings
from app.models.job import JobStatus
from app.repositories.job_repository import JobRepository
from app.services.queue_service import QueueService

log = logging.getLogger(__name__)


class RecoveryService:
    def __init__(self, repo: JobRepository, queue: QueueService):
        self._repo = repo
        self._queue = queue

    def requeue_stuck_jobs(
        self, visibility_timeout: Optional[int] = None, limit: int = 100
    ) -> list[uuid.UUID]:
        """Return abandoned RUNNING jobs to the queue, or dead-letter them.

        Args:
            visibility_timeout: Seconds a job may stay RUNNING before it is
                presumed abandoned. Defaults to the configured value. Must
                exceed the runtime of the slowest legitimate handler, otherwise
                healthy long-running jobs get duplicated — which is precisely
                the at-least-once tradeoff, made explicit and tunable.
            limit: Cap on jobs reclaimed per sweep, so one sweep after a large
                outage cannot monopolise a worker.

        Returns:
            The ids that were reclaimed.
        """
        timeout = (
            visibility_timeout
            if visibility_timeout is not None
            else settings.visibility_timeout
        )
        cutoff = utcnow() - timedelta(seconds=timeout)
        stuck = self._repo.list_stuck_running(cutoff, limit=limit)
        if not stuck:
            return []

        recovered: list[uuid.UUID] = []
        for job in stuck:
            # The attempt was already consumed before the handler ran, so a job
            # that reliably kills its worker still burns through its budget and
            # eventually dead-letters instead of crash-looping the fleet
            # forever. This is the poison-pill guard.
            if job.attempts >= job.max_attempts:
                log.error(
                    "Recovered job %s had exhausted its attempts — dead-lettering",
                    job.id,
                )
                self._repo.update_status(
                    job,
                    JobStatus.DEAD_LETTER,
                    result={
                        "error": "Worker lost while the job was running",
                        "reason": "abandoned and attempts exhausted",
                        "final": True,
                    },
                )
                self._queue.enqueue_dead_letter(str(job.id))
                self._queue.publish_update(str(job.id), JobStatus.DEAD_LETTER)
            else:
                log.warning(
                    "Recovering job %s stuck in RUNNING since %s — re-queueing",
                    job.id,
                    job.updated_at,
                )
                self._repo.update_status(
                    job,
                    JobStatus.QUEUED,
                    result={
                        "error": "Worker lost while the job was running",
                        "reason": "recovered by visibility timeout",
                        "attempt": job.attempts,
                    },
                )
                self._queue.enqueue(str(job.id), job.priority)
                self._queue.publish_update(str(job.id), JobStatus.QUEUED)

            recovered.append(job.id)

        log.info("Recovery sweep reclaimed %d abandoned job(s)", len(recovered))
        return recovered

    def requeue_orphaned_jobs(
        self, visibility_timeout: Optional[int] = None, limit: int = 100
    ) -> list[uuid.UUID]:
        """Re-enqueue pending jobs that are missing from Redis entirely.

        ``JobService.submit`` commits the row and then enqueues the id. Those
        are two separate systems and there is no transaction spanning them, so
        a process death in between — or a Redis flush, or a failover to a
        replica that had not caught up — leaves a row in QUEUED that no worker
        will ever see. The row is not lost, but without this it is inert, which
        from the caller's point of view is the same thing.

        Fixing the window itself would require a distributed transaction or an
        outbox table. This is the pragmatic alternative: notice and repair.

        The membership check is what makes it safe. A backlogged queue and an
        orphaned job look identical from the database — both are old rows in
        QUEUED — so re-enqueueing on age alone would duplicate every job in a
        queue that merely got behind.
        """
        timeout = (
            visibility_timeout
            if visibility_timeout is not None
            else settings.visibility_timeout
        )
        cutoff = utcnow() - timedelta(seconds=timeout)
        candidates = self._repo.list_stale_pending(cutoff, limit=limit)
        if not candidates:
            return []

        requeued: list[uuid.UUID] = []
        for job in candidates:
            if self._queue.contains(str(job.id), job.priority):
                continue  # genuinely waiting its turn, not orphaned

            log.warning(
                "Job %s is %s but absent from Redis — re-enqueueing",
                job.id,
                job.status.value,
            )
            if job.status != JobStatus.QUEUED:
                self._repo.update_status(job, JobStatus.QUEUED)
            self._queue.enqueue(str(job.id), job.priority)
            self._queue.publish_update(str(job.id), JobStatus.QUEUED)
            requeued.append(job.id)

        if requeued:
            log.info("Recovery sweep re-enqueued %d orphaned job(s)", len(requeued))
        return requeued
