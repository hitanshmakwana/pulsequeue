"""JobService — submit, fetch, list, manual retry.

Why this file exists:
    Business logic: idempotency enforcement, applying server-side defaults, and
    guarding illegal state transitions. The router calls ``submit()`` and gets
    a job back; it has no idea a database or a queue was involved.

Who owns this:
    ``JobService`` decides *what* should happen. ``JobRepository`` decides how
    it is stored. ``QueueService`` decides how it is queued. This class never
    builds an HTTP response and never touches Redis or SQL directly.
"""

import logging
import uuid
from typing import Optional

from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.exceptions import (
    DuplicateIdempotencyKey,
    InvalidStateTransition,
    JobNotFound,
)
from app.models.job import Job, JobStatus
from app.repositories.job_repository import JobRepository
from app.schemas.job import JobCreate
from app.services.queue_service import QueueService

log = logging.getLogger(__name__)


class JobService:
    def __init__(self, db: Session, queue: QueueService):
        self._repo = JobRepository(db)
        self._queue = queue

    def submit(self, job_in: JobCreate) -> Job:
        """Persist a job, then make it available to workers.

        Idempotency is enforced in two places, deliberately:

        1. A pre-check on ``idempotency_key``. This is the fast path — it
           catches the ordinary case (a client retrying a request seconds or
           minutes later) without attempting a doomed INSERT.

        2. Catching the unique-constraint violation. The pre-check is a
           check-then-act sequence and therefore racy: two concurrent requests
           with the same key can both read "no existing job" before either
           inserts. Only the database can actually adjudicate that race, so we
           let it, and the loser re-reads the winner's row.

        Without step 2, a burst of concurrent duplicate submissions returns
        500s. With it, every one of them returns the same job. The PRD requires
        this to be validated under concurrency, so it has to actually hold
        under concurrency.
        """
        if job_in.idempotency_key:
            existing = self._repo.get_by_idempotency_key(job_in.idempotency_key)
            if existing:
                log.info(
                    "Idempotent hit for key '%s' -> job %s",
                    job_in.idempotency_key,
                    existing.id,
                )
                return existing

        try:
            job = self._repo.create(
                job_type=job_in.job_type,
                payload=job_in.payload,
                priority=job_in.priority,
                idempotency_key=job_in.idempotency_key,
                # Server-side default: the client may omit max_attempts, in
                # which case the operator's configured policy applies. Defaults
                # are a business decision, so they are resolved here rather
                # than hardcoded into the wire schema.
                max_attempts=job_in.max_attempts or settings.max_retry_attempts,
            )
        except DuplicateIdempotencyKey:
            # Lost the race described above. The winner's row is now committed.
            winner = self._repo.get_by_idempotency_key(job_in.idempotency_key)
            if winner is None:
                # Only reachable if the constraint fired for some other reason.
                raise
            log.info(
                "Idempotency race for key '%s' -> returning job %s",
                job_in.idempotency_key,
                winner.id,
            )
            return winner

        # Enqueue only after the row is committed. The ordering matters: if the
        # process dies between the two, the job exists in Postgres as QUEUED and
        # is recoverable. The reverse ordering would put an id on the queue that
        # no worker could ever resolve to a row.
        self._queue.enqueue(str(job.id), job.priority)
        self._queue.publish_update(str(job.id), JobStatus.QUEUED)
        log.info("Submitted %s job %s (priority %s)", job.job_type, job.id, job.priority)
        return job

    def get(self, job_id: uuid.UUID) -> Optional[Job]:
        return self._repo.get_by_id(job_id)

    def list(
        self, status: Optional[JobStatus] = None, limit: int = 50
    ) -> list[Job]:
        return self._repo.list_by_status(status, limit)

    def manual_retry(self, job_id: uuid.UUID) -> Job:
        """Re-queue a dead-lettered job with a fresh attempt budget.

        Raises:
            JobNotFound: no such job.
            InvalidStateTransition: the job is not dead-lettered. Re-queueing a
                RUNNING job would double-process it; re-queueing a SUCCESS job
                would re-run a side effect that already happened.
        """
        job = self._repo.get_by_id(job_id)
        if not job:
            raise JobNotFound(f"Job {job_id} not found")
        if job.status != JobStatus.DEAD_LETTER:
            raise InvalidStateTransition(
                f"Only dead-lettered jobs can be manually retried "
                f"(job {job_id} is '{job.status.value}')"
            )

        # Reset the counter through the repository so it is actually committed.
        job = self._repo.reset_attempts(job)
        job = self._repo.update_status(job, JobStatus.QUEUED, result=None)

        # Take it off the dead-letter list — it no longer needs human attention.
        self._queue.remove_dead_letter(str(job.id))
        self._queue.enqueue(str(job.id), job.priority)
        self._queue.publish_update(str(job.id), JobStatus.QUEUED)
        log.info("Manually re-queued job %s", job.id)
        return job
