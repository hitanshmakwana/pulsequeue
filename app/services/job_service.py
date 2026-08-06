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
    DagCycleError,
    DuplicateIdempotencyKey,
    InvalidStateTransition,
    JobNotFound,
    UnresolvableDependency,
)
from app.models.job import Job, JobStatus
from app.repositories.job_repository import JobRepository
from app.schemas.job import JobCreate
from app.services.dag_service import DagService
from app.services.queue_service import QueueService

log = logging.getLogger(__name__)


class JobService:
    def __init__(self, db: Session, queue: QueueService):
        self._repo = JobRepository(db)
        self._queue = queue
        self._dag = DagService(self._repo, queue)

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

        DAG path:
            If ``depends_on`` is non-empty, the job is created as PENDING and
            NOT enqueued. ``DagService.resolve_dependents`` will transition it
            to QUEUED once all its dependencies reach SUCCESS. If any dep does
            not exist, ``UnresolvableDependency`` is raised. If the dep graph
            would form a cycle, ``DagCycleError`` is raised.
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

        # --- DAG validation -----------------------------------------------
        initial_status = JobStatus.QUEUED
        if job_in.depends_on:
            self._validate_deps(job_in.depends_on)
            initial_status = JobStatus.PENDING

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
                timeout_seconds=job_in.timeout_seconds,
                depends_on=job_in.depends_on,
                status=initial_status,
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

        if initial_status == JobStatus.PENDING:
            # DAG job: do not enqueue. Notify the dashboard so observers can
            # see it was accepted as PENDING.
            self._queue.publish_update(str(job.id), JobStatus.PENDING)
            log.info(
                "Submitted DAG job %s (%s) as PENDING — waiting on %d dep(s)",
                job.id,
                job.job_type,
                len(job_in.depends_on),
            )
        else:
            # Enqueue only after the row is committed. The ordering matters: if the
            # process dies between the two, the job exists in Postgres as QUEUED and
            # is recoverable. The reverse ordering would put an id on the queue that
            # no worker could ever resolve to a row.
            self._queue.enqueue(str(job.id), job.priority)
            self._queue.publish_update(str(job.id), JobStatus.QUEUED)
            log.info(
                "Submitted %s job %s (priority %s)", job.job_type, job.id, job.priority
            )

        return job

    def _validate_deps(self, dep_ids: list[uuid.UUID]) -> None:
        """Ensure all dep IDs exist and adding them would not form a cycle.

        Raises:
            UnresolvableDependency: a referenced job does not exist.
            DagCycleError: the new job would close a cycle in the graph.
        """
        dep_jobs = self._repo.get_many_by_ids(dep_ids)
        found_ids = {j.id for j in dep_jobs}

        missing = [str(d) for d in dep_ids if d not in found_ids]
        if missing:
            raise UnresolvableDependency(
                f"The following dependency job IDs do not exist: {missing}"
            )

        # Build the existing dependency map for cycle detection.
        # We only need the subgraph reachable from the new deps, not the whole
        # table — but for simplicity we load just the direct dep rows here.
        # A full transitive-closure check would require a recursive CTE query.
        existing_deps_map: dict[uuid.UUID, list[uuid.UUID]] = {
            j.id: j.depends_on or [] for j in dep_jobs
        }

        if not self._dag.validate_no_cycle(dep_ids, existing_deps_map):
            raise DagCycleError(
                f"Adding depends_on={dep_ids} would create a cycle in the job DAG"
            )

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
