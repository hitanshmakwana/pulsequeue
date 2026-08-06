"""JobRepository — the only file in the codebase that issues SQL.

Why this file exists:
    Services call repository methods *by name*; they never build a query. When
    a query needs to change — add a filter, add an index hint, join a table —
    there is exactly one file to open. Swapping PostgreSQL for another engine
    touches nothing outside this file.

Who owns this:
    ``JobRepository`` owns all reads and writes of the ``jobs`` table. Services
    own it. Routers must never import it. It knows nothing about Redis, HTTP,
    or retry policy.
"""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.clock import utcnow
from app.core.exceptions import DuplicateIdempotencyKey
from app.models.job import Job, JobStatus


class JobRepository:
    def __init__(self, db: Session):
        self._db = db

    # -- writes ------------------------------------------------------------

    def create(
        self,
        job_type: str,
        payload: dict,
        priority: int,
        idempotency_key: Optional[str],
        max_attempts: int,
        timeout_seconds: Optional[int] = None,
        depends_on: Optional[list[uuid.UUID]] = None,
        status: JobStatus = JobStatus.QUEUED,
    ) -> Job:
        """Insert a new job row and return it.

        Args:
            status: Callers creating a DAG-dependent job pass ``PENDING`` here.
                The default is ``QUEUED`` so existing callers need no changes.

        Raises:
            DuplicateIdempotencyKey: another row already holds this key. The
                SQLAlchemy exception is translated into a domain exception here
                so that no caller above this layer has to import SQLAlchemy to
                handle it.
        """
        job = Job(
            job_type=job_type,
            payload=payload,
            priority=priority,
            idempotency_key=idempotency_key,
            max_attempts=max_attempts,
            timeout_seconds=timeout_seconds,
            depends_on=depends_on or [],
            status=status,
        )
        self._db.add(job)
        try:
            self._db.commit()
        except IntegrityError as exc:
            # The session is poisoned after a failed flush; it must be rolled
            # back before it can be used again to look up the winning row.
            self._db.rollback()
            raise DuplicateIdempotencyKey(
                f"A job with idempotency_key '{idempotency_key}' already exists"
            ) from exc
        self._db.refresh(job)
        return job

    def update_status(
        self,
        job: Job,
        status: JobStatus,
        result: Optional[dict] = None,
    ) -> Job:
        """Update a job's status, and its result if one is supplied.

        ``result=None`` means "leave the existing result alone" rather than
        "clear it" — a job moving FAILED -> QUEUED for a retry should keep the
        error that explains why it is being retried.
        """
        job.status = status
        job.updated_at = utcnow()
        if result is not None:
            job.result = result
        self._db.commit()
        self._db.refresh(job)
        return job

    def increment_attempts(self, job: Job) -> Job:
        """Consume one attempt.

        Called *before* the handler runs, not after. If the worker is killed
        mid-execution, the attempt has already been recorded, so a job that
        reliably crashes its worker still exhausts its budget and dead-letters
        instead of looping forever. That is the poison-pill guard.
        """
        job.attempts += 1
        job.updated_at = utcnow()
        self._db.commit()
        self._db.refresh(job)
        return job

    def reset_attempts(self, job: Job) -> Job:
        """Zero the attempt counter, for a manual retry out of the DLQ."""
        job.attempts = 0
        job.updated_at = utcnow()
        self._db.commit()
        self._db.refresh(job)
        return job

    def touch(self, job: Job) -> Job:
        """Bump ``updated_at`` without changing anything else.

        This is the heartbeat a long-running job uses to hold off
        ``RecoveryService``'s visibility timeout.
        """
        job.updated_at = utcnow()
        self._db.commit()
        return job

    # -- reads -------------------------------------------------------------

    def get_by_id(self, job_id: uuid.UUID) -> Optional[Job]:
        return self._db.query(Job).filter(Job.id == job_id).first()

    def get_by_idempotency_key(self, key: str) -> Optional[Job]:
        return self._db.query(Job).filter(Job.idempotency_key == key).first()

    def list_by_status(
        self, status: Optional[JobStatus] = None, limit: int = 50
    ) -> list[Job]:
        query = self._db.query(Job)
        if status is not None:
            query = query.filter(Job.status == status)
        return query.order_by(Job.created_at.desc()).limit(limit).all()

    def list_stuck_running(self, cutoff: datetime, limit: int = 100) -> list[Job]:
        """Jobs still marked RUNNING but untouched since ``cutoff``.

        These are the jobs whose worker died without reporting an outcome. They
        are no longer in Redis (they were atomically popped), so nothing will
        ever pick them up again unless something goes looking. That is what
        ``RecoveryService`` uses this for.
        """
        return (
            self._db.query(Job)
            .filter(Job.status == JobStatus.RUNNING)
            .filter(Job.updated_at < cutoff)
            .order_by(Job.updated_at.asc())
            .limit(limit)
            .all()
        )

    def list_stale_pending(self, cutoff: datetime, limit: int = 100) -> list[Job]:
        """Jobs in a pending state that nothing has touched since ``cutoff``.

        QUEUED and FAILED are the two states where a job is waiting for Redis
        to hand it to a worker — QUEUED means it should be in the ready queue,
        FAILED means it should be in the delayed set. If the row is stale and
        the id is in neither, the enqueue was lost and the job is orphaned.

        The gap this closes: ``JobService.submit`` commits the row and *then*
        enqueues. A process death between those two statements leaves exactly
        this state, and nothing else in the system would ever notice.
        """
        return (
            self._db.query(Job)
            .filter(Job.status.in_((JobStatus.QUEUED, JobStatus.FAILED)))
            .filter(Job.updated_at < cutoff)
            .order_by(Job.updated_at.asc())
            .limit(limit)
            .all()
        )

    def count_by_status(self) -> dict[JobStatus, int]:
        """One aggregate query returning ``{JobStatus: count}`` for all statuses.

        A GROUP BY rather than five COUNT queries — the difference matters once
        the dashboard is polling this on every status change.
        """
        rows = (
            self._db.query(Job.status, func.count(Job.id)).group_by(Job.status).all()
        )
        counts: dict[JobStatus, int] = {member: 0 for member in JobStatus}
        for status_val, count in rows:
            counts[status_val] = count
        return counts

    # -- DAG queries -------------------------------------------------------

    def list_pending_dependents(self, completed_job_id: uuid.UUID) -> list[Job]:
        """All PENDING jobs that list ``completed_job_id`` in their ``depends_on``.

        Called by ``DagService`` after a job reaches SUCCESS. The result is the
        set of downstream jobs that *might* now be unblocked — DagService then
        checks each one to see if all its deps have succeeded.

        The ``ANY()`` operator works directly on Postgres ARRAY columns without
        needing a join table, which is why ``depends_on`` is stored as an array
        rather than a separate ``job_dependencies`` table. The tradeoff: querying
        "what are all the deps of job X?" is O(jobs) not O(deps), which is fine
        at this scale but would need a join table at millions of jobs.
        """
        return (
            self._db.query(Job)
            .filter(Job.status == JobStatus.PENDING)
            .filter(
                text(":job_id = ANY(depends_on)").bindparams(
                    job_id=str(completed_job_id)
                )
            )
            .all()
        )

    def get_many_by_ids(self, job_ids: list[uuid.UUID]) -> list[Job]:
        """Fetch multiple jobs by id in a single query.

        Used by ``DagService`` to load all dependency rows at once rather than
        N individual lookups.
        """
        if not job_ids:
            return []
        return self._db.query(Job).filter(Job.id.in_(job_ids)).all()
