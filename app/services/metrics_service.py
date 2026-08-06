"""MetricsService — read-only aggregation.

Why this file exists:
    Counting jobs by status is a business query, not an HTTP concern. Both the
    REST endpoint and any Prometheus exporter read through this one service, so
    there is never a second, subtly different definition of "how many jobs are
    running".

Who owns this:
    ``MetricsService`` owns all read-only aggregation. It wraps
    ``JobRepository`` and returns a schema object. Routers never compute counts.
"""

from sqlalchemy.orm import Session

from app.models.job import JobStatus
from app.repositories.job_repository import JobRepository
from app.schemas.job import StatsResponse


class MetricsService:
    def __init__(self, db: Session):
        self._repo = JobRepository(db)

    def get_stats(self) -> StatsResponse:
        """Current job counts, one per status.

        Note this reports Postgres state, which is the source of truth, rather
        than Redis queue depth. A job can be QUEUED in Postgres while sitting
        in the delayed-retry set rather than the ready queue; the database view
        is the one that answers "what is the system actually holding".

        PENDING is the DAG-blocked state: jobs that are waiting for upstream
        dependencies to complete before they can be queued.
        """
        counts = self._repo.count_by_status()
        return StatsResponse(
            pending=counts.get(JobStatus.PENDING, 0),
            queued=counts.get(JobStatus.QUEUED, 0),
            running=counts.get(JobStatus.RUNNING, 0),
            success=counts.get(JobStatus.SUCCESS, 0),
            failed=counts.get(JobStatus.FAILED, 0),
            dead_letter=counts.get(JobStatus.DEAD_LETTER, 0),
        )
