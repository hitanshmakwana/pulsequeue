"""RetryService — the single owner of retry policy.

Why this file exists:
    "Retry with backoff, or give up?" is a business decision, not an execution
    detail. Embedded in the worker, it would mean the worker has two jobs
    (execute work, decide policy) and that testing the backoff maths requires
    booting a worker, a Redis, and a Postgres.

    Pulled out here, ``compute_delay`` and ``should_dead_letter`` are pure
    functions of their inputs, and ``handle_failure`` is testable against two
    mocks. The worker calls ``handle_failure(job, exc)`` and does nothing else.

Who owns this:
    ``RetryService`` owns backoff calculation and the retry-versus-dead-letter
    decision, exclusively. Workers must never compute a delay or inspect an
    attempt count themselves.
"""

import logging

from app.core.config import settings
from app.models.job import Job, JobStatus
from app.repositories.job_repository import JobRepository
from app.services.queue_service import QueueService

log = logging.getLogger(__name__)


class RetryService:
    def __init__(self, repo: JobRepository, queue: QueueService):
        self._repo = repo
        self._queue = queue

    # -- policy (pure, no I/O) --------------------------------------------

    def compute_delay(self, attempt: int) -> float:
        """Exponential backoff: ``base * 2 ** attempt``.

        With the default base of 2s: attempt 0 -> 2s, 1 -> 4s, 2 -> 8s.

        Why exponential rather than a fixed interval? A transient dependency
        failure — a database failover, a rate limit, a restarting service —
        typically resolves on a timescale of seconds to tens of seconds.
        Backing off geometrically gives it room to recover, whereas retrying
        every 2s indefinitely keeps the failing service pinned under load from
        the very clients waiting for it to come back.

        ``attempt`` is zero-based: pass the number of failures that have
        *already* happened before this one.
        """
        return float(settings.base_retry_delay * (2**attempt))

    def should_dead_letter(self, job: Job) -> bool:
        """True once the job has consumed its whole attempt budget.

        ``attempts`` is incremented before each execution, so after the Nth
        failure ``attempts == N``. With ``max_attempts=3`` the job runs exactly
        three times and then dead-letters.
        """
        return job.attempts >= job.max_attempts

    # -- the decision ------------------------------------------------------

    def handle_failure(
        self, job: Job, exc: Exception, permanent: bool = False
    ) -> JobStatus:
        """Record a failed execution and decide what happens next.

        Args:
            job: The job whose handler raised.
            exc: The exception it raised.
            permanent: Set when retrying provably cannot help — currently only
                "no handler is registered for this job_type". Backing off three
                times before admitting a typo'd job type is pure latency, and
                it buries the real error under retry noise.

        Returns:
            The status the job was moved to: ``DEAD_LETTER`` or ``FAILED``.
            Returned so the worker can log the outcome without re-deriving it.
        """
        if permanent or self.should_dead_letter(job):
            reason = (
                "unretryable error" if permanent else f"exhausted {job.attempts} attempts"
            )
            log.error("Job %s dead-lettered — %s: %s", job.id, reason, exc)
            self._repo.update_status(
                job,
                JobStatus.DEAD_LETTER,
                result={"error": str(exc), "reason": reason, "final": True},
            )
            self._queue.enqueue_dead_letter(str(job.id))
            self._queue.publish_update(str(job.id), JobStatus.DEAD_LETTER)
            return JobStatus.DEAD_LETTER

        # `attempts` was already incremented for the execution that just
        # failed, so the first failure has attempts == 1. Subtracting one keeps
        # the first backoff at the configured base delay (2s, 4s, 8s) rather
        # than starting a rung too high (4s, 8s, 16s).
        delay = self.compute_delay(max(job.attempts - 1, 0))

        log.warning(
            "Job %s failed (attempt %d/%d) — retrying in %.1fs: %s",
            job.id,
            job.attempts,
            job.max_attempts,
            delay,
            exc,
        )
        self._repo.update_status(
            job,
            JobStatus.FAILED,
            result={
                "error": str(exc),
                "attempt": job.attempts,
                "retry_in": delay,
            },
        )
        self._queue.publish_update(str(job.id), JobStatus.FAILED)

        # Hand the job to the delayed set and return immediately. The worker is
        # free to pick up other work; whichever worker's next loop finds this
        # job due will promote it back into the ready queue.
        #
        # The job stays in FAILED until it is actually picked up again, which
        # is honest: "failed, awaiting retry at <retry_in>" is more information
        # than flipping it straight back to QUEUED would convey.
        self._queue.enqueue_delayed(str(job.id), job.priority, delay)
        return JobStatus.FAILED
