"""DagService — dependency resolution for the job DAG.

Why this file exists:
    The question "should this PENDING job become QUEUED now?" is a business
    decision. It lives here, not in the worker (which should stay a thin
    executor) and not in JobService (which owns submission, not execution
    lifecycle).

    The key insight: after any job reaches SUCCESS, some other jobs may become
    unblocked. DagService answers "which ones?" and transitions them to QUEUED.
    The worker calls ``resolve_dependents(job_id)`` after every successful
    completion; the service handles the rest.

Algorithm
---------
    1. Find all PENDING jobs that list ``completed_job_id`` in ``depends_on``.
    2. For each candidate, load all its dependency rows.
    3. If every dependency has status SUCCESS, transition the candidate to
       QUEUED and enqueue it.
    4. If any dependency has status DEAD_LETTER or CANCELLED, the candidate
       can never run — dead-letter it immediately rather than leaving it
       PENDING forever.

    Cycles are impossible at runtime because they are rejected at submit time
    by ``validate_no_cycle()``. So step 3 will always terminate.

Who owns this:
    ``DagService`` owns the PENDING → QUEUED transition exclusively. Nothing
    else should write that transition.
"""

import logging
import uuid

from app.models.job import Job, JobStatus
from app.repositories.job_repository import JobRepository
from app.services.queue_service import QueueService

log = logging.getLogger(__name__)

# Terminal statuses that cannot recover — if a dependency reaches one of these,
# dependent jobs that are waiting on it can never run.
_BLOCKING_STATUSES = {JobStatus.DEAD_LETTER}


class DagService:
    def __init__(self, repo: JobRepository, queue: QueueService):
        self._repo = repo
        self._queue = queue

    def resolve_dependents(self, completed_job_id: uuid.UUID) -> list[uuid.UUID]:
        """Check downstream jobs and unblock any that are now fully satisfied.

        Called by the worker immediately after a job reaches SUCCESS. This is
        the fan-out step: one completion may unblock many downstream jobs.

        Returns:
            IDs of jobs that were transitioned from PENDING → QUEUED.
        """
        candidates = self._repo.list_pending_dependents(completed_job_id)
        if not candidates:
            return []

        unblocked: list[uuid.UUID] = []
        for candidate in candidates:
            result = self._check_and_unblock(candidate)
            if result:
                unblocked.append(candidate.id)

        if unblocked:
            log.info(
                "Job %s completion unblocked %d downstream job(s): %s",
                completed_job_id,
                len(unblocked),
                unblocked,
            )
        return unblocked

    def _check_and_unblock(self, job: Job) -> bool:
        """Evaluate a single PENDING job against its dependency statuses.

        Returns True if the job was transitioned to QUEUED, False otherwise.
        """
        if not job.depends_on:
            # No deps at all — should not be PENDING, but handle defensively.
            self._transition_to_queued(job)
            return True

        dep_jobs = self._repo.get_many_by_ids(job.depends_on)
        dep_map: dict[uuid.UUID, Job] = {d.id: d for d in dep_jobs}

        # Check for blocking deps first (dead-lettered dependencies mean this
        # job can never run — fail it immediately rather than leaving it stuck).
        for dep_id in job.depends_on:
            dep = dep_map.get(dep_id)
            if dep is None:
                log.error(
                    "Job %s has missing dependency %s — dead-lettering",
                    job.id,
                    dep_id,
                )
                self._transition_to_dead_letter(
                    job, f"Dependency {dep_id} does not exist"
                )
                return False
            if dep.status in _BLOCKING_STATUSES:
                log.error(
                    "Job %s dependency %s reached %s — dead-lettering dependent",
                    job.id,
                    dep_id,
                    dep.status.value,
                )
                self._transition_to_dead_letter(
                    job,
                    f"Dependency {dep_id} reached terminal failure state "
                    f"'{dep.status.value}'",
                )
                return False

        # All deps must be SUCCESS to unblock.
        all_succeeded = all(
            dep_map.get(dep_id, None) is not None
            and dep_map[dep_id].status == JobStatus.SUCCESS
            for dep_id in job.depends_on
        )

        if all_succeeded:
            self._transition_to_queued(job)
            return True

        # Some deps still in progress — nothing to do yet.
        return False

    def _transition_to_queued(self, job: Job) -> None:
        self._repo.update_status(job, JobStatus.QUEUED)
        self._queue.enqueue(str(job.id), job.priority)
        self._queue.publish_update(str(job.id), JobStatus.QUEUED)
        log.info(
            "DAG: job %s unblocked — transitioned PENDING → QUEUED", job.id
        )

    def _transition_to_dead_letter(self, job: Job, reason: str) -> None:
        self._repo.update_status(
            job,
            JobStatus.DEAD_LETTER,
            result={"error": reason, "reason": "dependency_failure", "final": True},
        )
        self._queue.enqueue_dead_letter(str(job.id))
        self._queue.publish_update(str(job.id), JobStatus.DEAD_LETTER)

    # -- cycle detection (called at submit time) ---------------------------

    def validate_no_cycle(
        self, new_job_depends_on: list[uuid.UUID], all_jobs_deps: dict[uuid.UUID, list[uuid.UUID]]
    ) -> bool:
        """Return True if adding ``new_job_depends_on`` would NOT create a cycle.

        Uses iterative DFS from each direct dependency. Because the new job's
        id is not yet assigned, we treat it as a virtual root and walk its
        would-be dependency subtree looking for any node that would point back
        to a job whose subtree includes the new job's deps.

        In practice, since all deps must already exist in the database and the
        new job has no id yet (it's not inserted), a cycle can only arise if
        existing jobs transitively depend on something in ``new_job_depends_on``.
        The check is: can we reach any of ``new_job_depends_on`` by starting
        from any of them and following the dependency graph?

        This is O(V + E) over the reachable subgraph.
        """
        if not new_job_depends_on:
            return True

        # Build a set of all dep IDs for O(1) lookup
        dep_set = set(new_job_depends_on)

        # DFS from each dependency, following the existing graph
        visited: set[uuid.UUID] = set()
        stack = list(new_job_depends_on)

        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)

            children = all_jobs_deps.get(current, [])
            for child in children:
                if child in dep_set:
                    # The graph already has a path from a dep back into our dep
                    # set — adding this job would close a cycle.
                    return False
                if child not in visited:
                    stack.append(child)

        return True
