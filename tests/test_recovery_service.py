"""Unit tests for RecoveryService — the crash-recovery guarantee.

The PRD promises that no job is silently lost when a worker dies. Because
BZPOPMIN removes the job from Redis the moment it is taken, that promise rests
entirely on this sweep. These tests are what make the claim defensible.
"""

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from app.core.clock import utcnow
from app.models.job import JobStatus
from app.services.recovery_service import RecoveryService


@pytest.fixture()
def repo() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def queue() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def svc(repo, queue) -> RecoveryService:
    return RecoveryService(repo, queue)


def make_job(attempts: int = 1, max_attempts: int = 3, priority: int = 3) -> MagicMock:
    job = MagicMock()
    job.id = "22222222-2222-2222-2222-222222222222"
    job.attempts = attempts
    job.max_attempts = max_attempts
    job.priority = priority
    job.updated_at = utcnow() - timedelta(minutes=10)
    return job


def test_no_stuck_jobs_is_a_no_op(svc, repo, queue):
    repo.list_stuck_running.return_value = []
    assert svc.requeue_stuck_jobs() == []
    queue.enqueue.assert_not_called()


def test_abandoned_job_is_returned_to_the_queue(svc, repo, queue):
    """The core guarantee: a job orphaned by a dead worker gets picked up again."""
    job = make_job(attempts=1, max_attempts=3, priority=2)
    repo.list_stuck_running.return_value = [job]

    recovered = svc.requeue_stuck_jobs()

    assert recovered == [job.id]
    assert repo.update_status.call_args[0][1] is JobStatus.QUEUED
    queue.enqueue.assert_called_once_with(str(job.id), 2)


def test_recovery_uses_the_configured_visibility_timeout(svc, repo, monkeypatch):
    monkeypatch.setattr(
        "app.services.recovery_service.settings.visibility_timeout", 120
    )
    repo.list_stuck_running.return_value = []
    before = utcnow()

    svc.requeue_stuck_jobs()

    cutoff = repo.list_stuck_running.call_args[0][0]
    # Cutoff must be ~120s in the past; anything newer would reclaim healthy
    # long-running jobs and duplicate work.
    assert timedelta(seconds=115) <= (before - cutoff) <= timedelta(seconds=125)


def test_explicit_timeout_overrides_the_setting(svc, repo):
    repo.list_stuck_running.return_value = []
    before = utcnow()
    svc.requeue_stuck_jobs(visibility_timeout=10)
    cutoff = repo.list_stuck_running.call_args[0][0]
    assert (before - cutoff) < timedelta(seconds=15)


def test_poison_pill_is_dead_lettered_rather_than_requeued(svc, repo, queue):
    """A job that reliably kills its worker must not crash-loop the fleet.

    The attempt is consumed before the handler runs, so a job that takes down
    three workers has exhausted its budget and is dead-lettered instead of
    being handed to a fourth.
    """
    job = make_job(attempts=3, max_attempts=3)
    repo.list_stuck_running.return_value = [job]

    svc.requeue_stuck_jobs()

    assert repo.update_status.call_args[0][1] is JobStatus.DEAD_LETTER
    queue.enqueue_dead_letter.assert_called_once_with(str(job.id))
    queue.enqueue.assert_not_called()


def test_recovery_records_why_the_job_moved(svc, repo):
    """The result column must explain the transition — otherwise a recovered
    job looks identical to one that was never touched."""
    repo.list_stuck_running.return_value = [make_job()]
    svc.requeue_stuck_jobs()
    result = repo.update_status.call_args.kwargs["result"]
    assert "Worker lost" in result["error"]


def test_recovery_publishes_updates_to_the_dashboard(svc, repo, queue):
    job = make_job()
    repo.list_stuck_running.return_value = [job]
    svc.requeue_stuck_jobs()
    queue.publish_update.assert_called_once_with(str(job.id), JobStatus.QUEUED)


def test_sweep_is_bounded(svc, repo):
    """One sweep after a large outage must not monopolise a worker."""
    repo.list_stuck_running.return_value = []
    svc.requeue_stuck_jobs(limit=25)
    assert repo.list_stuck_running.call_args.kwargs["limit"] == 25


def test_mixed_batch_handles_each_job_on_its_own_merits(svc, repo, queue):
    repo.list_stuck_running.return_value = [
        make_job(attempts=1, max_attempts=3),
        make_job(attempts=3, max_attempts=3),
    ]

    recovered = svc.requeue_stuck_jobs()

    assert len(recovered) == 2
    assert queue.enqueue.call_count == 1
    assert queue.enqueue_dead_letter.call_count == 1


# --- orphaned pending jobs -------------------------------------------------
#
# The second orphan path: JobService.submit commits the row and then enqueues.
# There is no transaction across Postgres and Redis, so a process death between
# those two statements leaves a row nothing will ever pick up.


def test_orphaned_queued_job_is_re_enqueued(svc, repo, queue):
    job = make_job(priority=4)
    job.status = JobStatus.QUEUED
    repo.list_stale_pending.return_value = [job]
    queue.contains.return_value = False  # absent from Redis == orphaned

    assert svc.requeue_orphaned_jobs() == [job.id]
    queue.enqueue.assert_called_once_with(str(job.id), 4)


def test_backlogged_job_still_in_redis_is_left_alone(svc, repo, queue):
    """The check that stops this sweep from duplicating a slow queue.

    A deep backlog and an orphaned job look identical in the database — both
    are old rows in QUEUED. Only Redis can tell them apart, so age alone must
    never be enough to trigger a re-enqueue.
    """
    job = make_job()
    job.status = JobStatus.QUEUED
    repo.list_stale_pending.return_value = [job]
    queue.contains.return_value = True  # genuinely waiting its turn

    assert svc.requeue_orphaned_jobs() == []
    queue.enqueue.assert_not_called()


def test_orphaned_failed_job_is_moved_back_to_queued(svc, repo, queue):
    """A FAILED job lost from the delayed set must return to QUEUED, not stay
    FAILED — otherwise the status would misreport it as still backing off."""
    job = make_job()
    job.status = JobStatus.FAILED
    repo.list_stale_pending.return_value = [job]
    queue.contains.return_value = False

    svc.requeue_orphaned_jobs()

    assert repo.update_status.call_args[0][1] is JobStatus.QUEUED
    queue.enqueue.assert_called_once()


def test_already_queued_orphan_is_not_status_updated(svc, repo, queue):
    """No pointless write when the status is already correct."""
    job = make_job()
    job.status = JobStatus.QUEUED
    repo.list_stale_pending.return_value = [job]
    queue.contains.return_value = False

    svc.requeue_orphaned_jobs()

    repo.update_status.assert_not_called()


def test_orphan_sweep_with_nothing_stale_is_a_no_op(svc, repo, queue):
    repo.list_stale_pending.return_value = []
    assert svc.requeue_orphaned_jobs() == []
    queue.contains.assert_not_called()


def test_orphan_sweep_respects_the_visibility_timeout(svc, repo):
    """Only jobs older than the timeout are candidates — a job enqueued one
    second ago has simply not been picked up yet."""
    repo.list_stale_pending.return_value = []
    before = utcnow()
    svc.requeue_orphaned_jobs(visibility_timeout=30)
    cutoff = repo.list_stale_pending.call_args[0][0]
    assert timedelta(seconds=25) <= (before - cutoff) <= timedelta(seconds=35)
