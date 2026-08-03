"""Unit tests for JobService — business logic with no database and no Redis.

FR13 requires that idempotency logic be unit-testable without live
infrastructure. These tests are the evidence for that claim.
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from app.core.exceptions import (
    DuplicateIdempotencyKey,
    InvalidStateTransition,
    JobNotFound,
)
from app.models.job import JobStatus
from app.schemas.job import JobCreate
from app.services.job_service import JobService


@pytest.fixture()
def queue() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def repo() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def svc(repo, queue) -> JobService:
    """JobService with its repository swapped for a mock.

    JobService constructs its own JobRepository from the session, so the patch
    intercepts that construction. Everything below the service boundary is then
    a mock, which is what makes this a unit test rather than an integration one.
    """
    with patch("app.services.job_service.JobRepository", return_value=repo):
        yield JobService(db=MagicMock(), queue=queue)


def make_job(**overrides) -> MagicMock:
    job = MagicMock()
    job.id = overrides.get("id", uuid.uuid4())
    job.priority = overrides.get("priority", 3)
    job.status = overrides.get("status", JobStatus.QUEUED)
    job.attempts = overrides.get("attempts", 0)
    return job


# --- submit ---------------------------------------------------------------


def test_submit_persists_then_enqueues(svc, repo, queue):
    """Order matters: commit first, enqueue second.

    The reverse would put an id on the queue that no worker could resolve to a
    row if the process died in between.
    """
    job = make_job(priority=2)
    repo.create.return_value = job

    assert svc.submit(JobCreate(job_type="send_email", priority=2)) is job
    queue.enqueue.assert_called_once_with(str(job.id), 2)


def test_submit_applies_the_configured_default_max_attempts(svc, repo, monkeypatch):
    """Omitting max_attempts must fall back to the operator's setting."""
    monkeypatch.setattr("app.services.job_service.settings.max_retry_attempts", 7)
    repo.create.return_value = make_job()

    svc.submit(JobCreate(job_type="send_email"))

    assert repo.create.call_args.kwargs["max_attempts"] == 7


def test_submit_honours_an_explicit_max_attempts(svc, repo):
    repo.create.return_value = make_job()
    svc.submit(JobCreate(job_type="send_email", max_attempts=1))
    assert repo.create.call_args.kwargs["max_attempts"] == 1


# --- idempotency ----------------------------------------------------------


def test_duplicate_key_returns_the_existing_job_without_re_executing(svc, repo, queue):
    """FR6: the same idempotency key must never produce a second execution."""
    existing = make_job()
    repo.get_by_idempotency_key.return_value = existing

    result = svc.submit(JobCreate(job_type="send_email", idempotency_key="k-1"))

    assert result is existing
    repo.create.assert_not_called()
    queue.enqueue.assert_not_called()  # critically: not re-queued


def test_no_idempotency_key_skips_the_lookup(svc, repo):
    repo.create.return_value = make_job()
    svc.submit(JobCreate(job_type="send_email"))
    repo.get_by_idempotency_key.assert_not_called()


def test_concurrent_duplicate_submission_returns_the_winner(svc, repo, queue):
    """The pre-check is check-then-act and therefore racy.

    Two concurrent requests with the same key can both see "no existing job".
    Only the unique constraint can settle it, so the loser must catch the
    violation and return the winner's row rather than surfacing a 500.
    """
    winner = make_job()
    # First lookup (pre-check) finds nothing; the INSERT loses the race; the
    # second lookup finds the row the winner committed.
    repo.get_by_idempotency_key.side_effect = [None, winner]
    repo.create.side_effect = DuplicateIdempotencyKey("duplicate")

    result = svc.submit(JobCreate(job_type="send_email", idempotency_key="k-race"))

    assert result is winner
    queue.enqueue.assert_not_called()


def test_integrity_error_with_no_winner_row_propagates(svc, repo):
    """If the constraint fired for some other reason, do not swallow it."""
    repo.get_by_idempotency_key.side_effect = [None, None]
    repo.create.side_effect = DuplicateIdempotencyKey("duplicate")

    with pytest.raises(DuplicateIdempotencyKey):
        svc.submit(JobCreate(job_type="send_email", idempotency_key="k-x"))


# --- manual retry ---------------------------------------------------------


def test_manual_retry_requeues_a_dead_lettered_job(svc, repo, queue):
    job = make_job(status=JobStatus.DEAD_LETTER, attempts=3, priority=1)
    repo.get_by_id.return_value = job
    repo.reset_attempts.return_value = job
    repo.update_status.return_value = job

    svc.manual_retry(job.id)

    # The counter reset must go through the repository so it is committed —
    # assigning job.attempts = 0 in the service would never reach the database.
    repo.reset_attempts.assert_called_once_with(job)
    repo.update_status.assert_called_once_with(job, JobStatus.QUEUED, result=None)
    queue.enqueue.assert_called_once_with(str(job.id), 1)


def test_manual_retry_removes_the_job_from_the_dlq(svc, repo, queue):
    """Otherwise DLQ depth stops meaning 'things needing human attention'."""
    job = make_job(status=JobStatus.DEAD_LETTER)
    repo.get_by_id.return_value = job
    repo.reset_attempts.return_value = job
    repo.update_status.return_value = job

    svc.manual_retry(job.id)

    queue.remove_dead_letter.assert_called_once_with(str(job.id))


def test_manual_retry_rejects_a_missing_job(svc, repo):
    repo.get_by_id.return_value = None
    with pytest.raises(JobNotFound):
        svc.manual_retry(uuid.uuid4())


@pytest.mark.parametrize(
    "status",
    [JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.SUCCESS, JobStatus.FAILED],
)
def test_manual_retry_rejects_non_dead_lettered_jobs(svc, repo, queue, status):
    """Re-queueing a RUNNING job double-processes it; a SUCCESS job re-fires a
    side effect that already happened. Only DEAD_LETTER is a legal source state."""
    repo.get_by_id.return_value = make_job(status=status)

    with pytest.raises(InvalidStateTransition):
        svc.manual_retry(uuid.uuid4())

    queue.enqueue.assert_not_called()
