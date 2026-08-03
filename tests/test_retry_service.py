"""Unit tests for RetryService — isolated from worker, queue, database and HTTP.

The whole point of extracting retry policy into its own service is that these
tests need no infrastructure at all. If any test in this file required a
running Redis or Postgres, the extraction would not have worked.
"""

from unittest.mock import MagicMock

import pytest

from app.models.job import JobStatus
from app.services.retry_service import RetryService


@pytest.fixture()
def repo() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def queue() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def svc(repo, queue) -> RetryService:
    return RetryService(repo, queue)


def make_job(attempts: int, max_attempts: int = 3, priority: int = 3) -> MagicMock:
    job = MagicMock()
    job.id = "11111111-1111-1111-1111-111111111111"
    job.attempts = attempts
    job.max_attempts = max_attempts
    job.priority = priority
    return job


# --- backoff maths --------------------------------------------------------


def test_compute_delay_exponential(svc):
    """Retry delay should double each attempt."""
    assert svc.compute_delay(0) == 2
    assert svc.compute_delay(1) == 4
    assert svc.compute_delay(2) == 8


def test_compute_delay_respects_configured_base(svc, monkeypatch):
    monkeypatch.setattr("app.services.retry_service.settings.base_retry_delay", 5)
    assert svc.compute_delay(0) == 5
    assert svc.compute_delay(3) == 40


# --- dead-letter threshold ------------------------------------------------


def test_should_dead_letter_when_attempts_exhausted(svc):
    assert svc.should_dead_letter(make_job(attempts=3, max_attempts=3)) is True


def test_should_not_dead_letter_when_retries_remain(svc):
    assert svc.should_dead_letter(make_job(attempts=1, max_attempts=3)) is False


def test_should_dead_letter_when_attempts_overshoot(svc):
    """Defensive: >= not ==, so a miscount can never produce an infinite loop."""
    assert svc.should_dead_letter(make_job(attempts=9, max_attempts=3)) is True


# --- the retry branch -----------------------------------------------------


def test_handle_failure_schedules_a_delayed_retry(svc, repo, queue):
    """A retryable failure goes to the delayed set, not back to the ready queue.

    Enqueueing it directly would make the retry immediate, defeating the
    backoff entirely.
    """
    job = make_job(attempts=1, max_attempts=3, priority=2)

    assert svc.handle_failure(job, ValueError("smtp down")) is JobStatus.FAILED

    repo.update_status.assert_called_once()
    assert repo.update_status.call_args[0][1] is JobStatus.FAILED
    queue.enqueue_delayed.assert_called_once_with(str(job.id), 2, 2.0)
    queue.enqueue.assert_not_called()
    queue.enqueue_dead_letter.assert_not_called()


def test_first_retry_uses_the_base_delay(svc, queue):
    """attempts is incremented before execution, so the first failure has
    attempts == 1 — and must still back off by the base delay, not 2x it."""
    svc.handle_failure(make_job(attempts=1), RuntimeError("boom"))
    assert queue.enqueue_delayed.call_args[0][2] == 2.0


def test_second_retry_doubles_the_delay(svc, queue):
    svc.handle_failure(make_job(attempts=2, max_attempts=5), RuntimeError("boom"))
    assert queue.enqueue_delayed.call_args[0][2] == 4.0


def test_handle_failure_never_sleeps(svc, monkeypatch):
    """The worker must not be blocked for the duration of the backoff.

    Locks in the delayed-set design: if someone reintroduces time.sleep here,
    one slow-failing job would idle an entire worker process.
    """
    def explode(_seconds):
        raise AssertionError("RetryService must not block the worker")

    monkeypatch.setattr("time.sleep", explode)
    svc.handle_failure(make_job(attempts=1), RuntimeError("boom"))


def test_retry_records_the_error_and_next_delay(svc, repo):
    svc.handle_failure(make_job(attempts=1), ValueError("smtp down"))
    result = repo.update_status.call_args.kwargs["result"]
    assert result["error"] == "smtp down"
    assert result["attempt"] == 1
    assert result["retry_in"] == 2.0


# --- the dead-letter branch -----------------------------------------------


def test_handle_failure_dead_letters_when_exhausted(svc, repo, queue):
    job = make_job(attempts=3, max_attempts=3)

    assert svc.handle_failure(job, ValueError("still down")) is JobStatus.DEAD_LETTER

    assert repo.update_status.call_args[0][1] is JobStatus.DEAD_LETTER
    assert repo.update_status.call_args.kwargs["result"]["final"] is True
    queue.enqueue_dead_letter.assert_called_once_with(str(job.id))
    queue.enqueue_delayed.assert_not_called()


def test_permanent_failure_skips_retries_entirely(svc, repo, queue):
    """An unregistered job_type cannot be fixed by waiting.

    Retrying it three times only delays the real error by 14 seconds and buries
    it under retry noise.
    """
    job = make_job(attempts=1, max_attempts=3)

    status = svc.handle_failure(job, KeyError("no handler"), permanent=True)

    assert status is JobStatus.DEAD_LETTER
    queue.enqueue_delayed.assert_not_called()
    queue.enqueue_dead_letter.assert_called_once()


def test_dead_letter_publishes_an_update(svc, queue):
    job = make_job(attempts=3)
    svc.handle_failure(job, ValueError("gone"))
    queue.publish_update.assert_called_once_with(str(job.id), JobStatus.DEAD_LETTER)
