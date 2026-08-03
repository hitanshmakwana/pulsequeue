"""Unit tests for QueueService — no database, no HTTP, no live Redis.

Every test here runs against a MagicMock client. That is only possible because
QueueService takes its Redis connection by injection rather than importing the
singleton, which is the concrete payoff of that design choice.
"""

import json
from unittest.mock import MagicMock

import pytest

from app.services.queue_service import (
    CHANNEL,
    DELAYED_KEY,
    DLQ_KEY,
    LOCK_PREFIX,
    PRIORITY_BAND,
    QUEUE_KEY,
    QueueService,
)


@pytest.fixture()
def redis_mock() -> MagicMock:
    mock = MagicMock()
    mock.register_script.return_value = MagicMock(return_value=0)
    return mock


@pytest.fixture()
def queue(redis_mock: MagicMock) -> QueueService:
    return QueueService(redis_mock)


# --- dequeue -------------------------------------------------------------


def test_enqueue_and_dequeue(redis_mock, queue):
    """Jobs enqueued should be dequeued correctly."""
    redis_mock.zpopmin.return_value = [("job-abc", 3.0)]
    assert queue.dequeue_nowait() == "job-abc"


def test_dequeue_empty_queue(redis_mock, queue):
    """Dequeue from an empty queue returns None."""
    redis_mock.zpopmin.return_value = []
    assert queue.dequeue_nowait() is None


def test_blocking_dequeue_unwraps_bzpopmin(redis_mock, queue):
    """BZPOPMIN returns (key, member, score); we want just the member."""
    redis_mock.bzpopmin.return_value = (QUEUE_KEY, "job-xyz", 3.0)
    assert queue.dequeue(timeout=1) == "job-xyz"
    redis_mock.bzpopmin.assert_called_once_with(QUEUE_KEY, timeout=1)


def test_blocking_dequeue_timeout_returns_none(redis_mock, queue):
    """A BZPOPMIN timeout yields None, not an exception."""
    redis_mock.bzpopmin.return_value = None
    assert queue.dequeue(timeout=1) is None


def test_dequeue_without_timeout_is_non_blocking(redis_mock, queue):
    """timeout=None must not block — it falls through to ZPOPMIN."""
    redis_mock.zpopmin.return_value = [("job-1", 3.0)]
    assert queue.dequeue() == "job-1"
    redis_mock.bzpopmin.assert_not_called()


# --- priority ordering ----------------------------------------------------


def test_higher_priority_scores_lower(queue):
    """Priority 1 must always sort ahead of priority 5 (ZPOPMIN pops lowest)."""
    assert queue._job_score(1) < queue._job_score(5)


def test_priority_dominates_age(queue):
    """A brand-new priority-1 job still beats a very old priority-2 job.

    Guards the PRIORITY_BAND width: if the band were too narrow, a sufficiently
    old low-priority job could overtake a high-priority one and quietly break
    the priority guarantee.
    """
    ancient_p2 = 2 * PRIORITY_BAND + 0  # epoch zero
    assert queue._job_score(1) < ancient_p2


def test_equal_priority_is_fifo(queue, monkeypatch):
    """Two jobs of equal priority are ordered by enqueue time, oldest first."""
    times = iter([1_700_000_000_000, 1_700_000_000_500])
    monkeypatch.setattr(
        "app.services.queue_service.epoch_ms", lambda: next(times)
    )
    first = queue._job_score(3)
    second = queue._job_score(3)
    assert first < second


def test_enqueue_writes_to_the_queue_key(redis_mock, queue):
    queue.enqueue("job-1", priority=2)
    key, mapping = redis_mock.zadd.call_args[0]
    assert key == QUEUE_KEY
    assert list(mapping) == ["job-1"]


# --- delayed retries ------------------------------------------------------


def test_enqueue_delayed_encodes_priority_in_the_member(redis_mock, queue):
    """The delayed set is ordered by time, so priority rides along in the member."""
    queue.enqueue_delayed("job-9", priority=4, delay_seconds=8)
    key, mapping = redis_mock.zadd.call_args[0]
    assert key == DELAYED_KEY
    assert list(mapping) == ["4:job-9"]


def test_enqueue_delayed_score_is_in_the_future(redis_mock, queue, monkeypatch):
    monkeypatch.setattr(
        "app.services.queue_service.epoch_ms", lambda: 1_700_000_000_000
    )
    queue.enqueue_delayed("job-9", priority=3, delay_seconds=2.5)
    _key, mapping = redis_mock.zadd.call_args[0]
    assert mapping["3:job-9"] == 1_700_000_000_000 + 2500


def test_promote_due_passes_both_keys_to_the_script(redis_mock, queue):
    """Promotion must be one atomic script over (delayed -> ready)."""
    queue.promote_due(limit=25)
    call = queue._promote_due.call_args
    assert call.kwargs["keys"] == [DELAYED_KEY, QUEUE_KEY]
    assert call.kwargs["args"][1] == 25


# --- membership -----------------------------------------------------------


def test_contains_finds_a_job_in_the_ready_queue(redis_mock, queue):
    redis_mock.zscore.side_effect = lambda key, _member: 1.0 if key == QUEUE_KEY else None
    assert queue.contains("job-1", priority=3) is True


def test_contains_finds_a_job_in_the_delayed_set(redis_mock, queue):
    """The delayed set keys members as '<priority>:<job_id>', so a naive
    ZSCORE on the bare id would miss every job waiting out a backoff — and the
    orphan sweep would then re-enqueue all of them."""
    def zscore(key, member):
        return 1.0 if key == DELAYED_KEY and member == "2:job-1" else None

    redis_mock.zscore.side_effect = zscore
    assert queue.contains("job-1", priority=2) is True


def test_contains_returns_false_when_absent_from_both(redis_mock, queue):
    redis_mock.zscore.return_value = None
    assert queue.contains("job-1", priority=3) is False


# --- dead-letter queue ----------------------------------------------------


def test_enqueue_dead_letter(redis_mock, queue):
    queue.enqueue_dead_letter("job-dead")
    redis_mock.lpush.assert_called_once_with(DLQ_KEY, "job-dead")


def test_remove_dead_letter(redis_mock, queue):
    """Manually retrying a job must take it off the DLQ."""
    redis_mock.lrem.return_value = 1
    assert queue.remove_dead_letter("job-dead") == 1
    redis_mock.lrem.assert_called_once_with(DLQ_KEY, 0, "job-dead")


# --- pub/sub --------------------------------------------------------------


def test_publish_update_format(redis_mock, queue):
    """publish_update should serialize correctly to the Pub/Sub channel."""
    queue.publish_update("job-123", "success")
    redis_mock.publish.assert_called_once_with(
        CHANNEL, json.dumps({"job_id": "job-123", "status": "success"})
    )


def test_publish_update_normalises_enum_status(redis_mock, queue):
    """Callers pass JobStatus members; the wire format must be the plain value."""
    from app.models.job import JobStatus

    queue.publish_update("job-123", JobStatus.DEAD_LETTER)
    _channel, payload = redis_mock.publish.call_args[0]
    assert json.loads(payload)["status"] == "dead_letter"


# --- distributed lock -----------------------------------------------------


def test_acquire_lock_uses_set_nx_ex(redis_mock, queue):
    """SET NX EX is the atomic form; anything else races."""
    redis_mock.set.return_value = True
    assert queue.acquire_lock("recovery", ttl_seconds=30, token="worker-a") is True
    redis_mock.set.assert_called_once_with(
        f"{LOCK_PREFIX}recovery", "worker-a", nx=True, ex=30
    )


def test_acquire_lock_returns_false_when_held(redis_mock, queue):
    redis_mock.set.return_value = None
    assert queue.acquire_lock("recovery", ttl_seconds=30, token="worker-b") is False


def test_release_lock_is_owner_checked(redis_mock, queue):
    """Release goes through a compare-and-delete script, never a bare DEL."""
    queue._release_lock = MagicMock(return_value=1)
    assert queue.release_lock("recovery", "worker-a") is True
    assert queue._release_lock.call_args.kwargs["args"] == ["worker-a"]
    redis_mock.delete.assert_not_called()
