"""Integration tests for the REST API.

These run against a real PostgreSQL (Redis is mocked — the API's contract with
Redis is "enqueue was called", which the mock asserts precisely). Start the
dependencies with::

    docker compose up -d postgres redis
"""

import uuid

import pytest

from app.models.job import JobStatus

pytestmark = pytest.mark.integration


# --- health ---------------------------------------------------------------


def test_health(client):
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json() == {"status": "ok"}


# --- submit ---------------------------------------------------------------


def test_submit_job(client):
    """Submitting a valid job should return 201 with status=queued."""
    res = client.post(
        "/jobs", json={"job_type": "send_email", "payload": {"to": "a@b.com"}}
    )
    assert res.status_code == 201
    data = res.json()
    assert data["status"] == "queued"
    assert data["job_type"] == "send_email"
    assert data["attempts"] == 0
    assert "id" in data


def test_submit_enqueues_onto_redis(client, fake_redis):
    """The router must actually hand the job to the queue, not just persist it."""
    res = client.post("/jobs", json={"job_type": "send_email"})
    job_id = res.json()["id"]
    enqueued = [
        call.args[1] for call in fake_redis.zadd.call_args_list if len(call.args) > 1
    ]
    assert any(job_id in mapping for mapping in enqueued)


def test_submit_applies_default_priority_and_max_attempts(client):
    data = client.post("/jobs", json={"job_type": "send_email"}).json()
    assert data["priority"] == 3
    assert data["max_attempts"] == 3


@pytest.mark.parametrize(
    "body",
    [
        {"job_type": ""},  # empty type
        {"job_type": "send_email", "priority": 0},  # below range
        {"job_type": "send_email", "priority": 6},  # above range
        {"job_type": "send_email", "max_attempts": 0},  # below range
        {"payload": {}},  # missing required job_type
    ],
)
def test_submit_rejects_invalid_bodies(client, body):
    """Validation belongs to the schema, not to hand-written checks in routes."""
    assert client.post("/jobs", json=body).status_code == 422


# --- idempotency ----------------------------------------------------------


def test_idempotency(client):
    """Same idempotency key should return the same job."""
    key = f"unique-key-{uuid.uuid4()}"
    body = {"job_type": "send_email", "payload": {}, "idempotency_key": key}
    res1 = client.post("/jobs", json=body)
    res2 = client.post("/jobs", json=body)
    assert res1.json()["id"] == res2.json()["id"]


def test_idempotent_resubmission_does_not_re_enqueue(client, fake_redis):
    """FR6 is about execution, not just about the response body.

    Returning the same id while quietly enqueueing a second copy would still
    run the work twice.
    """
    key = f"unique-key-{uuid.uuid4()}"
    body = {"job_type": "send_email", "idempotency_key": key}
    client.post("/jobs", json=body)
    fake_redis.zadd.reset_mock()
    client.post("/jobs", json=body)
    fake_redis.zadd.assert_not_called()


def test_different_keys_create_different_jobs(client):
    res1 = client.post(
        "/jobs", json={"job_type": "send_email", "idempotency_key": str(uuid.uuid4())}
    )
    res2 = client.post(
        "/jobs", json={"job_type": "send_email", "idempotency_key": str(uuid.uuid4())}
    )
    assert res1.json()["id"] != res2.json()["id"]


# --- fetch ----------------------------------------------------------------


def test_get_job_roundtrip(client):
    created = client.post(
        "/jobs", json={"job_type": "resize_image", "payload": {"width": 42}}
    ).json()
    fetched = client.get(f"/jobs/{created['id']}").json()
    assert fetched["id"] == created["id"]
    assert fetched["job_type"] == "resize_image"


def test_get_nonexistent_job(client):
    """Fetching a non-existent job should return 404."""
    assert client.get(f"/jobs/{uuid.uuid4()}").status_code == 404


def test_get_job_with_malformed_id(client):
    assert client.get("/jobs/not-a-uuid").status_code == 422


# --- list -----------------------------------------------------------------


def test_list_jobs(client):
    client.post("/jobs", json={"job_type": "send_email"})
    res = client.get("/jobs")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


def test_list_filters_by_status(client):
    client.post("/jobs", json={"job_type": "send_email"})
    res = client.get("/jobs", params={"status": "queued"})
    assert res.status_code == 200
    assert all(job["status"] == "queued" for job in res.json())


def test_list_rejects_an_unknown_status(client):
    assert client.get("/jobs", params={"status": "banana"}).status_code == 422


def test_list_respects_limit(client):
    for _ in range(3):
        client.post("/jobs", json={"job_type": "send_email"})
    assert len(client.get("/jobs", params={"limit": 2}).json()) == 2


# --- stats ----------------------------------------------------------------


def test_stats_shape(client):
    res = client.get("/jobs/stats")
    assert res.status_code == 200
    assert set(res.json()) == {"queued", "running", "success", "failed", "dead_letter"}


def test_stats_counts_a_submitted_job(client, clean_jobs_table):
    client.post("/jobs", json={"job_type": "send_email"})
    assert client.get("/jobs/stats").json()["queued"] == 1


def test_stats_route_is_not_shadowed_by_the_job_id_route(client):
    """/jobs/stats must resolve to the stats handler, not to /jobs/{job_id}."""
    assert client.get("/jobs/stats").status_code == 200


# --- manual retry ---------------------------------------------------------


def test_manual_retry_of_a_queued_job_is_rejected(client):
    """Only dead-lettered jobs may be manually retried (409, not 500)."""
    job_id = client.post("/jobs", json={"job_type": "send_email"}).json()["id"]
    res = client.post(f"/jobs/{job_id}/retry")
    assert res.status_code == 409
    assert "dead-lettered" in res.json()["detail"]


def test_manual_retry_of_a_missing_job_is_404(client):
    assert client.post(f"/jobs/{uuid.uuid4()}/retry").status_code == 404


def test_manual_retry_revives_a_dead_lettered_job(client, db_session):
    """The full DLQ recovery path an ops engineer actually uses."""
    from app.models.job import Job

    job_id = client.post("/jobs", json={"job_type": "send_email"}).json()["id"]

    # Drive the job into the dead-letter state the way the worker would.
    job = db_session.query(Job).filter(Job.id == uuid.UUID(job_id)).one()
    job.status = JobStatus.DEAD_LETTER
    job.attempts = 3
    db_session.commit()

    res = client.post(f"/jobs/{job_id}/retry")

    assert res.status_code == 200
    assert res.json()["status"] == "queued"
    assert res.json()["attempts"] == 0  # budget genuinely reset, not just in memory


# --- layering ---------------------------------------------------------------


def test_routers_contain_no_sql_or_redis_calls():
    """The Maintainability NFR is build-blocking, so it gets a test.

    Checkpoint 3 asks a human to eyeball the routers for SQL. A human stops
    checking around week two; this does not.
    """
    from pathlib import Path

    # Resolved from this file, not the working directory, so the check holds
    # wherever pytest is invoked from.
    repo_root = Path(__file__).resolve().parent.parent
    router_src = (repo_root / "app/api/routers/jobs.py").read_text(encoding="utf-8")
    for forbidden in ("session.query", "self._db", "SELECT ", "INSERT ", "zadd", "lpush"):
        assert forbidden not in router_src, f"router leaked a lower layer: {forbidden}"
    assert "from app.repositories" not in router_src
