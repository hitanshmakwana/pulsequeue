"""Shared pytest fixtures.

The suite is split in two:

    unit         — no external process required. Services are exercised against
                   ``MagicMock`` repositories and Redis clients. This is the
                   payoff of the layered architecture: retry policy, idempotency
                   logic and queue semantics are all testable with nothing
                   running.

    integration  — needs Postgres and Redis (``docker compose up postgres redis``).
                   Marked with ``@pytest.mark.integration``.

Integration tests skip automatically when the database is unreachable, so a
fresh clone can run ``pytest`` and get a green result. CI must not silently skip
them, so it sets ``REQUIRE_INTEGRATION_TESTS=1``, which turns a skip into a
failure.
"""

import os
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.api.dependencies import get_redis
from app.core.database import SessionLocal, engine
from app.main import app
from app.models.job import Job


def _database_available() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def database_required() -> None:
    """Skip (or fail, in CI) when Postgres is not reachable."""
    if _database_available():
        return
    message = (
        "PostgreSQL is not reachable at DATABASE_URL. "
        "Start it with: docker compose up -d postgres redis"
    )
    if os.getenv("REQUIRE_INTEGRATION_TESTS") == "1":
        pytest.fail(message)
    pytest.skip(message, allow_module_level=True)


@pytest.fixture()
def fake_redis() -> MagicMock:
    """Stand-in Redis client.

    Integration tests exercise the HTTP layer and the database; they should not
    also depend on a live Redis, and asserting on this mock is how a test
    proves the API actually enqueued something.
    """
    mock = MagicMock()
    # QueueService calls register_script at construction time and then calls
    # whatever it returns, so the return value must itself be callable.
    mock.register_script.return_value = MagicMock(return_value=0)
    mock.zpopmin.return_value = []
    mock.bzpopmin.return_value = None
    return mock


@pytest.fixture()
def client(database_required, fake_redis):
    """TestClient with a real database and a mocked Redis.

    Used as a context manager so FastAPI's lifespan actually runs — without
    that, ``init_db()`` never fires and the ``jobs`` table would not exist.
    """
    app.dependency_overrides[get_redis] = lambda: fake_redis
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture()
def db_session(database_required):
    """Direct database session for tests that assert on persisted rows."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture()
def clean_jobs_table(database_required):
    """Empty the jobs table before and after a test.

    Only used by tests whose assertions depend on global counts (stats). Most
    tests assert on rows they created themselves and do not need this.
    """
    def _truncate() -> None:
        session = SessionLocal()
        try:
            session.query(Job).delete()
            session.commit()
        finally:
            session.close()

    _truncate()
    yield
    _truncate()
