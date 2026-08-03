"""Database infrastructure — engine, session factory, declarative Base.

Why this file exists:
    Connection plumbing only. No models live here, no queries, no business
    logic. This is the lowest layer in the dependency chain: everything points
    at it, it points at nothing.

Who owns this:
    ``core/database.py`` creates the plumbing. ``repositories/`` consumes the
    session. ``api/dependencies.py`` exposes ``get_db()`` to FastAPI. The
    worker opens its own session per job. Nobody else touches sessions.
"""

import logging
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.core.config import settings

log = logging.getLogger(__name__)

# create_engine builds a connection pool to PostgreSQL.
#   pool_pre_ping  — issue a cheap SELECT 1 before handing out a pooled
#                    connection, so a connection killed by a container restart
#                    or an idle timeout is transparently replaced instead of
#                    surfacing as a random OperationalError mid-request.
#   pool_recycle   — proactively discard connections older than 30 min, which
#                    is shorter than the idle timeout of every managed Postgres
#                    provider we might deploy to.
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    future=True,
)

# sessionmaker is a factory — each SessionLocal() call yields a fresh session.
# autocommit/autoflush off: the repository decides explicitly when to commit.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    """Declarative base every ORM model inherits from."""


def init_db(retries: int = 15, delay: float = 2.0) -> None:
    """Create any missing tables.

    v1 deliberately has no migration tool (see docs/DECISIONS.md) — the schema
    is a single table, and Alembic would be ceremony without payoff. This is
    the documented tradeoff, not an oversight.

    The retry loop exists because in ``docker compose up`` the application
    containers can win the race against Postgres finishing its first-boot
    initialisation even with a healthcheck gate, and crash-looping on startup
    is a worse failure mode than waiting.
    """
    # Imported here rather than at module scope: app.models.job imports Base
    # from this module, so a top-level import would be circular. Importing it
    # is what registers the Job table on Base.metadata.
    from app.models import job  # noqa: F401

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            Base.metadata.create_all(bind=engine)
            log.info("Database schema ready")
            return
        except Exception as exc:  # pragma: no cover - infrastructure path
            last_error = exc
            log.warning(
                "Database not ready (attempt %d/%d): %s", attempt, retries, exc
            )
            time.sleep(delay)

    raise RuntimeError(
        f"Could not initialise the database after {retries} attempts"
    ) from last_error
