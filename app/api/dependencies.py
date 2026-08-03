"""FastAPI dependency providers.

Why this file exists:
    ``Depends()`` providers belong in one place, not scattered through routers.
    Routers import from here; they never import ``core/database.py`` or
    ``core/redis.py`` directly. This module is the seam between HTTP and
    infrastructure — and the seam tests override to inject fakes.

Who owns this:
    ``api/`` owns request-scoped wiring. Services are constructed here and
    handed to routers fully assembled, so a route function's body is a single
    delegating call.
"""

import redis
from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.core.redis import get_redis_client
from app.services.job_service import JobService
from app.services.metrics_service import MetricsService
from app.services.queue_service import QueueService


def get_db():
    """Yield a request-scoped DB session, always closed afterwards.

    One session per request: it is the unit of work, and returning it to the
    pool in a ``finally`` is what stops a raised exception from leaking a
    connection.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_redis() -> redis.Redis:
    """Return the process-wide Redis client."""
    return get_redis_client()


def get_queue_service(r: redis.Redis = Depends(get_redis)) -> QueueService:
    return QueueService(r)


def get_job_service(
    db: Session = Depends(get_db),
    queue: QueueService = Depends(get_queue_service),
) -> JobService:
    return JobService(db=db, queue=queue)


def get_metrics_service(db: Session = Depends(get_db)) -> MetricsService:
    return MetricsService(db=db)
