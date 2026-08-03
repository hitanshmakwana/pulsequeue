"""The ``jobs`` table — SQLAlchemy ORM model.

Why this file exists:
    This is a data *shape*, nothing more. No methods, no validation, no
    behaviour. ``JobRepository`` queries it; ``JobService`` interprets the
    results. Putting a ``def retry(self)`` on this class would smear business
    logic across the persistence layer and is exactly what the layering rules
    exist to prevent.

Who owns this:
    ``models/`` defines the shape. Only ``repositories/`` imports it for
    querying. Routers must never import it except for the ``JobStatus`` enum,
    which is part of the public API contract (it appears in query strings and
    responses).
"""

import enum
import uuid

from sqlalchemy import Column, DateTime, Index, Integer, String
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.core.clock import utcnow
from app.core.database import Base


class JobStatus(str, enum.Enum):
    """The job state machine's vertices.

    Inheriting from ``str`` means these serialise to their lowercase values in
    JSON responses and compare equal to plain strings, which keeps the API
    contract readable without a translation layer.

        QUEUED  -> RUNNING -> SUCCESS
                      |
                      +----> FAILED ------> (delayed re-queue) -> RUNNING
                      |
                      +----> DEAD_LETTER   (attempts exhausted)
    """

    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Unique + nullable: jobs submitted without a key are always distinct, but
    # two jobs may never share a key. The database is the real enforcement
    # point for idempotency — the service-layer pre-check is only an
    # optimisation that avoids a doomed INSERT in the common case.
    idempotency_key = Column(String(255), unique=True, nullable=True, index=True)

    job_type = Column(String(100), nullable=False)

    # JSONB rather than JSON: binary storage, and it can be indexed later if
    # payload querying ever becomes a requirement.
    payload = Column(JSONB, nullable=False, default=dict)

    priority = Column(Integer, nullable=False, default=3)  # 1 (high) .. 5 (low)

    # values_callable makes Postgres store the lowercase *values*
    # ("queued") rather than SQLAlchemy's default of the member *names*
    # ("QUEUED"). Without it, a `psql` session shows uppercase while every API
    # response shows lowercase, which is a confusing thing to debug at 2am.
    status = Column(
        SAEnum(
            JobStatus,
            name="job_status",
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
        ),
        nullable=False,
        default=JobStatus.QUEUED,
        index=True,
    )

    attempts = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=3)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )

    # Handler output on success, or {"error": ..., "retry_in": ...} on failure.
    result = Column(JSONB, nullable=True)

    __table_args__ = (
        # Serves two hot queries at once:
        #   RecoveryService  — WHERE status = 'running' AND updated_at < cutoff
        #   GET /jobs?status — WHERE status = ? ORDER BY created_at DESC
        Index("ix_jobs_status_updated_at", "status", "updated_at"),
        Index("ix_jobs_status_created_at", "status", "created_at"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid only
        return f"<Job {self.id} {self.job_type} {self.status}>"
