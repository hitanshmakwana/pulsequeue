"""Pydantic schemas — the API contract.

Why this file exists:
    These define what comes in over HTTP and what goes back out. They are
    deliberately *not* the ORM model. The database schema can gain a column
    without it appearing in the API, and the API can rename a field without a
    migration. Coupling the two is the single most common way a service ends
    up unable to change its storage.

Who owns this:
    ``schemas/`` owns the wire format. Routers use them for validation and
    serialisation. Services accept ``JobCreate`` (a validated value object) and
    return ORM objects; FastAPI converts on the way out via
    ``from_attributes``.
"""

import uuid
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field

from app.models.job import JobStatus


class JobCreate(BaseModel):
    """Body of ``POST /jobs``."""

    job_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Registered handler name, e.g. 'send_email'.",
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary JSON passed straight to the handler.",
    )
    priority: int = Field(
        default=3, ge=1, le=5, description="1 = highest, 5 = lowest."
    )
    idempotency_key: Optional[str] = Field(
        default=None,
        max_length=255,
        description=(
            "Optional client-supplied dedupe key. Resubmitting with the same "
            "key returns the original job instead of executing it again."
        ),
    )
    max_attempts: Optional[int] = Field(
        default=None,
        ge=1,
        le=10,
        description=(
            "Total executions before dead-lettering. Defaults to the "
            "MAX_RETRY_ATTEMPTS server setting."
        ),
    )

    # default_factory=dict above rather than `= {}`: a bare dict literal would
    # be a shared mutable default across every instance.


class JobResponse(BaseModel):
    """Any response that returns a single job."""

    id: uuid.UUID
    job_type: str
    status: JobStatus
    priority: int
    attempts: int
    max_attempts: int
    created_at: datetime
    updated_at: datetime
    result: Optional[dict[str, Any]] = None

    # Lets FastAPI build this straight from a SQLAlchemy row object.
    model_config = {"from_attributes": True}


class StatsResponse(BaseModel):
    """Body of ``GET /jobs/stats`` — one count per terminal and non-terminal state."""

    queued: int
    running: int
    success: int
    failed: int
    dead_letter: int
