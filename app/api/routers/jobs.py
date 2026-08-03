"""Job routes.

Why this file exists:
    HTTP request parsing and response shaping. Note what is absent: no SQL, no
    Redis calls, no retry logic, no status arithmetic. Every route body is one
    delegating call plus, at most, a translation of a domain exception into a
    status code. That is the measure of whether the layering held.

Who owns this:
    ``api/routers/`` owns the HTTP contract. It depends on services and schemas
    and nothing below them.
"""

import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.api.dependencies import get_job_service, get_metrics_service
from app.core.exceptions import InvalidStateTransition, JobNotFound
from app.models.job import JobStatus
from app.schemas.job import JobCreate, JobResponse, StatsResponse
from app.services.job_service import JobService
from app.services.metrics_service import MetricsService

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a job",
)
def submit_job(
    job_in: JobCreate,
    svc: JobService = Depends(get_job_service),
) -> JobResponse:
    """Queue a job for asynchronous execution.

    Supplying an ``idempotency_key`` that has been seen before returns the
    original job untouched instead of executing the work a second time.
    """
    return svc.submit(job_in)


# Declared before /{job_id} so the literal path wins the route match. FastAPI
# resolves in declaration order, and while `job_id: uuid.UUID` would reject
# "stats" with a 422 anyway, relying on a validation failure to route correctly
# is a trap for the next person who loosens the type.
@router.get("/stats", response_model=StatsResponse, summary="Job counts by status")
def get_stats(svc: MetricsService = Depends(get_metrics_service)) -> StatsResponse:
    return svc.get_stats()


@router.get("", response_model=list[JobResponse], summary="List jobs")
def list_jobs(
    status: Optional[JobStatus] = Query(
        default=None, description="Filter by status, e.g. ?status=dead_letter"
    ),
    limit: int = Query(default=50, ge=1, le=500),
    svc: JobService = Depends(get_job_service),
) -> list[JobResponse]:
    """Most recently created jobs first, optionally filtered by status."""
    return svc.list(status, limit)


@router.get("/{job_id}", response_model=JobResponse, summary="Get one job")
def get_job(
    job_id: uuid.UUID,
    svc: JobService = Depends(get_job_service),
) -> JobResponse:
    job = svc.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.post(
    "/{job_id}/retry",
    response_model=JobResponse,
    summary="Manually re-queue a dead-lettered job",
)
def retry_job(
    job_id: uuid.UUID,
    svc: JobService = Depends(get_job_service),
) -> JobResponse:
    """Give a dead-lettered job a fresh attempt budget and re-queue it.

    Translating domain exceptions to HTTP is this layer's job, and only this
    layer's — ``JobService`` raises ``InvalidStateTransition`` because it is a
    rule about jobs, not about status codes, and the worker calls the same
    service with no HTTP anywhere in sight.
    """
    try:
        return svc.manual_retry(job_id)
    except JobNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except InvalidStateTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
