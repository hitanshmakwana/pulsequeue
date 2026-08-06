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

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from prometheus_client import CONTENT_TYPE_LATEST

from app.api.dependencies import get_job_service, get_metrics_service, get_queue_service
from app.core.exceptions import (
    DagCycleError,
    InvalidStateTransition,
    JobNotFound,
    UnresolvableDependency,
)
from app.core.metrics import dead_letter_depth, delayed_depth, queue_depth
from app.models.job import JobStatus
from app.schemas.job import JobCreate, JobResponse, StatsResponse
from app.services.job_service import JobService
from app.services.metrics_service import MetricsService
from app.services.queue_service import QueueService

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

    Supplying ``depends_on`` creates a DAG-dependent job: it is accepted as
    PENDING and will only be enqueued once every listed dependency has reached
    SUCCESS. Cycles and missing dependency IDs are rejected with 422.
    """
    try:
        return svc.submit(job_in)
    except UnresolvableDependency as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except DagCycleError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


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


# ---------------------------------------------------------------------------
# Prometheus metrics endpoint
# ---------------------------------------------------------------------------

metrics_router = APIRouter(tags=["ops"])


@metrics_router.get(
    "/metrics",
    summary="Prometheus metrics",
    description=(
        "Exposes all Prometheus instruments in the standard text format. "
        "In multiprocess mode, aggregates metrics from all worker processes "
        "via the shared PROMETHEUS_MULTIPROC_DIR volume. "
        "Scraped by Prometheus every 5s."
    ),
)
def prometheus_metrics(queue: QueueService = Depends(get_queue_service)) -> Response:
    """Return Prometheus text-format metrics for scraping.

    Queue-depth gauges are refreshed here on every scrape so they are always
    current. In multiprocess mode, make_registry() reads the mmap files written
    by every worker process and returns their union.
    """
    from prometheus_client import generate_latest

    from app.core.metrics import make_registry

    queue_depth.set(queue.queue_depth())
    delayed_depth.set(queue.delayed_depth())
    dead_letter_depth.set(queue.dead_letter_depth())

    return Response(
        content=generate_latest(make_registry()),
        media_type=CONTENT_TYPE_LATEST,
    )

