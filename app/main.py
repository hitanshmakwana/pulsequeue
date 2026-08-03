"""FastAPI application entry point — wiring only.

Why this file exists:
    It assembles the app: configure logging, create the schema, mount routers,
    mount the dashboard, expose the WebSocket. It contains no business logic of
    its own, and nothing imports *from* it except the ASGI server and the tests.
"""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles

from app.api.routers import jobs
from app.core.database import init_db
from app.core.logging import configure_logging
from app.websocket.stream import stream_job_updates

log = logging.getLogger(__name__)

# Resolved from this file's location rather than the process working
# directory, so `uvicorn app.main:app` behaves identically whether it is
# launched from the repo root, from a container's /app, or from a test runner.
DASHBOARD_DIR = Path(__file__).resolve().parent.parent / "dashboard"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown.

    Schema creation lives here rather than at module import so that importing
    ``app.main`` — which every unit test does — does not require a running
    database. Import-time side effects that reach the network are the reason
    test suites end up needing docker-compose to collect.
    """
    configure_logging("api")
    log.info("PulseQueue API starting")
    init_db()
    yield
    log.info("PulseQueue API shutting down")


app = FastAPI(
    title="PulseQueue",
    description="Distributed job queue and notification service",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(jobs.router)

# html=True serves index.html for the directory root, so /dashboard/ works.
app.mount(
    "/dashboard",
    StaticFiles(directory=str(DASHBOARD_DIR), html=True),
    name="dashboard",
)


@app.get("/health", tags=["ops"])
def health() -> dict[str, str]:
    """Liveness probe used by docker-compose, CI and the deployment platform."""
    return {"status": "ok"}


@app.websocket("/jobs/stream")
async def job_stream(websocket: WebSocket) -> None:
    """Live feed of job status changes, consumed by the dashboard."""
    await stream_job_updates(websocket)
