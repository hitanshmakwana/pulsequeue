"""Prometheus metrics — the single source of truth for every instrument.

Multiprocess mode
-----------------
    PulseQueue runs as multiple separate OS processes: one API process and
    N worker processes. Prometheus counters/histograms are in-memory by default,
    so the API's /metrics endpoint would never see what the workers recorded.

    Fix: prometheus_client's built-in multiprocess mode.
    - Set PROMETHEUS_MULTIPROC_DIR to a directory shared by all containers
      (via a Docker volume: /tmp/prometheus_multiproc)
    - Each process atomically writes its metrics to a file in that directory
    - The /metrics endpoint calls MultiProcessCollector() to aggregate all files

    This is the standard pattern for gunicorn/uvicorn multi-worker deployments.
    Ref: https://prometheus.github.io/client_python/multiprocess/

Instruments
-----------
    pulsequeue_jobs_total
        Counter — terminal outcome per job. Labels: job_type, outcome.
        outcome values: "success", "failed", "dead_letter", "timeout"

    pulsequeue_job_duration_seconds
        Histogram — handler wall-clock time. Labels: job_type, status.

    pulsequeue_claim_latency_seconds
        Histogram — enqueue_time → worker claim time. Labels: job_type.

    pulsequeue_queue_depth / delayed_depth / dead_letter_depth
        Gauges — live Redis queue depths. Updated on every /metrics scrape.
"""

import os

from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    multiprocess,
)

# In multiprocess mode the instruments must write to files, which requires
# PROMETHEUS_MULTIPROC_DIR to exist before any metric is created. We create
# it defensively here rather than relying on Docker to have done it.
_multiproc_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR", "")
if _multiproc_dir:
    os.makedirs(_multiproc_dir, exist_ok=True)


def make_registry() -> CollectorRegistry:
    """Return the registry to use for generate_latest().

    In multiprocess mode, this creates a fresh CollectorRegistry with a
    MultiProcessCollector that reads every process's mmap files. In single-
    process mode (tests, local dev) it returns the default REGISTRY so that
    the instruments defined below are included.
    """
    if _multiproc_dir:
        reg = CollectorRegistry()
        multiprocess.MultiProcessCollector(reg)
        return reg
    return REGISTRY


# --- Counters ----------------------------------------------------------------

jobs_total = Counter(
    "pulsequeue_jobs_total",
    "Total jobs that reached a terminal state.",
    labelnames=["job_type", "outcome"],
)

# --- Histograms --------------------------------------------------------------

_DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)

job_duration_seconds = Histogram(
    "pulsequeue_job_duration_seconds",
    "Wall-clock execution time of the handler, from claim to terminal state.",
    labelnames=["job_type", "status"],
    buckets=_DURATION_BUCKETS,
)

claim_latency_seconds = Histogram(
    "pulsequeue_claim_latency_seconds",
    "Time from job submission to first worker claim.",
    labelnames=["job_type"],
    buckets=_DURATION_BUCKETS,
)

# --- Gauges ------------------------------------------------------------------

queue_depth = Gauge(
    "pulsequeue_queue_depth",
    "Number of jobs currently waiting in the Redis ready queue.",
)

delayed_depth = Gauge(
    "pulsequeue_delayed_depth",
    "Number of jobs waiting out a retry backoff in the delayed set.",
)

dead_letter_depth = Gauge(
    "pulsequeue_dead_letter_depth",
    "Number of jobs in the dead-letter queue awaiting manual intervention.",
)
