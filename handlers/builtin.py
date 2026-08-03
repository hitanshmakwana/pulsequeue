"""Built-in job handlers.

Why this file exists:
    This is the extension point. Adding a job type means adding a function here
    with a ``@job_handler`` decorator — no worker code, no registry code, no
    router code changes. That property is the whole point of the plugin
    registry, and this file is the demonstration of it.

    In a real deployment these would call SendGrid, an image pipeline, a report
    renderer. Here they sleep to simulate I/O and, in one case, fail randomly so
    that retry and dead-letter behaviour is observable without having to break
    something on purpose.

Handler contract:
    - Takes the job's ``payload`` dict.
    - Returns a JSON-serialisable dict, stored as the job's ``result``.
    - Raises on failure. The exception message is stored and the job is handed
      to ``RetryService``.
"""

import logging
import random
import time
from typing import Any

from app.registry.job_registry import job_handler

log = logging.getLogger(__name__)

# Simulated transient failure rate for send_email. Kept as a module constant so
# it is obvious this is a demo affordance rather than production behaviour, and
# so tests can patch it to 0 for determinism.
EMAIL_FAILURE_RATE = 0.2


@job_handler("send_email")
def send_email(payload: dict[str, Any]) -> dict:
    """Simulate sending an email.

    Fails ~20% of the time so that retry, backoff and dead-lettering are
    exercised by ordinary traffic rather than needing a fault injector.
    """
    recipient = payload.get("to")
    time.sleep(0.5)  # stand in for SMTP round-trip latency
    if random.random() < EMAIL_FAILURE_RATE:
        raise ConnectionError(f"SMTP connection failed for {recipient}")
    log.debug("Email sent to %s", recipient)
    return {"sent": True, "recipient": recipient}


@job_handler("resize_image")
def resize_image(payload: dict[str, Any]) -> dict:
    """Simulate resizing an image — a CPU-bound job with no failure mode."""
    time.sleep(1.0)
    return {
        "width": payload.get("width", 800),
        "height": payload.get("height", 600),
        "source": payload.get("url"),
    }


@job_handler("generate_report")
def generate_report(payload: dict[str, Any]) -> dict:
    """Simulate a slow report build — the job type that justifies priorities."""
    time.sleep(2.0)
    report_id = payload.get("report_id", "unknown")
    return {"report_url": f"/reports/{report_id}.pdf"}


@job_handler("benchmark_noop")
def benchmark_noop(payload: dict[str, Any]) -> dict:
    """Do nothing, immediately. Exists purely for load testing.

    The three handlers above sleep to imitate real I/O, which means a
    throughput measurement taken with them reports the sleep duration, not the
    system. Three workers running a 0.5s handler cannot exceed 6 jobs/sec no
    matter how fast the queue is — that number describes ``time.sleep``.

    This handler removes the simulated work so the measurement isolates what
    PulseQueue itself costs per job: one atomic dequeue, three row updates, and
    a pub/sub publish. That is the number worth quoting, and quoting it next to
    the simulated-I/O number is what makes both honest.
    """
    return {"ok": True}
