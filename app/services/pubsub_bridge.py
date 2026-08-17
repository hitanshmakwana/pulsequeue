"""Pub/Sub Bridge — optional GCP intake gateway.

Why this file exists:
    When GCP_PROJECT_ID is set, this service subscribes to a Cloud Pub/Sub
    subscription and forwards each message as a POST to the local /jobs API.
    The existing Redis-native pipeline (Lua-atomic priority queue, backoff,
    DLQ, recovery) handles the rest — this is purely an intake adapter.

    When GCP_PROJECT_ID is NOT set (local dev, CI), the bridge exits
    immediately after logging a no-op message. Nothing else in the codebase
    changes.

Pub/Sub DLT behaviour:
    If the bridge fails to submit a message to /jobs (API down, malformed
    payload), it NACKs the message. Pub/Sub re-delivers up to
    `max_delivery_attempts` times, then moves the message to the dead-letter
    topic (pulsequeue-dlq Pub/Sub topic). This is distinct from PulseQueue's
    own Redis DLQ — it handles messages that never even reached the API.

Run it with::

    python -m app.services.pubsub_bridge

Or as the `pubsub-bridge` Docker Compose service in docker-compose.gcp.yml.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy import of google-cloud-pubsub so the package is only required on GCP.
# If the package is missing, the bridge exits cleanly with a clear error.
# ---------------------------------------------------------------------------
try:
    from google.cloud import pubsub_v1  # type: ignore[import]
    from google.api_core.exceptions import GoogleAPICallError  # type: ignore[import]
    _PUBSUB_AVAILABLE = True
except ImportError:
    _PUBSUB_AVAILABLE = False


# The API runs on the same VM / Docker network, so we talk to it over HTTP.
# In docker-compose.gcp.yml this is set to http://api:8000; in bare-metal
# mode it defaults to localhost:8000.
API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")
JOBS_URL = f"{API_BASE_URL}/jobs"

# How long to wait between empty-poll retries (seconds).
_EMPTY_POLL_BACKOFF = 1.0
# Maximum seconds to wait for the API to come up at startup.
_API_STARTUP_TIMEOUT = 120


def _wait_for_api() -> bool:
    """Block until the /health endpoint returns 200 or timeout elapses."""
    health_url = f"{API_BASE_URL}/health"
    deadline = time.monotonic() + _API_STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=3) as resp:
                if resp.status == 200:
                    log.info("API is healthy at %s", API_BASE_URL)
                    return True
        except Exception:
            pass
        time.sleep(2)
    log.error("API did not become healthy within %ds", _API_STARTUP_TIMEOUT)
    return False


def _submit_job(payload_dict: dict[str, Any]) -> bool:
    """POST a job to the PulseQueue /jobs endpoint.

    Returns True on success (2xx), False on any error.
    The caller NACKs on False so Pub/Sub re-delivers.
    """
    try:
        body = json.dumps(payload_dict).encode("utf-8")
        req = urllib.request.Request(
            JOBS_URL,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
            if 200 <= status < 300:
                log.debug("Job submitted via Pub/Sub bridge, API status=%d", status)
                return True
            log.warning("API returned non-2xx %d for Pub/Sub message", status)
            return False
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            # Idempotency hit — the job already exists. ACK so Pub/Sub
            # doesn't keep re-delivering a duplicate we've already handled.
            log.info("Idempotency hit (409) for Pub/Sub message — ACKing")
            return True
        log.warning("API HTTP %d: %s", exc.code, exc)
        return False
    except Exception as exc:
        log.warning("Failed to submit Pub/Sub message to API: %s", exc)
        return False


def _handle_message(message: Any) -> None:
    """Callback invoked by the Pub/Sub subscriber for each message.

    Message format expected::

        {
            "job_type": "send_email",
            "payload": {"to": "user@example.com"},
            "priority": 2,
            "idempotency_key": "optional-key"   # optional
        }

    Any message that cannot be decoded or is missing required fields is
    NACKed so it flows to the dead-letter topic after max_delivery_attempts.
    """
    try:
        data = json.loads(message.data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        log.error("Malformed Pub/Sub message (cannot decode): %s", exc)
        message.nack()
        return

    if "job_type" not in data:
        log.error("Pub/Sub message missing required field 'job_type': %r", data)
        message.nack()
        return

    log.info(
        "Received Pub/Sub message: job_type=%s, message_id=%s",
        data.get("job_type"),
        message.message_id,
    )

    if _submit_job(data):
        message.ack()
    else:
        message.nack()


def run_bridge() -> None:
    """Entry point for the Pub/Sub bridge process."""
    from app.core.config import settings
    from app.core.logging import configure_logging

    configure_logging("pubsub-bridge")

    if not settings.gcp_project_id:
        log.info(
            "GCP_PROJECT_ID is not set — Pub/Sub bridge is disabled. "
            "Set GCP_PROJECT_ID to enable the Pub/Sub intake gateway."
        )
        return

    if not _PUBSUB_AVAILABLE:
        log.error(
            "google-cloud-pubsub is not installed. "
            "Add it to requirements.txt: google-cloud-pubsub>=2.21.0"
        )
        return

    log.info(
        "Pub/Sub bridge starting: project=%s subscription=%s → %s",
        settings.gcp_project_id,
        settings.pubsub_subscription,
        JOBS_URL,
    )

    if not _wait_for_api():
        log.error("Exiting: API unreachable")
        return

    subscription_path = (
        f"projects/{settings.gcp_project_id}"
        f"/subscriptions/{settings.pubsub_subscription}"
    )

    # StreamingPullFuture: the subscriber opens a bidirectional gRPC stream
    # and calls _handle_message in a thread pool for each received message.
    # max_messages limits in-flight messages so a burst doesn't overwhelm the
    # API or exhaust the thread pool.
    subscriber = pubsub_v1.SubscriberClient()
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)

    log.info("Subscribing to %s", subscription_path)
    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=_handle_message,
        flow_control=flow_control,
    )

    try:
        # Block the main thread. The subscriber processes messages in a
        # background thread pool managed by the Pub/Sub client.
        log.info("Pub/Sub bridge running — press Ctrl+C to stop")
        streaming_pull_future.result()
    except KeyboardInterrupt:
        log.info("Pub/Sub bridge shutting down")
        streaming_pull_future.cancel()
        streaming_pull_future.result()
    except GoogleAPICallError as exc:
        log.exception("Pub/Sub API error: %s", exc)
        streaming_pull_future.cancel()
        raise


if __name__ == "__main__":
    run_bridge()
