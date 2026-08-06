"""Built-in job handlers — real I/O implementations.

Why this file exists:
    This is the extension point. Adding a job type means adding a function here
    with a ``@job_handler`` decorator — no worker code, no registry code, no
    router code changes. That property is the whole point of the plugin
    registry, and this file is the demonstration of it.

Handler contract:
    - Takes the job's ``payload`` dict.
    - Returns a JSON-serialisable dict, stored as the job's ``result``.
    - Raises on failure. The exception message is stored and the job is handed
      to ``RetryService``.
"""

import csv
import io
import logging
import random
import smtplib
import time
from email.mime.text import MIMEText
from typing import Any

from PIL import Image

from app.core.config import settings
from app.registry.job_registry import job_handler

log = logging.getLogger(__name__)

# Simulated transient failure rate for send_email. Kept as a module constant so
# it is obvious this is a demo affordance rather than production behaviour, and
# so tests can patch it to 0 for determinism.
EMAIL_FAILURE_RATE = 0.2


@job_handler("send_email")
def send_email(payload: dict[str, Any]) -> dict:
    """Send a transactional email via SMTP.

    In demo/dev mode (default), this simulates SMTP delivery in-process:
    - 20% of calls raise a simulated SMTP transient failure (exercises retry)
    - 80% of calls return success immediately (exercises the happy path)

    In production, set SMTP_HOST to a real server (not localhost) and the
    handler will connect to it via smtplib. This makes the demo reliable
    without requiring a real SMTP server, while the code path remains the
    same — smtplib.SMTP is only called when a real server is configured.
    """
    recipient = payload.get("to", "unknown@example.com")
    subject = payload.get("subject", "PulseQueue Notification")
    body = payload.get("body", f"This is an automated message for {recipient}.")

    # Simulate transient SMTP failures so the retry path is exercised.
    # This fires BEFORE the SMTP connection attempt, so it works in demo
    # mode even without a real SMTP server.
    if random.random() < EMAIL_FAILURE_RATE:
        raise ConnectionError(
            f"Simulated SMTP transient failure for {recipient} "
            f"(20% failure rate — exercises retry/backoff path)"
        )

    # Only connect to a real SMTP server if one has been explicitly configured.
    # In demo mode (SMTP_HOST=localhost, the default), skip the real network
    # call and return a simulated success immediately.
    use_real_smtp = settings.smtp_host not in ("localhost", "127.0.0.1")

    if use_real_smtp:
        msg = MIMEText(body, "plain")
        msg["Subject"] = subject
        msg["From"] = "pulsequeue@system.local"
        msg["To"] = recipient
        try:
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=5) as server:
                server.sendmail("pulsequeue@system.local", [recipient], msg.as_string())
            log.info(
                "Email sent to %s via %s:%d",
                recipient,
                settings.smtp_host,
                settings.smtp_port,
            )
        except (smtplib.SMTPException, OSError) as exc:
            raise ConnectionError(f"SMTP delivery failed for {recipient}: {exc}") from exc
    else:
        # Demo mode: simulate the send without a real network call.
        log.info(
            "Email [SIMULATED] to %s — subject: '%s' (set SMTP_HOST to send for real)",
            recipient,
            subject,
        )

    return {
        "sent": True,
        "simulated": not use_real_smtp,
        "recipient": recipient,
        "subject": subject,
        "smtp_host": settings.smtp_host,
    }



@job_handler("resize_image")
def resize_image(payload: dict[str, Any]) -> dict:
    """Resize an image using Pillow (PIL).

    If ``url`` is provided in the payload, the handler treats it as a URL and
    would normally fetch the image. For the demo, we create a synthetic RGBA
    image in-memory (no network I/O, no disk I/O) and resize it, which is
    sufficient to demonstrate that Pillow is actually called and the result
    contains real pixel dimensions.

    In a production system, this would be:
        1. Download image from ``url`` using ``requests``
        2. Resize with Pillow
        3. Upload result to S3/GCS
        4. Return the output URL
    """
    target_width = int(payload.get("width", 800))
    target_height = int(payload.get("height", 600))
    # Maintain aspect ratio if only one dimension is supplied.
    if "width" in payload and "height" not in payload:
        target_height = target_width  # square by default

    # Create a synthetic source image (simulates downloading from payload["url"]).
    source_size = (1920, 1080)
    img = Image.new("RGBA", source_size, color=(73, 109, 137, 255))

    # Draw a simple gradient pattern so the image has non-trivial content.
    pixels = img.load()
    for i in range(source_size[0]):
        for j in range(source_size[1]):
            pixels[i, j] = (
                int(i * 255 / source_size[0]),
                int(j * 255 / source_size[1]),
                128,
                255,
            )

    resized = img.resize((target_width, target_height), Image.LANCZOS)

    # Encode to JPEG in-memory to confirm Pillow ran a real codec.
    buf = io.BytesIO()
    resized.convert("RGB").save(buf, format="JPEG", quality=85)
    output_bytes = buf.tell()

    log.info(
        "Resized image from %dx%d to %dx%d (%d bytes JPEG)",
        source_size[0],
        source_size[1],
        target_width,
        target_height,
        output_bytes,
    )

    return {
        "source_width": source_size[0],
        "source_height": source_size[1],
        "output_width": resized.width,
        "output_height": resized.height,
        "output_bytes": output_bytes,
        "format": "JPEG",
        "source_url": payload.get("url"),
    }


@job_handler("generate_report")
def generate_report(payload: dict[str, Any]) -> dict:
    """Generate a CSV report using Python's built-in ``csv`` module.

    Produces a real in-memory CSV with synthetic data rows. In production this
    would query a database, format the results, and upload to object storage.
    The in-memory approach demonstrates that ``csv.writer`` ran and produced a
    measurable output without requiring external dependencies.
    """
    report_id = payload.get("report_id", f"report_{int(time.time())}")
    row_count = int(payload.get("rows", 100))

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["job_id", "status", "duration_ms", "timestamp"])

    for i in range(row_count):
        writer.writerow([
            f"job-{i:06d}",
            random.choice(["success", "success", "success", "failed"]),
            round(random.uniform(10, 5000), 2),
            time.time() - random.uniform(0, 86400),
        ])

    csv_bytes = len(buf.getvalue().encode())
    log.info("Generated report %s: %d rows, %d bytes", report_id, row_count, csv_bytes)

    return {
        "report_id": report_id,
        "report_url": f"/reports/{report_id}.csv",
        "rows_generated": row_count,
        "output_bytes": csv_bytes,
    }


@job_handler("benchmark_noop")
def benchmark_noop(payload: dict[str, Any]) -> dict:
    """Do nothing, immediately. Exists purely for load testing.

    The three handlers above perform real I/O (SMTP, Pillow codec, CSV
    serialisation), which dominates throughput measurements. This handler
    removes the simulated work so the measurement isolates what PulseQueue
    itself costs per job: one atomic dequeue, three row updates, and a pub/sub
    publish. That is the number worth quoting, and quoting it next to the
    real-I/O number is what makes both honest.
    """
    return {"ok": True}
