"""WebSocket streaming of job status changes.

Why this file exists:
    Connection lifecycle management is its own concern, distinct from job
    routing. Keeping it here leaves ``main.py`` as pure wiring and lets the
    streaming logic be reasoned about (and replaced) on its own.

How it works:
    Workers publish ``{"job_id", "status"}`` on the ``pq:updates`` Redis
    channel. This handler subscribes and forwards each message verbatim to the
    browser. Redis pub/sub is fan-out with no persistence, which is exactly
    right for a live view: a dashboard that connects late should see what is
    happening now, not replay history.

Who owns this:
    ``websocket/`` owns connection handling. It reads the channel name from
    ``QueueService`` rather than hardcoding it, so key naming stays in one place.
"""

import asyncio
import logging

import redis.asyncio as aioredis
from fastapi import WebSocket, WebSocketDisconnect

from app.core.config import settings
from app.services.queue_service import CHANNEL

log = logging.getLogger(__name__)

# How long to wait for a pub/sub message before looping. The loop must come up
# for air periodically so that a client which vanished without a close frame is
# detected by the next send, and so cancellation is honoured promptly.
_POLL_INTERVAL = 1.0


async def stream_job_updates(websocket: WebSocket) -> None:
    """Forward job status updates to one connected client until it disconnects.

    An async Redis client is used rather than the synchronous singleton: this
    runs on the event loop, and a blocking read here would stall every other
    request the process is serving.
    """
    await websocket.accept()

    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    pubsub = client.pubsub()
    await pubsub.subscribe(CHANNEL)
    log.info("Dashboard client connected")

    try:
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=_POLL_INTERVAL
            )
            if message is None:
                # Idle tick. Nothing to forward; loop so cancellation and
                # client disconnects are noticed within a second.
                continue
            await websocket.send_text(message["data"])
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as exc:  # pragma: no cover - defensive
        # A dead socket surfaces as a RuntimeError from Starlette rather than
        # WebSocketDisconnect. Either way the connection is over; log it at
        # debug and unwind cleanly rather than letting it escape as a 500.
        log.debug("Dashboard stream closed: %s", exc)
    finally:
        try:
            await pubsub.unsubscribe(CHANNEL)
            await pubsub.aclose()
        finally:
            await client.aclose()
        log.info("Dashboard client disconnected")
