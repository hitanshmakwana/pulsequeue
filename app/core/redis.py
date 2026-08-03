"""Redis connection singleton.

Why this file exists:
    Connection management is separate from queue semantics. ``QueueService``
    receives a client and uses it; it never constructs one. That separation is
    what lets every ``QueueService`` unit test pass a ``MagicMock`` instead of
    needing a live Redis.

Who owns this:
    ``core/`` owns the connection. ``api/dependencies.py`` hands it to the API
    layer; the worker fetches it directly at startup.
"""

import redis

from app.core.config import settings

# Module-level singleton. redis-py's client is a thin handle over a connection
# pool and is thread-safe, so one instance per process is correct — creating a
# client per request would leak pools.
#
# decode_responses=True: we only ever store UTF-8 job ids and JSON strings, so
# decoding at the client boundary keeps every consumer free of bytes handling.
_client = redis.from_url(
    settings.redis_url,
    decode_responses=True,
    socket_keepalive=True,
    health_check_interval=30,
)


def get_redis_client() -> redis.Redis:
    """Return the shared Redis client."""
    return _client
