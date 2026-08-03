"""Time primitives.

Why this file exists:
    Three separate layers need "what time is it, in UTC" — the ORM model (row
    timestamps), ``QueueService`` (sorted-set scores and delayed-retry
    deadlines) and ``RecoveryService`` (visibility-timeout cutoffs). Funnelling
    them through one module keeps the representation consistent (always
    timezone-aware UTC, never naive local time) and gives tests a single seam
    to patch when they need to control the clock.

Note on ``datetime.utcnow()``:
    It is deprecated as of Python 3.12 and, worse, returns a *naive* datetime
    that merely happens to hold UTC. Mixing naive and aware datetimes raises at
    comparison time, which is exactly the comparison ``RecoveryService`` makes.
    We use timezone-aware datetimes everywhere.
"""

from datetime import datetime, timezone


def utcnow() -> datetime:
    """Current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def epoch_ms() -> int:
    """Current time as integer milliseconds since the Unix epoch.

    Used for Redis sorted-set scores, where an integer keeps the score exactly
    representable as a float64 and keeps ordering intuitive.
    """
    return int(utcnow().timestamp() * 1000)
