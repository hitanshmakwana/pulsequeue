"""Structured logging configuration.

Why this file exists:
    Both entry points (the API process and the worker process) need identical,
    predictable log formatting, and neither should be configuring handlers
    inline. Container logs are read from stdout, so everything goes there.

Who owns this:
    ``core/`` owns it. Entry points call ``configure_logging()`` exactly once at
    startup. Every other module just does ``log = logging.getLogger(__name__)``
    and never touches handlers or levels.
"""

import logging
import sys

from app.core.config import settings

_configured = False


def configure_logging(component: str) -> None:
    """Install a single stdout handler on the root logger.

    Args:
        component: Short tag identifying the process ("api", "worker"). It is
            embedded in every line so interleaved container logs stay readable.

    Idempotent: safe to call from a reloader or from a test that imports the
    app more than once.
    """
    global _configured
    if _configured:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt=f"%(asctime)s %(levelname)-7s [{component}] %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(settings.log_level.upper())

    # These two are chatty at INFO and drown out job lifecycle events.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    _configured = True
