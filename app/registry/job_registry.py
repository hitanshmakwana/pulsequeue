"""JobRegistry — the plugin system that maps ``job_type`` to a handler.

Why this file exists:
    A hardcoded ``HANDLERS = {"send_email": send_email}`` dict means adding a
    job type requires editing worker internals — the module least safe to touch,
    because a mistake there stops every job type at once. That violates the
    Open/Closed Principle: the worker should be open for extension and closed
    for modification.

    With a decorator, registration happens at import time. ``handlers/builtin.py``
    declares what it provides; the worker imports that module once at startup and
    looks handlers up by name. Neither knows anything about the other.

    This is not an invented pattern — it is what Celery does with ``@task``,
    Flask with ``@route``, and pytest with fixtures. Naming the precedent is
    worth as much in an interview as the implementation.

Who owns this:
    The registry is the single source of truth for ``job_type`` -> handler.
    ``handlers/`` registers into it; the worker reads from it. Nothing else
    dispatches jobs.
"""

from typing import Any, Callable

# Module-level dict: job_type string -> callable. Module state is the right
# scope here — the registry is per-process, and every process that needs it
# populates it the same way, by importing the handler modules.
_registry: dict[str, Callable[[dict[str, Any]], dict]] = {}


def job_handler(job_type: str) -> Callable:
    """Register the decorated function as the handler for ``job_type``.

    Usage::

        @job_handler("send_email")
        def send_email(payload: dict) -> dict:
            ...
            return {"sent": True}

    A handler takes the job's payload dict and returns a JSON-serialisable dict
    that is stored as the job's result. Raising any exception marks the attempt
    failed and hands the job to ``RetryService``.

    Raises:
        ValueError: two handlers registered for the same ``job_type``. Silently
            letting the second win would mean a copy-pasted decorator quietly
            disables a job type in production, and the symptom would appear
            nowhere near the cause.
    """

    def decorator(fn: Callable[[dict[str, Any]], dict]) -> Callable:
        existing = _registry.get(job_type)
        if existing is not None and existing is not fn:
            raise ValueError(
                f"job_type '{job_type}' is already handled by "
                f"{existing.__module__}.{existing.__qualname__}"
            )
        _registry[job_type] = fn
        return fn

    return decorator


def get_handler(job_type: str) -> Callable[[dict[str, Any]], dict]:
    """Look up a handler by ``job_type``.

    Raises:
        KeyError: nothing is registered under that name. The message lists what
            *is* registered, because the overwhelmingly likely cause is a typo
            or a handler module that was never imported.
    """
    if job_type not in _registry:
        raise KeyError(
            f"No handler registered for job_type '{job_type}'. "
            f"Available: {sorted(_registry)}"
        )
    return _registry[job_type]


def is_registered(job_type: str) -> bool:
    return job_type in _registry


def list_registered() -> list[str]:
    """Every registered job type name, sorted."""
    return sorted(_registry)
