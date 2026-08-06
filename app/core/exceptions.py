"""Domain exceptions.

Why this file exists:
    The service layer must never leak persistence details upward and must never
    know about HTTP. If ``JobService`` raised ``sqlalchemy.exc.IntegrityError``,
    the router would have to import SQLAlchemy to catch it — a dependency
    pointing the wrong way. If it raised ``HTTPException``, the service would
    be unusable from the worker, which speaks no HTTP.

    So the repository translates persistence errors into these domain errors,
    services raise them, and the router is the single place that maps a domain
    error onto a status code.

Who owns this:
    ``core/`` defines them. Repositories and services raise them. The API layer
    (``app/api/routers/jobs.py``) is the only place that translates them into
    HTTP responses.
"""


class PulseQueueError(Exception):
    """Base class for every error this system raises deliberately."""


class DuplicateIdempotencyKey(PulseQueueError):
    """A job with this ``idempotency_key`` already exists.

    Raised by ``JobRepository.create`` when the unique constraint fires. This
    happens when two requests carrying the same key race each other past
    ``JobService``'s pre-check — the database, not the application, is the
    thing that actually enforces uniqueness.
    """


class JobNotFound(PulseQueueError):
    """No job exists with the requested id."""


class InvalidStateTransition(PulseQueueError):
    """The requested operation is illegal for the job's current status.

    Example: manually retrying a job that is still RUNNING. The job state
    machine is a business rule, so it is enforced in the service layer.
    """


class JobTimeoutError(PulseQueueError):
    """A job handler exceeded its configured ``timeout_seconds`` budget.

    Raised by the worker when ``concurrent.futures.Future.result(timeout=N)``
    raises ``TimeoutError``. Translated into a domain exception so that
    ``RetryService.handle_failure`` can treat it like any other failure without
    importing ``concurrent.futures``.

    The distinction from a plain ``Exception`` matters for metrics: the worker
    records ``outcome="timeout"`` against the job type so that timeouts are
    visible separately from business-logic failures in the Prometheus dashboard.
    """


class DagCycleError(PulseQueueError):
    """A job's dependency graph contains a cycle.

    Raised by ``DagService`` when a submitted job would introduce a circular
    dependency chain. A cyclic DAG can never resolve — every job waits for
    another that waits for it — so it is rejected at submit time rather than
    silently deadlocking the queue.
    """


class UnresolvableDependency(PulseQueueError):
    """A ``depends_on`` job ID does not exist in the database.

    Raised by ``JobService.submit`` when the caller references a dep that has
    not been submitted yet. Failing fast here prevents silent phantom deps that
    would leave a job permanently PENDING.
    """

