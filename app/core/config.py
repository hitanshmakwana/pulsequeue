"""Application configuration.

Why this file exists:
    Every environment-derived setting is read here and nowhere else. No other
    module in the codebase imports ``os`` or ``dotenv``. If a knob changes, it
    changes in exactly one place.

Who owns this:
    ``core/`` owns infrastructure concerns. Services, repositories and routers
    import the ``settings`` singleton; they never read the environment directly.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- Infrastructure -------------------------------------------------
    database_url: str = "postgresql://pulse:pulse@localhost:5432/pulsequeue"
    redis_url: str = "redis://localhost:6379/0"

    # --- Worker ---------------------------------------------------------
    # Number of worker processes docker-compose starts (`deploy.replicas`).
    worker_concurrency: int = 3
    # Seconds BZPOPMIN blocks before the worker loops to re-check shutdown
    # and run its periodic recovery sweep. Lower = more responsive shutdown,
    # higher = fewer wasted Redis round-trips on an idle queue.
    dequeue_timeout: int = 1

    # --- Retry policy (consumed exclusively by RetryService) ------------
    max_retry_attempts: int = 3
    base_retry_delay: int = 2

    # --- Crash recovery (consumed exclusively by RecoveryService) -------
    # A job sitting in RUNNING longer than this is presumed abandoned by a
    # worker that died without releasing it. Must be comfortably longer than
    # the slowest legitimate handler.
    visibility_timeout: int = 300
    # How often a worker attempts the recovery sweep. Only one worker in the
    # fleet actually performs it — the rest lose the Redis lock and move on.
    recovery_interval: int = 30

    # --- Misc -----------------------------------------------------------
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


# Module-level singleton — imported everywhere as
#   from app.core.config import settings
settings = Settings()
