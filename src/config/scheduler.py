"""Settings for the retry scheduler.

Mirrors the sbr/src/config/scheduler.py pattern. All values are tunable
via environment variables so the same image can be tuned per
environment (dev vs prod) without code changes.

The instance is lazy-instantiated via :func:`get_scheduler_settings` to
avoid touching the filesystem at import time (which would break alembic
runs and other tools that load the project without runtime privileges).
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SchedulerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="stack.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    scheduler_activate: bool = Field(
        default=True,
        description="Disable to skip starting the scheduler (useful in tests).",
    )
    max_concurrent_requests: int = Field(
        default=2,
        description="Max parallel retry workers.",
    )
    retry_interval_minutes: float = Field(
        default=5.0,
        description="How often the scheduler wakes up to retry failures.",
    )
    stale_threshold_minutes: float = Field(
        default=30.0,
        description="PROCESSING/CREATED videos older than this go to FAILED.",
    )
    max_retry_attempts: int = Field(
        default=3,
        description="After this many retries the video becomes INVALID.",
    )
    failed_videos_folder: Path | None = Field(
        default=None,
        description="Where to move files that exceeded max_retry_attempts.",
    )


_scheduler_settings: SchedulerSettings | None = None


def get_scheduler_settings() -> SchedulerSettings:
    """Return the cached scheduler settings, creating them on first call."""
    global _scheduler_settings
    if _scheduler_settings is None:
        from src.config.database import settings as database_settings

        _scheduler_settings = SchedulerSettings()
        if _scheduler_settings.failed_videos_folder is None:
            _scheduler_settings.failed_videos_folder = (
                database_settings.failed_videos_folder
            )
    return _scheduler_settings


def ensure_scheduler_dirs() -> None:
    """Create the failed-videos folder if it doesn't exist.

    Separated from :func:`get_scheduler_settings` so importing the module
    is side-effect free. The FastAPI lifespan calls this on startup.
    """
    settings = get_scheduler_settings()
    if settings.failed_videos_folder is not None:
        settings.failed_videos_folder.mkdir(parents=True, exist_ok=True)
