"""Settings for the visit state machine.

All values are tunable via environment variables so the same image
can be tuned per environment (dev vs prod) without code changes.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class VisitSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="stack.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    entry_confirm_seconds: float = Field(
        default=1.0,
        description=(
            "How many seconds of consecutive in-ROI detections are "
            "required before an ENTERING visit flips to PRESENT."
        ),
    )
    exit_confirm_seconds: float = Field(
        default=10.0,
        description=(
            "How many seconds without an in-ROI detection are required "
            "before PRESENT flips to LEAVING, and again to CLOSED."
        ),
    )
    stale_check_interval_seconds: float = Field(
        default=5.0,
        description="How often the periodic stale-check task wakes up.",
    )
    recovery_grace_multiplier: float = Field(
        default=3.0,
        description=(
            "On startup, ENTERING visits older than "
            "entry_confirm_seconds * this multiplier are deleted as "
            "false positives."
        ),
    )


visit_settings = VisitSettings()
