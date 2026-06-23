from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ThreadsSettings(BaseSettings):
    """Concurrency settings for the ProcessPoolExecutor used by the video worker."""

    model_config = SettingsConfigDict(
        env_file="stack.env", env_file_encoding="utf-8", extra="ignore"
    )

    max_process_workers: int = Field(default=4, validation_alias="MAX_PROCESS_WORKERS")


threads_settings = ThreadsSettings()
