from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggingSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="stack.env", env_file_encoding="utf-8", extra="ignore"
    )

    logs_to_file: bool = Field(default=False)
    log_level: str = Field(default="DEBUG")
    logs_dir: Path = Field(default=Path("./logs"))


logging_settings = LoggingSettings()
