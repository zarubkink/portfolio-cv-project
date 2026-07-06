from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="stack.env", env_file_encoding="utf-8", extra="ignore"
    )

    database_url: str = Field(
        default="postgresql+asyncpg://agro:agro@db:5432/agro",
        validation_alias="DB_URL",
        description="asyncpg URL, must reach the running PostgreSQL",
    )
    db_pool_size: int = Field(default=20)
    db_max_overflow: int = Field(default=10)

    videos_storage: Path = Field(
        default=Path("./data/videos"),
        description="Cold storage for ingested videos",
    )
    failed_videos_folder: Path = Field(
        default=Path("./data/failed_videos"),
        description="Folder for permanently failed videos",
    )


settings = DatabaseSettings()
