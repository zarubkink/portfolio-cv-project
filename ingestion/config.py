from pathlib import Path

from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.config.database import settings as database_settings


class IngestionSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="stack.env", env_file_encoding="utf-8", extra="ignore"
    )

    stations_root: Path = Field(
        default=Path("./data/queue"),
        description="Корень с папками STATION_XX, куда падают видео от камер",
    )
    videos_storage: Path = Field(
        default=database_settings.videos_storage,
        description="Cold storage (тот же, что и в API)",
    )
    api_url: AnyHttpUrl = Field(
        default="http://localhost:8000/v1/videos/handle",
        description="Эндпоинт API для приёма видео",
    )
    timestamp_format: str = Field(
        default="%Y%m%d_%H%M%S",
        description="Формат timestamp в имени файла: STATION_XX/YYYYMMDD_HHMMSS.mp4",
    )
    cursor_sleep_sec: float = Field(
        default=5.0, description="Пауза между проходами, если файлов нет"
    )
    stations_watcher_sleep_sec: float = Field(
        default=60.0, description="Пауза между обходами списка станций"
    )
    producer_error_sleep_sec: float = Field(
        default=60.0, description="Пауза после ошибки producer-а"
    )
    default_video_duration_sec: float = Field(
        default=60.0, description="Заглушка длительности видео по умолчанию"
    )
    num_concurrent_requests: int = Field(
        default=4, description="Параллельных HTTP-запросов к API"
    )
    allowed_extensions: tuple[str, ...] = Field(
        default=(".mp4", ".avi", ".mov", ".mkv"),
        description="Допустимые расширения видео",
    )
    api_request_timeout_sec: float = Field(
        default=1800.0, description="Таймаут запроса к API"
    )
    consumer_sleep_on_error_sec: float = Field(default=5.0)


config = IngestionSettings()
config.videos_storage.mkdir(parents=True, exist_ok=True)
config.stations_root.mkdir(parents=True, exist_ok=True)
