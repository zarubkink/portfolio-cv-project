from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class VideoSettings(BaseSettings):
    """Параметры чтения и downsampling видео."""

    model_config = SettingsConfigDict(
        env_file="stack.env", env_file_encoding="utf-8", extra="ignore"
    )

    target_width: int = Field(default=640, description="Resize target width")
    target_fps: int = Field(default=10, description="Sub-sample target FPS")


video_settings = VideoSettings()
