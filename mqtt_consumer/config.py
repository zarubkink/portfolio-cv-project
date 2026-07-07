"""Consumer config — the server-side subscriber that forwards MQTT
detections to the FastAPI ``/v1/events/ingest`` endpoint.

The consumer runs as a standalone process (typically ``mqtt_consumer``
in ``compose.yaml``). It owns the long-lived MQTT broker connection
so the FastAPI workers stay stateless.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConsumerSettings(BaseSettings):
    """Connection + forwarder settings for the MQTT consumer."""

    model_config = SettingsConfigDict(
        env_file="stack.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_url: str = Field(
        default="http://api:8000",
        validation_alias="CONSUMER_API_URL",
    )
    ingest_path: str = Field(
        default="/v1/events/ingest",
        validation_alias="CONSUMER_INGEST_PATH",
    )
    request_timeout_sec: float = Field(
        default=10.0,
        validation_alias="CONSUMER_REQUEST_TIMEOUT",
    )
    max_retries: int = Field(
        default=3,
        validation_alias="CONSUMER_MAX_RETRIES",
    )
    retry_backoff_sec: float = Field(
        default=1.0,
        validation_alias="CONSUMER_RETRY_BACKOFF",
    )
    client_id_prefix: str = Field(
        default="agro-consumer",
        validation_alias="CONSUMER_CLIENT_ID_PREFIX",
    )


consumer_settings = ConsumerSettings()
