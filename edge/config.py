"""Settings for the edge device process.

The edge process runs on a Raspberry Pi on the farm, opens an RTSP
stream from the local camera, runs the cheap ArUco detector and
publishes detection batches to MQTT. These settings are read from
``stack.env`` (same as the rest of the project) so the operator can
flip a single flag instead of editing code on the Pi.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class EdgeSettings(BaseSettings):
    """Connection + batching settings for the edge process."""

    model_config = SettingsConfigDict(
        env_file="stack.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    rtsp_url: str = Field(
        default="rtsp://localhost:8554/cam",
        validation_alias="EDGE_RTSP_URL",
    )
    station_code: str = Field(
        default="STATION_EDGE",
        validation_alias="EDGE_STATION_CODE",
    )
    fps_target: float = Field(
        default=10.0,
        validation_alias="EDGE_FPS_TARGET",
    )
    frame_skip: int = Field(
        default=1,
        validation_alias="EDGE_FRAME_SKIP",
    )
    reconnect_delay: float = Field(
        default=2.0,
        validation_alias="EDGE_RECONNECT_DELAY",
    )
    publish_interval_sec: float = Field(
        default=5.0,
        validation_alias="EDGE_PUBLISH_INTERVAL_SEC",
    )
    publish_max_events: int = Field(
        default=500,
        validation_alias="EDGE_PUBLISH_MAX_EVENTS",
    )
    roi_polygon: list[list[int]] | None = Field(
        default=None,
        validation_alias="EDGE_ROI_POLYGON",
    )
    topic_prefix: str = Field(
        default="farm",
        validation_alias="EDGE_TOPIC_PREFIX",
    )


edge_settings = EdgeSettings()
