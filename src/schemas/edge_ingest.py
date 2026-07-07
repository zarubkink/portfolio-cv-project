"""Schemas for the edge device MQTT ingress.

The MQTT consumer (a separate process) receives detection messages
from edge cameras and forwards them here. To keep the existing
``events.video_file_id`` FK constraint unchanged, the endpoint
manufactures a "live stream sentinel" row per station — a single
``video_files`` row per ``station_code`` whose ``storage_uri``
encodes the sentinel identity and whose ``content_hash`` is the
sha-256 of that URI. ``status`` stays ``PROCESSING`` so the row
acts as a forever-open container for edge events; a future
cleanup job can sweep stale sentinels based on
``updated_at < now - retention``.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class EdgeEventIn(BaseModel):
    """One detection payload from the edge process.

    Mirrors :class:`src.schemas.event.EventCreate` minus the fields
    the API resolves server-side (``video_file_id``, ``tractor_id``,
    ``wall_clock_at``).
    """

    model_config = ConfigDict(extra="ignore")

    aruco_id: int | None = None
    confidence: float | None = None
    bbox: dict | None = None
    inside_roi: bool = False
    frame_number: int = Field(ge=0)
    timestamp_in_video: float = Field(ge=0.0)
    detector_metadata: dict | None = None


class EdgeBatchIn(BaseModel):
    """Top-level payload published by the edge process per batch.

    ``station_code`` is required because the MQTT wildcard
    subscription may deliver to consumers that handle multiple
    stations. The handler keeps the payload format self-contained
    so a broken topic does not make us lose the station mapping.
    """

    model_config = ConfigDict(extra="ignore")

    station_code: str = Field(min_length=1, max_length=64)
    started_at: datetime
    ended_at: datetime
    events: list[EdgeEventIn] = Field(default_factory=list, max_length=10_000)


class EdgeBatchOut(BaseModel):
    """Response from :func:`POST /v1/events/ingest`."""

    video_file_id: int
    events_created: int
    sentinel_created: bool


__all__ = ["EdgeBatchIn", "EdgeBatchOut", "EdgeEventIn"]
