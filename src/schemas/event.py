"""Pydantic / SQLModel schemas for raw detection events."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum


class EventType(StrEnum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    DETECTED = "DETECTED"


class DetectorMethod(StrEnum):
    """Identifies which pipeline produced a detection.

    Stored in :attr:`Event.detector_method` so we can swap detection
    strategies (YOLO, colour classifier, re-id) without a schema change.
    """

    ARUCO = "aruco"
    YOLO_ARUCO = "yolo_aruco"
    COLOR_CLASS = "color_class"
    REID = "reid"
    FALLBACK = "fallback"


__all__ = ["EventType", "DetectorMethod"]


class EventBase:
    """Re-export of fields used by the pipeline; full SQLModel is defined in
    Stage 6 alongside the ``events`` table migration.
    """

    event_type: EventType
    detector_method: DetectorMethod = DetectorMethod.ARUCO
    inside_roi: bool
    frame_number: int
    timestamp_in_video: float
    confidence: float | None = None
    aruco_id: int | None = None
    bbox: dict | None = None
    detector_metadata: dict | None = None


class EventPublic:
    id: int
    video_file_id: int
    tractor_id: int | None
    wall_clock_at: datetime
    created_at: datetime | None
