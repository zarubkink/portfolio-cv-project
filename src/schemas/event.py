"""Pydantic / SQLModel schemas for raw detection events."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict


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


__all__ = [
    "DetectorMethod",
    "EventBase",
    "EventCreate",
    "EventPublic",
    "EventType",
]


class EventBase:
    """Column mixin used by :class:`src.models.event.Event` (SQLModel table).

    Kept as a plain class — not a Pydantic ``BaseModel`` — so that
    ``class Event(BaseFields, EventBase, table=True)`` continues to work
    the same way it did before ``EventCreate`` was introduced.
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


class EventCreate(BaseModel):
    """Payload used by the pipeline to insert a raw detection event.

    Mirrors :class:`src.models.event.Event` minus the auto-managed
    columns (``id``, ``created_at``, ``updated_at``, ``deleted_at``)
    plus ``video_file_id`` / ``tractor_id`` / ``wall_clock_at`` which
    are filled in by the recogniser rather than by the detector.
    """

    model_config = ConfigDict(extra="ignore")

    video_file_id: int
    tractor_id: int | None = None
    aruco_id: int | None = None
    event_type: EventType = EventType.DETECTED
    detector_method: DetectorMethod = DetectorMethod.ARUCO
    inside_roi: bool = False
    frame_number: int
    timestamp_in_video: float
    wall_clock_at: datetime
    confidence: float | None = None
    bbox: dict | None = None
    detector_metadata: dict | None = None


class EventPublic(BaseModel):
    """API response shape for a single event row."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    video_file_id: int
    tractor_id: int | None
    aruco_id: int | None
    event_type: EventType
    detector_method: DetectorMethod
    inside_roi: bool
    frame_number: int
    timestamp_in_video: float
    wall_clock_at: datetime
    confidence: float | None = None
    bbox: dict | None = None
    detector_metadata: dict | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
