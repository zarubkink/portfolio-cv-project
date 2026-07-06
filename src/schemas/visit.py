"""Pydantic/SQLModel schemas for the ``visits`` table.

VisitState is exposed as a StrEnum so JSON payloads can carry the
state machine value as a plain string.

``ABSENT`` is a logical state used in API responses to indicate "no
open visit for this tractor"; it is never stored in the database.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from sqlmodel import Field, SQLModel


class VisitState(StrEnum):
    ABSENT = "ABSENT"
    ENTERING = "ENTERING"
    PRESENT = "PRESENT"
    LEAVING = "LEAVING"
    CLOSED = "CLOSED"


class VisitBase(SQLModel):
    tractor_id: int = Field(..., foreign_key="tractors.id")
    station_id: int = Field(..., foreign_key="stations.id")
    state: VisitState = Field(default=VisitState.ENTERING)
    arrived_at: datetime | None = None
    departed_at: datetime | None = None
    last_seen_at: datetime | None = None
    entry_event_id: int | None = None
    exit_event_id: int | None = None
    last_event_id: int | None = None
    entry_seen_seconds: float = 0.0


class VisitPublic(SQLModel):
    id: int
    tractor_id: int
    station_id: int
    state: VisitState
    arrived_at: datetime | None
    departed_at: datetime | None
    last_seen_at: datetime | None
    duration_seconds: float | None
    entry_event_id: int | None
    exit_event_id: int | None
    last_event_id: int | None
    created_at: datetime | None
    updated_at: datetime | None


class VisitCurrent(SQLModel):
    """Slim shape for ``GET /v1/status/tractors`` responses."""

    tractor_id: int
    station_id: int
    state: VisitState
    arrived_at: datetime | None
    last_seen_at: datetime | None
    current_dwell_seconds: float | None


class VisitCurrentStation(SQLModel):
    """Per-station grouping for ``GET /v1/status/stations``."""

    station_id: int
    code: str
    name: str
    tractors: list[VisitTractorAtStation]


class VisitTractorAtStation(SQLModel):
    tractor_id: int | None
    tractor_name: str | None
    state: VisitState
    arrived_at: datetime | None
    current_dwell_seconds: float | None


VisitCurrentStation.model_rebuild()
