from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, ForeignKey, Index, text
from sqlalchemy.dialects.postgresql import ENUM as PgEnum
from sqlmodel import Field

from src.models.base import BaseFields
from src.schemas.event import DetectorMethod, EventBase, EventType

_event_type_enum = PgEnum(
    EventType,
    name="event_type",
    values_callable=lambda e: [m.value for m in e],
    create_type=False,
)

_detector_method_enum = PgEnum(
    DetectorMethod,
    name="detector_method",
    values_callable=lambda e: [m.value for m in e],
    create_type=False,
)


class Event(BaseFields, EventBase, table=True):
    __tablename__ = "events"
    __table_args__ = (
        Index(
            "ix_events_video_frame",
            "video_file_id",
            "frame_number",
        ),
        Index(
            "ix_events_tractor_wall",
            "tractor_id",
            "wall_clock_at",
            postgresql_where="tractor_id IS NOT NULL",
        ),
        Index(
            "ix_events_aruco_wall",
            "aruco_id",
            "wall_clock_at",
            postgresql_where="aruco_id IS NOT NULL",
        ),
        Index(
            "ix_events_inside_roi_wall",
            "wall_clock_at",
            postgresql_where="inside_roi = TRUE",
        ),
        Index(
            "ix_events_detector_method",
            "detector_method",
            "wall_clock_at",
        ),
    )

    video_file_id: int = Field(
        sa_column=Column(
            ForeignKey("video_files.id", ondelete="CASCADE"),
            nullable=False,
        ),
    )

    tractor_id: int | None = Field(
        default=None,
        sa_column=Column(
            ForeignKey("tractors.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )

    bbox: dict | None = Field(
        default=None,
        sa_column=Column(JSON, nullable=True),
        description='BBox {"x","y","w","h"} в пикселях кадра.',
    )

    detector_metadata: dict | None = Field(
        default=None,
        sa_column=Column(JSON, nullable=True),
        description="Метод-специфичные данные (triggered_by_mog2, velocity, ...).",
    )

    wall_clock_at: datetime = Field(
        sa_column=Column(
            DateTime(),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        description="Server-side timestamp set on insert.",
    )

    event_type: EventType = Field(
        default=EventType.DETECTED,
        sa_column=Column(_event_type_enum, nullable=False, server_default="DETECTED"),
    )

    detector_method: DetectorMethod = Field(
        default=DetectorMethod.ARUCO,
        sa_column=Column(_detector_method_enum, nullable=False, server_default="aruco"),
    )
