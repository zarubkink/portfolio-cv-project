from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Column,
    Computed,
    DateTime,
    Float,
    ForeignKey,
    Index,
    text,
)
from sqlalchemy.dialects.postgresql import ENUM as PgEnum
from sqlmodel import Field

from src.models.base import BaseFields
from src.schemas.visit import VisitBase, VisitState

_visit_state_enum = PgEnum(
    VisitState,
    name="visit_state",
    values_callable=lambda e: [m.value for m in e],
    create_type=False,
)


class Visit(BaseFields, VisitBase, table=True):
    __tablename__ = "visits"
    __table_args__ = (
        Index(
            "ix_visits_active_tractor",
            "tractor_id",
            postgresql_where=("state IN ('ENTERING', 'PRESENT', 'LEAVING')"),
        ),
        Index(
            "ix_visits_active_station",
            "station_id",
            postgresql_where=("state IN ('ENTERING', 'PRESENT', 'LEAVING')"),
        ),
        Index(
            "ix_visits_tractor_time",
            "tractor_id",
            text("arrived_at DESC"),
            postgresql_where="state = 'CLOSED'",
        ),
        Index(
            "ix_visits_station_time",
            "station_id",
            text("arrived_at DESC"),
            postgresql_where="state = 'CLOSED'",
        ),
        Index(
            "uq_visit_open",
            "tractor_id",
            "station_id",
            unique=True,
            postgresql_where="state <> 'CLOSED'",
        ),
    )

    tractor_id: int = Field(
        sa_column=Column(ForeignKey("tractors.id", ondelete="RESTRICT"), nullable=False)
    )
    station_id: int = Field(
        sa_column=Column(ForeignKey("stations.id", ondelete="RESTRICT"), nullable=False)
    )

    arrived_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime, nullable=True),
    )
    departed_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime, nullable=True),
    )
    last_seen_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime, nullable=True),
    )

    entry_event_id: int | None = Field(
        default=None,
        sa_column=Column(ForeignKey("events.id", ondelete="SET NULL"), nullable=True),
    )
    exit_event_id: int | None = Field(
        default=None,
        sa_column=Column(ForeignKey("events.id", ondelete="SET NULL"), nullable=True),
    )
    last_event_id: int | None = Field(
        default=None,
        sa_column=Column(ForeignKey("events.id", ondelete="SET NULL"), nullable=True),
    )

    entry_seen_seconds: float = Field(
        default=0.0,
        sa_column=Column(Float, nullable=False, server_default="0"),
        description="Accumulated in-ROI seconds used for ENTERING debounce.",
    )

    duration_seconds: float | None = Field(
        default=None,
        sa_column=Column(
            Float,
            Computed(
                "CASE WHEN state = 'CLOSED' "
                "AND arrived_at IS NOT NULL AND departed_at IS NOT NULL "
                "THEN EXTRACT(EPOCH FROM (departed_at - arrived_at)) "
                "ELSE NULL END",
                persisted=True,
            ),
            nullable=True,
        ),
        description="GENERATED. Only set when state='CLOSED'.",
    )

    state: VisitState = Field(
        default=VisitState.ENTERING,
        sa_column=Column(
            _visit_state_enum,
            nullable=False,
            server_default="ENTERING",
        ),
    )
