"""Read-only endpoints that expose the current state of every tractor
and station, plus the visit history.

These endpoints are intentionally query-only: writes go through the
video handler's call into :class:`VisitService`.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession

from src.dependencies import get_async_session
from src.schemas.visit import VisitPublic
from src.services.visit_service import VisitService

router = APIRouter(prefix="/v1/status", tags=["status"])


@router.get("/tractors")
async def current_tractors(
    session: Annotated[AsyncSession, Depends(get_async_session)],
):
    """All open visits, one entry per (tractor, station)."""
    return await VisitService(session).get_current_tractors()


@router.get("/stations")
async def current_stations(
    session: Annotated[AsyncSession, Depends(get_async_session)],
):
    """Active stations with the tractors currently on each one."""
    return await VisitService(session).get_current_stations()


@router.get("/tractor/{tractor_id}")
async def current_tractor(
    tractor_id: int,
    session: Annotated[AsyncSession, Depends(get_async_session)],
):
    """Where is this tractor right now? Returns ``ABSENT`` if no open visit."""
    return await VisitService(session).get_current_tractor(tractor_id)


@router.get("/visits/history", response_model=list[VisitPublic])
async def visits_history(
    session: Annotated[AsyncSession, Depends(get_async_session)],
    tractor_id: Annotated[int | None, Query()] = None,
    station_id: Annotated[int | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    """Closed visits, newest first, optionally filtered by tractor or station."""
    rows = await VisitService(session).get_history(
        tractor_id=tractor_id,
        station_id=station_id,
        limit=limit,
        offset=offset,
    )
    return [
        VisitPublic(
            id=v.id,
            tractor_id=v.tractor_id,
            station_id=v.station_id,
            state=v.state,
            arrived_at=v.arrived_at,
            departed_at=v.departed_at,
            last_seen_at=v.last_seen_at,
            duration_seconds=v.duration_seconds,
            entry_event_id=v.entry_event_id,
            exit_event_id=v.exit_event_id,
            last_event_id=v.last_event_id,
            created_at=v.created_at,
            updated_at=v.updated_at,
        )
        for v in rows
    ]
