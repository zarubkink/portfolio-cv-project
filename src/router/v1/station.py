from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlmodel.ext.asyncio.session import AsyncSession

from src.dependencies import get_async_session
from src.models.station import Station
from src.schemas.station import (
    StationCreate,
    StationFilter,
    StationPublic,
    StationUpdate,
)
from src.services.station_service import StationService

router = APIRouter(prefix="/stations", tags=["stations"])


def _to_public(station: Station) -> StationPublic:
    return StationPublic(
        id=station.id,
        code=station.code,
        name=station.name,
        location=station.location,
        video_dir=station.video_dir,
        roi_polygon=station.roi_polygon,
        is_entry_zone=station.is_entry_zone,
        is_active=station.is_active,
    )


@router.get("/", response_model=list[StationPublic])
async def list_stations(
    limit: int = 100,
    offset: int = 0,
    session: AsyncSession = Depends(get_async_session),
):
    service = StationService(session)
    repo = service.repo
    items = await repo.list(limit=limit, offset=offset)
    return [_to_public(s) for s in items]


@router.post("/filter", response_model=list[StationPublic])
async def filter_stations(
    filters: StationFilter,
    limit: int = 100,
    offset: int = 0,
    session: AsyncSession = Depends(get_async_session),
):
    service = StationService(session)
    items = await service.repo.get_filtered(
        options=filters.model_dump(exclude_unset=True),
        limit=limit,
        offset=offset,
    )
    return [_to_public(s) for s in items]


@router.get("/{station_id}", response_model=StationPublic)
async def get_station(
    station_id: int, session: AsyncSession = Depends(get_async_session)
):
    service = StationService(session)
    station = await service.repo.get(station_id)
    if not station:
        raise HTTPException(404, f"Station id={station_id} not found")
    return _to_public(station)


@router.post("/", response_model=StationPublic, status_code=201)
async def create_station(
    payload: StationCreate,
    session: AsyncSession = Depends(get_async_session),
):
    service = StationService(session)
    try:
        station = await service.create(payload)
    except HTTPException:
        raise
    except IntegrityError as e:
        raise HTTPException(409, f"Station code={payload.code} already exists") from e
    return _to_public(station)


@router.patch("/{station_id}", response_model=StationPublic)
async def update_station(
    station_id: int,
    payload: StationUpdate,
    session: AsyncSession = Depends(get_async_session),
):
    service = StationService(session)
    station = await service.update(station_id, payload)
    return _to_public(station)


@router.delete("/{station_id}", status_code=204)
async def delete_station(
    station_id: int, session: AsyncSession = Depends(get_async_session)
):
    service = StationService(session)
    await service.delete(station_id)
