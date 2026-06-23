from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlmodel.ext.asyncio.session import AsyncSession

from src.dependencies import get_async_session
from src.models.tractor import Tractor
from src.schemas.tractor import (
    TractorCreate,
    TractorFilter,
    TractorPublic,
    TractorUpdate,
)
from src.services.tractor_service import TractorService

router = APIRouter(prefix="/tractors", tags=["tractors"])


def _to_public(t: Tractor) -> TractorPublic:
    return TractorPublic(
        id=t.id,
        aruco_ids=list(t.aruco_ids or []),
        primary_aruco_id=int(t.primary_aruco_id)
        if t.primary_aruco_id is not None
        else 0,
        name=t.name,
        model=t.model,
        notes=t.notes,
        is_active=t.is_active,
        created_at=t.created_at.isoformat() if t.created_at else None,
    )


@router.get("/", response_model=list[TractorPublic])
async def list_tractors(
    limit: int = 100,
    offset: int = 0,
    session: AsyncSession = Depends(get_async_session),
):
    service = TractorService(session)
    items = await service.repo.list(limit=limit, offset=offset)
    return [_to_public(t) for t in items]


@router.post("/filter", response_model=list[TractorPublic])
async def filter_tractors(
    filters: TractorFilter,
    limit: int = 100,
    offset: int = 0,
    session: AsyncSession = Depends(get_async_session),
):
    service = TractorService(session)
    items = await service.repo.get_filtered(
        options=filters.model_dump(exclude_unset=True),
        limit=limit,
        offset=offset,
    )
    return [_to_public(t) for t in items]


@router.get("/{tractor_id}", response_model=TractorPublic)
async def get_tractor(
    tractor_id: int, session: AsyncSession = Depends(get_async_session)
):
    service = TractorService(session)
    t = await service.repo.get(tractor_id)
    if not t:
        raise HTTPException(404, f"Tractor id={tractor_id} not found")
    return _to_public(t)


@router.post("/", response_model=TractorPublic, status_code=201)
async def create_tractor(
    payload: TractorCreate,
    session: AsyncSession = Depends(get_async_session),
):
    service = TractorService(session)
    try:
        t = await service.create(payload)
    except HTTPException:
        raise
    except IntegrityError as e:
        raise HTTPException(
            409,
            f"Primary ArUco ID={payload.aruco_ids[0]} already registered",
        ) from e
    return _to_public(t)


@router.patch("/{tractor_id}", response_model=TractorPublic)
async def update_tractor(
    tractor_id: int,
    payload: TractorUpdate,
    session: AsyncSession = Depends(get_async_session),
):
    service = TractorService(session)
    t = await service.update(tractor_id, payload)
    return _to_public(t)


@router.delete("/{tractor_id}", status_code=204)
async def delete_tractor(
    tractor_id: int, session: AsyncSession = Depends(get_async_session)
):
    service = TractorService(session)
    await service.delete(tractor_id)
