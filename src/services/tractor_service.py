from fastapi import HTTPException
from loguru import logger
from sqlalchemy.exc import IntegrityError
from sqlmodel.ext.asyncio.session import AsyncSession

from src.models.tractor import Tractor
from src.repositories.tractor import TractorRepository
from src.schemas.tractor import TractorCreate, TractorUpdate


class TractorService:
    def __init__(self, session: AsyncSession):
        self.repo = TractorRepository(session)

    async def create(self, payload: TractorCreate) -> Tractor:
        if not payload.aruco_ids:
            raise HTTPException(422, "aruco_ids must contain at least 1 element")
        try:
            return await self.repo.create(payload.model_dump(exclude_unset=True))
        except IntegrityError as e:
            logger.warning(f"Tractor create integrity error: {e}")
            raise HTTPException(
                409,
                f"Primary ArUco ID={payload.aruco_ids[0]} already registered",
            ) from e

    async def update(self, tractor_id: int, payload: TractorUpdate) -> Tractor:
        tractor = await self.repo.get(tractor_id)
        if tractor is None:
            raise HTTPException(404, f"Tractor id={tractor_id} not found")
        try:
            return await self.repo.update(
                tractor, payload.model_dump(exclude_unset=True)
            )
        except IntegrityError as e:
            raise HTTPException(
                409,
                "Primary ArUco ID conflict with another tractor",
            ) from e

    async def delete(self, tractor_id: int) -> None:
        tractor = await self.repo.get(tractor_id)
        if tractor is None:
            raise HTTPException(404)
        await self.repo.delete(tractor)

    async def get_by_any_aruco_id(self, aruco_id: int) -> Tractor | None:
        return await self.repo.get_by_any_aruco_id(aruco_id)
