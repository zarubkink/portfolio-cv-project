from loguru import logger
from sqlalchemy.exc import IntegrityError
from sqlmodel.ext.asyncio.session import AsyncSession

from src.models.station import Station
from src.repositories.station import StationRepository
from src.schemas.station import StationCreate, StationUpdate


class StationService:
    def __init__(self, session: AsyncSession):
        self.repo = StationRepository(session)

    async def create(self, payload: StationCreate) -> Station:
        if await self.repo.get_by_code(payload.code):
            from fastapi import HTTPException

            raise HTTPException(409, f"Station with code={payload.code} already exists")
        return await self.repo.create(payload.model_dump(exclude_unset=True))

    async def update(self, station_id: int, payload: StationUpdate) -> Station:
        station = await self.repo.get(station_id)
        if station is None:
            from fastapi import HTTPException

            raise HTTPException(404, f"Station id={station_id} not found")
        try:
            return await self.repo.update(
                station, payload.model_dump(exclude_unset=True)
            )
        except IntegrityError as e:
            logger.warning(f"Station update integrity error: {e}")
            raise

    async def delete(self, station_id: int) -> None:
        station = await self.repo.get(station_id)
        if station is None:
            from fastapi import HTTPException

            raise HTTPException(404)
        await self.repo.delete(station)
