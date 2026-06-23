from src.models.station import Station
from src.repositories.base import AsyncRepository


class StationRepository(AsyncRepository[Station]):
    def __init__(self, session):
        super().__init__(Station, session)

    async def get_by_code(self, code: str) -> Station | None:
        from sqlmodel import select

        stmt = select(Station).where(Station.code == code).limit(1)
        res = await self.session.exec(stmt)
        return res.first()
