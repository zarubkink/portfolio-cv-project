from sqlalchemy import text
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.models.tractor import Tractor
from src.repositories.base import AsyncRepository


class TractorRepository(AsyncRepository[Tractor]):
    def __init__(self, session: AsyncSession):
        super().__init__(Tractor, session)

    async def get_by_any_aruco_id(self, aruco_id: int) -> Tractor | None:
        """Поиск трактора по любому из его aruco_ids.

        Использует GIN-индекс ix_tractors_aruco_ids, если он создан миграцией."""
        stmt = select(Tractor).where(text("$1 = ANY(aruco_ids)")).params(aruco_id)
        res = await self.session.exec(stmt)
        return res.first()

    async def get_by_primary_aruco_id(self, aruco_id: int) -> Tractor | None:
        stmt = select(Tractor).where(Tractor.primary_aruco_id == aruco_id).limit(1)
        res = await self.session.exec(stmt)
        return res.first()
