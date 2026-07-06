from __future__ import annotations

from sqlalchemy import text
from sqlmodel import select

from src.models.visit import Visit
from src.repositories.base import AsyncRepository
from src.schemas.visit import VisitState


class VisitRepository(AsyncRepository[Visit]):
    def __init__(self, session):
        super().__init__(Visit, session)

    async def list_open(self) -> list[Visit]:
        """All visits whose state is not CLOSED."""
        stmt = select(Visit).where(Visit.state != VisitState.CLOSED).order_by(Visit.id)
        return list((await self.session.exec(stmt)).all())

    async def list_open_for_station(self, station_id: int) -> list[Visit]:
        stmt = (
            select(Visit)
            .where(Visit.station_id == station_id)
            .where(Visit.state != VisitState.CLOSED)
            .order_by(Visit.id)
        )
        return list((await self.session.exec(stmt)).all())

    async def list_open_for_tractor(self, tractor_id: int) -> list[Visit]:
        stmt = (
            select(Visit)
            .where(Visit.tractor_id == tractor_id)
            .where(Visit.state != VisitState.CLOSED)
            .order_by(Visit.id)
        )
        return list((await self.session.exec(stmt)).all())

    async def get_open_for_pair(self, tractor_id: int, station_id: int) -> Visit | None:
        stmt = (
            select(Visit)
            .where(Visit.tractor_id == tractor_id)
            .where(Visit.station_id == station_id)
            .where(Visit.state != VisitState.CLOSED)
        )
        return (await self.session.exec(stmt)).first()

    async def list_active(self) -> list[Visit]:
        """All visits in ENTERING / PRESENT / LEAVING (used for stale checks)."""
        stmt = select(Visit).where(
            Visit.state.in_([
                VisitState.ENTERING,
                VisitState.PRESENT,
                VisitState.LEAVING,
            ])
        )
        return list((await self.session.exec(stmt)).all())

    async def list_history(
        self,
        *,
        tractor_id: int | None = None,
        station_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Visit]:
        stmt = select(Visit).where(Visit.state == VisitState.CLOSED)
        if tractor_id is not None:
            stmt = stmt.where(Visit.tractor_id == tractor_id)
        if station_id is not None:
            stmt = stmt.where(Visit.station_id == station_id)
        stmt = stmt.order_by(text("arrived_at DESC")).limit(limit).offset(offset)
        return list((await self.session.exec(stmt)).all())

    async def count_by_state(self) -> dict[str, int]:
        rows = await self.session.exec(
            text("SELECT state::text, COUNT(*) FROM visits GROUP BY state")
        )
        return {state: int(count) for state, count in rows.all()}
