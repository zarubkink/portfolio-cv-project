from __future__ import annotations

from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.models.event import Event
from src.repositories.base import AsyncRepository


class EventRepository(AsyncRepository[Event]):
    def __init__(self, session: AsyncSession):
        super().__init__(Event, session)

    async def list_by_video(self, video_id: int) -> list[Event]:
        stmt = (
            select(Event)
            .where(Event.video_file_id == video_id)
            .order_by(Event.frame_number, Event.id)
        )
        res = await self.session.exec(stmt)
        return list(res.all())

    async def count_by_video(self, video_id: int) -> int:
        from sqlalchemy import func

        stmt = (
            select(func.count())
            .select_from(Event)
            .where(Event.video_file_id == video_id)
        )
        res = await self.session.exec(stmt)
        return int(res.one() or 0)
