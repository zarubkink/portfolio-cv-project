from sqlalchemy import text
from sqlmodel import select

from src.models.video_file import VideoFile
from src.repositories.base import AsyncRepository


class VideoFileRepository(AsyncRepository[VideoFile]):
    def __init__(self, session):
        super().__init__(VideoFile, session)

    async def get_by_hash(self, content_hash: bytes) -> VideoFile | None:
        stmt = select(VideoFile).where(VideoFile.content_hash == content_hash).limit(1)
        res = await self.session.exec(stmt)
        return res.first()

    async def list_by_station(
        self, station_id: int, limit: int = 100, offset: int = 0
    ) -> list[VideoFile]:
        stmt = (
            select(VideoFile)
            .where(VideoFile.station_id == station_id)
            .order_by(VideoFile.id.desc())
            .limit(limit)
            .offset(offset)
        )
        res = await self.session.exec(stmt)
        return res.all()

    async def count_by_status(self) -> dict[str, int]:
        rows = await self.session.exec(
            text("SELECT status::text, COUNT(*) FROM video_files GROUP BY status")
        )
        return {status: int(count) for status, count in rows.all()}
