from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlmodel import select

from src.models.video_file import VideoFile
from src.repositories.base import AsyncRepository


def _to_naive_utc(dt: datetime) -> datetime:
    """Drop tzinfo so the value can be compared against TIMESTAMP WITHOUT TZ columns."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


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

    async def get_stale_videos(
        self, status: str, threshold_minutes: float
    ) -> list[VideoFile]:
        """Videos that have been in ``status`` longer than ``threshold_minutes``.

        We compare against ``updated_at`` because it advances on every
        state transition (PROCESSING → COMPLETED, …). For PROCESSING,
        the absence of a recent ``updated_at`` indicates the worker died.
        """
        threshold = _to_naive_utc(
            datetime.now(UTC) - timedelta(minutes=threshold_minutes)
        )
        stmt = (
            select(VideoFile)
            .where(VideoFile.status == status)
            .where(VideoFile.updated_at < threshold)
            .order_by(VideoFile.id)
        )
        res = await self.session.exec(stmt)
        return list(res.all())

    async def get_failed_videos_within_limit(
        self, max_retry_attempts: int
    ) -> list[VideoFile]:
        """FAILED videos whose retry_count is below the configured cap."""
        stmt = (
            select(VideoFile)
            .where(VideoFile.status == "FAILED")
            .where(
                (VideoFile.retry_count < max_retry_attempts)
                | VideoFile.retry_count.is_(None)
            )
            .order_by(VideoFile.id)
        )
        res = await self.session.exec(stmt)
        return list(res.all())

    async def get_failed_videos_unlimited(self) -> list[VideoFile]:
        """FAILED videos marked for unlimited retry (retry_count IS NULL)."""
        stmt = (
            select(VideoFile)
            .where(VideoFile.status == "FAILED")
            .where(VideoFile.retry_count.is_(None))
            .order_by(VideoFile.id)
        )
        res = await self.session.exec(stmt)
        return list(res.all())
