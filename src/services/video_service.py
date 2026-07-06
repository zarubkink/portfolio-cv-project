import asyncio
from datetime import UTC, datetime
from pathlib import Path

from fastapi import HTTPException, status
from loguru import logger
from sqlmodel.ext.asyncio.session import AsyncSession

from src.config.database import settings
from src.models.video_file import VideoFile
from src.repositories.video_file import VideoFileRepository
from src.schemas.video_file import VideoStatus
from src.utils import hash_large_file


def _to_naive_utc(dt: datetime) -> datetime:
    """Колонки `TIMESTAMP WITHOUT TIME ZONE` хранят naive; приводим к UTC-naive."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


class VideoService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.repo = VideoFileRepository(session)

    async def get(self, video_id: int) -> VideoFile | None:
        return await self.repo.get(video_id)

    async def get_by_hash(self, content_hash: bytes) -> VideoFile | None:
        return await self.repo.get_by_hash(content_hash)

    async def list(self, limit: int = 100, offset: int = 0) -> list[VideoFile]:
        return await self.repo.list(limit=limit, offset=offset)

    async def create_and_commit(
        self,
        storage_uri: str,
        started_at: datetime,
        ended_at: datetime,
        station_id: int | None = None,
        fps: float | None = None,
        width: int | None = None,
        height: int | None = None,
        duration_seconds: float | None = None,
    ) -> VideoFile:
        """Создать VideoFile после проверок (SHA-256, дедуп, валидация времени).

        Хэш считается в ThreadPool через asyncio.to_thread, чтобы не блокировать loop.
        """
        started_naive = _to_naive_utc(started_at)
        ended_naive = _to_naive_utc(ended_at)
        if started_naive >= ended_naive:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                "started_at must be < ended_at",
            )

        path = Path(storage_uri)
        if not path.exists():
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                f"Video file does not exist: {storage_uri}",
            )

        content_hash = await asyncio.to_thread(hash_large_file, path)

        existing = await self.repo.get_by_hash(content_hash)
        if existing is not None:
            raise HTTPException(
                status.HTTP_406_NOT_ACCEPTABLE,
                f"Video already exists (hash={content_hash.hex()[:16]}..., "
                f"video_id={existing.id})",
            )

        vf = await self.repo.create({
            "station_id": station_id,
            "storage_uri": str(path),
            "content_hash": content_hash,
            "started_at": started_naive,
            "ended_at": ended_naive,
            "fps": fps,
            "width": width,
            "height": height,
            "duration_seconds": duration_seconds,
            "status": VideoStatus.CREATED,
        })
        logger.info(f"VideoFile id={vf.id} created: {storage_uri}")
        return vf

    async def update_status(
        self,
        vf_id: int,
        new_status: VideoStatus,
        error_message: str | None = None,
    ) -> VideoFile | None:
        data: dict = {"status": new_status}
        if error_message is not None:
            data["error_message"] = error_message
        return await self.repo.update_by_id(vf_id, data)

    async def update_counters(
        self,
        vf_id: int,
        frames_processed: int | None = None,
        triggers_fired: int | None = None,
        events_found: int | None = None,
    ) -> VideoFile | None:
        data: dict = {}
        if frames_processed is not None:
            data["frames_processed"] = frames_processed
        if triggers_fired is not None:
            data["triggers_fired"] = triggers_fired
        if events_found is not None:
            data["events_found"] = events_found
        if not data:
            return await self.repo.get(vf_id)
        return await self.repo.update_by_id(vf_id, data)

    async def increment_retry_count(self, vf_id: int) -> VideoFile | None:
        vf = await self.repo.get(vf_id)
        if vf is None:
            return None
        return await self.repo.update(vf, {"retry_count": vf.retry_count + 1})

    async def move_to_failed(self, vf_id: int) -> str | None:
        """Перенести видео в failed_videos/, вернуть новый путь."""
        import shutil

        vf = await self.repo.get(vf_id)
        if vf is None or not Path(vf.storage_uri).exists():
            return None
        src = Path(vf.storage_uri)
        dst = settings.failed_videos_folder / f"{src.name}.{vf.id}"
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        await self.repo.update(vf, {"storage_uri": str(dst)})
        logger.warning(f"VideoFile id={vf.id} moved to failed: {dst}")
        return str(dst)

    async def mark_unlimited_retry(self, vf_id: int) -> VideoFile | None:
        """Mark a FAILED video for unlimited retries (retry_count → NULL)."""
        return await self.repo.update_by_id(vf_id, {"retry_count": None})

    async def mark_permanently_failed(
        self,
        vf_id: int,
        new_status: VideoStatus = VideoStatus.INVALID,
    ) -> VideoFile | None:
        """Move the file to failed_videos/ and flip status to INVALID.

        Idempotent: if the file is already in failed_videos/, only the
        status is updated. Used by the retry scheduler once
        ``max_retry_attempts`` is exceeded.
        """
        moved = await self.move_to_failed(vf_id)
        vf = await self.repo.get(vf_id)
        if vf is None:
            return None
        data: dict = {
            "status": new_status,
            "error_message": "max_retry_attempts exceeded",
        }
        if moved:
            data["storage_uri"] = moved
        return await self.repo.update(vf, data)
