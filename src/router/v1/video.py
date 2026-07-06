import shutil
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
)
from sqlmodel.ext.asyncio.session import AsyncSession

from src.config.database import settings
from src.dependencies import get_async_session
from src.models.video_file import VideoFile
from src.services.video_handler import handle_video
from src.services.video_service import VideoService

router = APIRouter(prefix="/videos", tags=["videos"])


def _to_public(v: VideoFile) -> dict:
    return {
        "id": v.id,
        "station_id": v.station_id,
        "storage_uri": v.storage_uri,
        "started_at": v.started_at.isoformat() if v.started_at else None,
        "ended_at": v.ended_at.isoformat() if v.ended_at else None,
        "fps": v.fps,
        "width": v.width,
        "height": v.height,
        "duration_seconds": v.duration_seconds,
        "status": v.status.value if hasattr(v.status, "value") else v.status,
        "retry_count": v.retry_count,
        "error_message": v.error_message,
        "frames_processed": v.frames_processed,
        "triggers_fired": v.triggers_fired,
        "created_at": v.created_at.isoformat() if v.created_at else None,
        "updated_at": v.updated_at.isoformat() if v.updated_at else None,
    }


@router.get("/")
async def list_videos(
    limit: int = 100,
    offset: int = 0,
    session: AsyncSession = Depends(get_async_session),
):
    service = VideoService(session)
    items = await service.list(limit=limit, offset=offset)
    return [_to_public(v) for v in items]


@router.get("/{video_id}")
async def get_video(video_id: int, session: AsyncSession = Depends(get_async_session)):
    service = VideoService(session)
    v = await service.get(video_id)
    if not v:
        raise HTTPException(404, f"VideoFile id={video_id} not found")
    return _to_public(v)


@router.post("/upload", status_code=201)
async def upload_video(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    station_id: int | None = Form(None),
    started_at: datetime = Form(...),
    ended_at: datetime = Form(...),
    session: AsyncSession = Depends(get_async_session),
):
    """Multipart upload of a video. Saves to cold storage under the
    SHA-256 content hash and schedules background processing.
    """
    if not file.filename:
        raise HTTPException(422, "file.filename required")

    ext = Path(file.filename).suffix.lower()
    if ext not in {".mp4", ".avi", ".mov", ".mkv"}:
        raise HTTPException(422, f"Unsupported extension: {ext}")

    settings.videos_storage.mkdir(parents=True, exist_ok=True)
    tmp_path = settings.videos_storage / f"tmp_{uuid.uuid4().hex}{ext}"
    try:
        with tmp_path.open("wb") as out:
            shutil.copyfileobj(file.file, out)

        service = VideoService(session)
        vf = await service.create_and_commit(
            storage_uri=str(tmp_path),
            started_at=started_at,
            ended_at=ended_at,
            station_id=station_id,
        )
        content_hash_hex = vf.content_hash.hex()
        final_path = settings.videos_storage / f"{content_hash_hex}{ext}"
        if not final_path.exists():
            tmp_path.rename(final_path)
        else:
            tmp_path.unlink(missing_ok=True)
        if final_path != Path(vf.storage_uri):
            await service.repo.update(vf, {"storage_uri": str(final_path)})

        background_tasks.add_task(_background_process, vf.id, str(final_path))
        return {
            **_to_public(vf),
            "background": True,
        }
    except HTTPException:
        tmp_path.unlink(missing_ok=True)
        raise
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


async def _background_process(video_id: int, storage_uri: str) -> None:
    """Bridge between FastAPI BackgroundTasks and our async handler.

    BackgroundTasks runs sync and async callables directly in the loop,
    so we just forward to the handler.
    """
    from src.services.video_handler import process_video_background

    await process_video_background(uuid.uuid4().hex[:12], video_id, storage_uri)


@router.post("/handle")
async def handle_video_endpoint(
    background_tasks: BackgroundTasks,
    filepath: str = Form(...),
    station_id: int | None = Form(None),
    station_code: str | None = Form(None),
    started_at: datetime = Form(...),
    ended_at: datetime = Form(...),
    session: AsyncSession = Depends(get_async_session),
):
    """Server-side file path → register and process in background.

    Used by the ingestion watcher once the clip is in cold storage.
    Accepts either ``station_id`` or ``station_code`` (the watcher uses
    the latter because the source filename encodes the station).
    """
    if station_id is None and station_code:
        from src.repositories.station import StationRepository

        station_repo = StationRepository(session)
        station = await station_repo.get_by_code(station_code)
        if station is None:
            raise HTTPException(422, f"Unknown station_code={station_code!r}")
        station_id = station.id
    return await handle_video(
        task_id=None,
        filepath=filepath,
        station_id=station_id,
        started_at=started_at,
        ended_at=ended_at,
        session=session,
        background_tasks=background_tasks,
    )
