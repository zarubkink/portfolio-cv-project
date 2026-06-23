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
from src.schemas.video_file import VideoStatus
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
    file: UploadFile = File(...),
    station_id: int | None = Form(None),
    started_at: datetime = Form(...),
    ended_at: datetime = Form(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    session: AsyncSession = Depends(get_async_session),
):
    """Multipart-загрузка видео. Сохраняем в cold_storage и регистрируем в БД.

    В Этапе 3 НЕ запускаем обработку — только создаём запись со статусом CREATED.
    Обработка подключится в Этапе 6 (через background_tasks).
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
        # Переименуем под content_hash — идемпотентное хранение.
        content_hash_hex = vf.content_hash.hex()
        final_path = settings.videos_storage / f"{content_hash_hex}{ext}"
        if not final_path.exists():
            tmp_path.rename(final_path)
        else:
            tmp_path.unlink(missing_ok=True)
        if final_path != Path(vf.storage_uri):
            await service.repo.update(vf, {"storage_uri": str(final_path)})
        return _to_public(vf)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


@router.post("/handle")
async def handle_video(
    filepath: str = Form(...),
    station_id: int | None = Form(None),
    started_at: datetime = Form(...),
    ended_at: datetime = Form(...),
    session: AsyncSession = Depends(get_async_session),
):
    """Эндпоинт для ingestion: путь уже на сервере.

    В Этапе 3 — только регистрация в БД, без обработки.
    Обработка будет добавлена в Этапе 6 (POST /handle с background_tasks).
    """
    service = VideoService(session)
    vf = await service.create_and_commit(
        storage_uri=filepath,
        started_at=started_at,
        ended_at=ended_at,
        station_id=station_id,
    )
    return {
        "status": "queued" if vf.status == VideoStatus.CREATED else "ok",
        "video_id": vf.id,
        **_to_public(vf),
    }
