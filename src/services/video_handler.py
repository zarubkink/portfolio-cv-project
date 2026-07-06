"""End-to-end video handling: register, schedule, run, persist.

The function shape mirrors ``sbr/src/services/audio_handler.py``:

* :func:`handle_video` — the public entry point. Registers a row in
  ``video_files`` (CREATED) and either schedules background processing
  via FastAPI ``BackgroundTasks`` or runs it inline.
* :func:`process_video_background` — opens its own DB session and calls
  the error-handling wrapper. Used both by BackgroundTasks and by the
  retry scheduler (Stage 7).
* :func:`process_video_with_error_handling` — status transitions
  (CREATED → PROCESSING → COMPLETED / FAILED) plus retry bookkeeping.
* :func:`recognize_video` — the actual pipeline: dispatch to the
  :class:`ProcessPoolExecutor`, then map :class:`DetectionEvent` to
  ``events`` rows using the multi-marker tractor lookup.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loguru import logger
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.dependencies import engine
from src.models.station import Station
from src.schemas.event import DetectorMethod, EventCreate, EventType
from src.schemas.video_file import VideoStatus
from src.services.event_service import EventService
from src.services.exceptions import is_retriable_without_limit
from src.services.tractor_service import TractorService
from src.services.video_executor import run_video_in_process_pool
from src.services.video_service import VideoService


def _new_task_id() -> str:
    return uuid.uuid4().hex[:12]


def _bbox_to_dict(bbox: tuple[int, int, int, int] | None) -> dict | None:
    if bbox is None:
        return None
    x, y, w, h = bbox
    return {"x": x, "y": y, "w": w, "h": h}


async def recognize_video(
    task_id: str,
    video_id: int,
    storage_uri: str,
    station: Station | None,
    session: AsyncSession,
) -> tuple[int, int, int]:
    """Run the worker, persist events, update counters.

    Returns ``(events_count, frames_processed, triggers_fired)``.
    """
    roi_polygon: list[list[int]] | None = (
        list(station.roi_polygon) if station and station.roi_polygon else None
    )

    video_service = VideoService(session)
    tractor_service = TractorService(session)
    event_service = EventService(session)

    events, frames_processed, triggers_fired = await run_video_in_process_pool(
        storage_uri, roi_polygon
    )

    logger.info(
        f"[{task_id}] video={video_id} frames={frames_processed} "
        f"triggers={triggers_fired} events={len(events)}"
    )

    await video_service.update_counters(
        video_id,
        frames_processed=frames_processed,
        triggers_fired=triggers_fired,
        events_found=len(events),
    )

    if not events:
        return 0, frames_processed, triggers_fired

    aruco_to_tractor: dict[int, int] = {}
    payloads: list[EventCreate] = []
    for ev in events:
        tractor_id: int | None = None
        if ev.aruco_id is not None:
            if ev.aruco_id in aruco_to_tractor:
                tractor_id = aruco_to_tractor[ev.aruco_id]
            else:
                tractor = await tractor_service.get_by_any_aruco_id(ev.aruco_id)
                if tractor is not None:
                    tractor_id = tractor.id
                    aruco_to_tractor[ev.aruco_id] = tractor_id
        payloads.append(
            EventCreate(
                video_file_id=video_id,
                tractor_id=tractor_id,
                aruco_id=ev.aruco_id,
                event_type=EventType.DETECTED,
                detector_method=DetectorMethod.ARUCO,
                inside_roi=ev.inside_roi,
                frame_number=ev.frame_number,
                timestamp_in_video=ev.timestamp_in_video,
                wall_clock_at=datetime.now(UTC),
                confidence=ev.confidence,
                bbox=_bbox_to_dict(ev.bbox),
                detector_metadata=ev.detector_metadata,
            )
        )

    await event_service.create_many(payloads)
    return len(events), frames_processed, triggers_fired


async def process_video_with_error_handling(
    task_id: str,
    video_id: int,
    storage_uri: str,
    session: AsyncSession,
    *,
    is_retry: bool = False,
) -> None:
    """Run :func:`recognize_video` with status transitions and rollback."""
    video_service = VideoService(session)
    await video_service.update_status(video_id, VideoStatus.PROCESSING)

    try:
        await session.commit()
    except Exception:
        await session.rollback()
        raise

    video = await video_service.get(video_id)
    station: Station | None = None
    if video is not None and video.station_id is not None:
        station = (
            await session.exec(select(Station).where(Station.id == video.station_id))
        ).first()

    try:
        events_count, frames, triggers = await recognize_video(
            task_id=task_id,
            video_id=video_id,
            storage_uri=storage_uri,
            station=station,
            session=session,
        )
        await video_service.update_status(video_id, VideoStatus.COMPLETED)
        await session.commit()
        logger.info(
            f"[{task_id}] video={video_id} COMPLETED "
            f"(events={events_count}, frames={frames}, triggers={triggers})"
        )
    except Exception as exc:
        await session.rollback()
        if is_retry:
            if is_retriable_without_limit(exc):
                logger.warning(
                    f"[{task_id}] retriable error, leaving for unlimited retry: {exc}"
                )
            else:
                await video_service.increment_retry_count(video_id)
                await session.commit()
        else:
            await video_service.update_status(
                video_id,
                VideoStatus.FAILED,
                error_message=str(exc),
            )
            await session.commit()
        logger.error(f"[{task_id}] video={video_id} failed: {exc}")
        raise


async def process_video_background(
    task_id: str,
    video_id: int,
    storage_uri: str,
) -> None:
    """Background entry point — opens its own DB session."""
    logger.info(f"[{task_id}] background processing started for video={video_id}")
    async with AsyncSession(engine) as session:
        try:
            await process_video_with_error_handling(
                task_id=task_id,
                video_id=video_id,
                storage_uri=storage_uri,
                session=session,
            )
        except Exception as exc:
            logger.opt(exception=exc).error(
                f"[{task_id}] background processing failed for video={video_id}"
            )
            try:
                video_service = VideoService(session)
                await video_service.update_status(
                    video_id,
                    VideoStatus.FAILED,
                    error_message=str(exc),
                )
                await session.commit()
            except Exception:
                await session.rollback()


async def handle_video(
    task_id: str | None,
    filepath: str | Path,
    station_id: int | None,
    started_at: datetime,
    ended_at: datetime,
    session: AsyncSession,
    background_tasks: Any = None,
) -> dict:
    """Public entry point used by both ``/upload`` and ``/handle``."""
    if task_id is None:
        task_id = _new_task_id()
    video_service = VideoService(session)
    vf = await video_service.create_and_commit(
        station_id=station_id,
        storage_uri=str(filepath),
        started_at=started_at,
        ended_at=ended_at,
    )

    if background_tasks is not None:
        background_tasks.add_task(
            process_video_background,
            task_id,
            vf.id,
            vf.storage_uri,
        )
        logger.info(f"[{task_id}] video={vf.id} queued for background processing")
        return {
            "status": "queued",
            "video_id": vf.id,
            "task_id": task_id,
        }

    await process_video_with_error_handling(
        task_id=task_id,
        video_id=vf.id,
        storage_uri=vf.storage_uri,
        session=session,
    )
    refreshed = await video_service.get(vf.id)
    return {
        "status": "ok",
        "video_id": vf.id,
        "task_id": task_id,
        "video_status": (refreshed.status.value if refreshed is not None else None),
    }


__all__ = [
    "handle_video",
    "process_video_background",
    "process_video_with_error_handling",
    "recognize_video",
]
