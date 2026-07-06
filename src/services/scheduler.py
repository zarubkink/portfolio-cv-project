"""Background retry scheduler for failed and stale videos.

Mirrors ``sbr/src/services/scheduler.py``. The scheduler is a process-local
singleton started during FastAPI's lifespan. Two passes per tick:

1. :meth:`_mark_stale_videos` — videos that have been in PROCESSING or
   CREATED for longer than ``stale_threshold_minutes`` are flipped to
   FAILED. This is how we recover from a worker that died mid-pipeline.
2. :meth:`_process_failed_videos` — FAILED videos whose retry_count is
   below the cap (or NULL for unlimited retries) are dispatched back
   through :func:`process_video_with_error_handling` with
   ``is_retry=True``. Once ``retry_count`` exceeds
   ``max_retry_attempts`` the video is moved to ``failed_videos/`` and
   marked INVALID.
"""

from __future__ import annotations

import asyncio
import uuid

from loguru import logger
from sqlmodel.ext.asyncio.session import AsyncSession

from src.config.scheduler import get_scheduler_settings
from src.dependencies import engine
from src.schemas.video_file import VideoStatus
from src.services.exceptions import is_retriable_without_limit
from src.services.video_handler import process_video_with_error_handling
from src.services.video_service import VideoService


class VideoRetryScheduler:
    """Process-local singleton that runs periodic retries.

    Use :data:`scheduler` rather than instantiating directly.
    """

    def __new__(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialised", False):
            return
        self._initialised = True
        self._running = False
        self._task: asyncio.Task | None = None
        self._semaphore: asyncio.Semaphore | None = None

    def _sem(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            settings = get_scheduler_settings()
            self._semaphore = asyncio.Semaphore(settings.max_concurrent_requests)
        return self._semaphore

    async def start(self) -> None:
        settings = get_scheduler_settings()
        if not settings.scheduler_activate:
            logger.warning(
                "VideoRetryScheduler is disabled by "
                "scheduler_activate=False, skipping start"
            )
            return
        if self._running:
            logger.warning("VideoRetryScheduler is already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("VideoRetryScheduler started")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("VideoRetryScheduler stopped")

    async def _run_loop(self) -> None:
        settings = get_scheduler_settings()
        interval = max(settings.retry_interval_minutes * 60, 1.0)
        while self._running:
            try:
                await self.tick()
            except asyncio.CancelledError:
                break
            except Exception as exc:  # pragma: no cover - defensive
                logger.opt(exception=exc).error("VideoRetryScheduler loop error")
            await asyncio.sleep(interval)

    async def tick(self) -> dict[str, int]:
        """Run one pass: stale detection + failed-video retry.

        Public so tests can drive the scheduler deterministically without
        waiting on the real sleep interval. Returns a small summary that
        the admin endpoint surfaces back to the caller.
        """
        stale_count = await self._mark_stale_videos()
        retried_count = await self._process_failed_videos()
        return {"stale_marked": stale_count, "retried": retried_count}

    async def _mark_stale_videos(self) -> int:
        settings = get_scheduler_settings()
        async with AsyncSession(engine) as session:
            try:
                video_service = VideoService(session)
                stale_created = await video_service.repo.get_stale_videos(
                    VideoStatus.CREATED, settings.stale_threshold_minutes
                )
                stale_processing = await video_service.repo.get_stale_videos(
                    VideoStatus.PROCESSING, settings.stale_threshold_minutes
                )
                stale = stale_created + stale_processing
                if not stale:
                    logger.debug("No stale videos found")
                    return 0
                logger.warning(
                    f"Found {len(stale)} stale videos "
                    f"(CREATED: {len(stale_created)}, "
                    f"PROCESSING: {len(stale_processing)})"
                )
                for vf in stale:
                    await video_service.update_status(
                        vf.id,
                        VideoStatus.FAILED,
                        error_message=(
                            f"Stuck in {vf.status} > "
                            f"{settings.stale_threshold_minutes}m"
                        ),
                    )
                await session.commit()
                return len(stale)
            except Exception as exc:
                logger.opt(exception=exc).error("Error marking stale videos")
                await session.rollback()
                return 0

    async def _process_failed_videos(self) -> int:
        settings = get_scheduler_settings()
        async with AsyncSession(engine) as session:
            try:
                video_service = VideoService(session)
                limited = await video_service.repo.get_failed_videos_within_limit(
                    settings.max_retry_attempts
                )
                unlimited = await video_service.repo.get_failed_videos_unlimited()
                failed = limited + unlimited
                if not failed:
                    logger.debug("No failed videos to retry")
                    return 0
                logger.info(
                    f"Retrying {len(failed)} failed videos "
                    f"(limited: {len(limited)}, unlimited: {len(unlimited)})"
                )
                targets = [
                    (vf.id, vf.storage_uri, vf.retry_count, vf.retry_count is None)
                    for vf in failed
                ]
            except Exception as exc:
                logger.opt(exception=exc).error("Error listing failed videos")
                return 0

        tasks = [asyncio.create_task(self._process_single(*t)) for t in targets]
        await asyncio.gather(*tasks, return_exceptions=True)
        return len(targets)

    async def _process_single(
        self,
        video_id: int,
        storage_uri: str,
        current_retry_count: int | None,
        is_unlimited: bool,
    ) -> None:
        settings = get_scheduler_settings()
        async with self._sem():
            task_id = uuid.uuid4().hex[:12]
            async with AsyncSession(engine) as session:
                try:
                    if is_unlimited:
                        logger.info(
                            f"[{task_id}] retrying video {video_id} "
                            f"(unlimited, retry_count={current_retry_count})"
                        )
                    else:
                        attempt = (current_retry_count or 0) + 1
                        logger.info(
                            f"[{task_id}] retrying video {video_id} "
                            f"(attempt {attempt}/{settings.max_retry_attempts})"
                        )
                    await process_video_with_error_handling(
                        task_id=task_id,
                        video_id=video_id,
                        storage_uri=storage_uri,
                        session=session,
                        is_retry=True,
                    )
                    logger.info(f"[{task_id}] video {video_id} recovered on retry")
                except Exception as exc:
                    await self._handle_retry_failure(
                        task_id=task_id,
                        video_id=video_id,
                        storage_uri=storage_uri,
                        current_retry_count=current_retry_count,
                        is_unlimited=is_unlimited,
                        error=exc,
                        session=session,
                    )

    async def _handle_retry_failure(
        self,
        *,
        task_id: str,
        video_id: int,
        storage_uri: str,
        current_retry_count: int | None,
        is_unlimited: bool,
        error: Exception,
        session: AsyncSession,
    ) -> None:
        settings = get_scheduler_settings()
        video_service = VideoService(session)
        vf = await video_service.get(video_id)
        if vf is None:
            logger.warning(f"[{task_id}] video {video_id} vanished, skipping")
            return

        if is_unlimited or is_retriable_without_limit(error):
            if not is_unlimited:
                logger.warning(
                    f"[{task_id}] video {video_id} hit a retriable error, "
                    f"leaving for unlimited retry: {error}"
                )
                await video_service.mark_unlimited_retry(video_id)
                await session.commit()
            else:
                logger.warning(
                    f"[{task_id}] video {video_id} unlimited retry failed: {error}"
                )
            return

        new_retry_count = (current_retry_count or 0) + 1
        if new_retry_count >= settings.max_retry_attempts:
            logger.warning(
                f"[{task_id}] video {video_id} exceeded max retry attempts "
                f"({new_retry_count}/{settings.max_retry_attempts}), "
                f"marking INVALID and moving to failed folder"
            )
            await video_service.mark_permanently_failed(video_id)
            await session.commit()
        else:
            logger.error(
                f"[{task_id}] video {video_id} retry failed "
                f"(retry_count={new_retry_count}): {error}"
            )
            await video_service.increment_retry_count(video_id)
            await session.commit()


scheduler = VideoRetryScheduler()


__all__ = ["VideoRetryScheduler", "scheduler"]
