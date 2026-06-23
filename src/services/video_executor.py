"""Picklable worker for the OpenCV-heavy video pipeline.

Mirrors the lazy-singleton pattern from
``sbr/src/services/search_executor.py``. The worker function must take
only picklable arguments (str, list-of-lists, primitives) because it is
shipped to a :class:`ProcessPoolExecutor` worker process.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor

from src.config.threads import threads_settings
from src.services.video_processor import (
    DetectionEvent,
    process_video,
)

_process_pool: ProcessPoolExecutor | None = None


def _get_process_pool() -> ProcessPoolExecutor:
    global _process_pool
    if _process_pool is None:
        _process_pool = ProcessPoolExecutor(
            max_workers=threads_settings.max_process_workers
        )
    return _process_pool


def process_video_worker(
    video_path: str,
    roi_polygon: list[list[int]] | None,
) -> tuple[list[DetectionEvent], int, int]:
    """Top-level picklable function executed in a worker process."""
    events, frames, triggers = process_video(
        video_path,
        roi_polygon=roi_polygon,
    )
    return events, frames, triggers


async def run_video_in_process_pool(
    video_path: str,
    roi_polygon: list[list[int]] | None,
) -> tuple[list[DetectionEvent], int, int]:
    """Run :func:`process_video_worker` in the shared process pool."""
    pool = _get_process_pool()
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        pool, process_video_worker, video_path, roi_polygon
    )
