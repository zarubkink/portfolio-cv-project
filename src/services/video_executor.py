"""Picklable workers for the OpenCV-heavy video pipeline.

Mirrors the lazy-singleton pattern from
``sbr/src/services/search_executor.py``. The worker function must take
only picklable arguments (str, list-of-lists, primitives) because it is
shipped to a :class:`ProcessPoolExecutor` worker process. The
``TriggerDetector`` and ``ArucoDetector`` instances are recreated inside
the worker so nothing OpenCV-related crosses the process boundary; only
the configuration dict travels with the job.

Why :class:`ProcessPoolExecutor` and not a thread pool? ArUco is CPU
bound and OpenCV's Python bindings release the GIL only intermittently
— the threads in a :class:`ThreadPoolExecutor` therefore serialise on
the same core. A process pool gives true parallelism: each worker gets
its own interpreter and its own MOG2 / ArUco dictionaries.

A :class:`ThreadPoolExecutor` variant is still shipped (see
:func:`run_videos_in_thread_pool`) so the integration test and the
benchmark script in ``scripts/benchmark_executor.py`` can measure the
real-world speed-up.
"""

from __future__ import annotations

import asyncio
import pickle
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
)
from dataclasses import dataclass
from typing import Any

from src.config.threads import threads_settings
from src.services.video_processor import (
    DetectionEvent,
    process_video,
)


@dataclass(frozen=True, slots=True)
class VideoJob:
    """One unit of work for the parallel executor.

    The fields are intentionally picklable so the same instance can
    cross the process boundary without an explicit ``copy_reg``.
    """

    video_path: str
    roi_polygon: list[list[int]] | None = None


# Sentinel result type — workers return ``Result`` so the parallel API
# can use a single ``asyncio.gather`` return shape.
@dataclass(frozen=True, slots=True)
class VideoJobResult:
    """The outcome of one :class:`VideoJob`."""

    job: VideoJob
    events: list[DetectionEvent]
    frames_processed: int
    triggers_fired: int
    error: BaseException | None = None


_process_pool: ProcessPoolExecutor | None = None
_thread_pool: ThreadPoolExecutor | None = None


def _get_process_pool() -> ProcessPoolExecutor:
    global _process_pool
    if _process_pool is None:
        _process_pool = ProcessPoolExecutor(
            max_workers=threads_settings.max_process_workers
        )
    return _process_pool


def _get_thread_pool() -> ThreadPoolExecutor:
    global _thread_pool
    if _thread_pool is None:
        _thread_pool = ThreadPoolExecutor(
            max_workers=threads_settings.max_process_workers,
            thread_name_prefix="video-worker",
        )
    return _thread_pool


def reset_executors() -> None:
    """Drop both singleton pools. Test-only convenience."""
    global _process_pool, _thread_pool
    for pool in (_process_pool, _thread_pool):
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
    _process_pool = None
    _thread_pool = None


def process_video_worker(
    video_path: str,
    roi_polygon: list[list[int]] | None,
) -> tuple[list[DetectionEvent], int, int]:
    """Top-level picklable function executed in a worker process.

    Detector instances are built here so they live in the worker
    interpreter — only the configuration values cross the pickle
    boundary.
    """
    events, frames, triggers = process_video(
        video_path,
        roi_polygon=roi_polygon,
    )
    return events, frames, triggers


def _wrap_job(job: VideoJob) -> VideoJobResult:
    """ThreadPool adapter: turn the worker's tuple into VideoJobResult."""
    try:
        events, frames, triggers = process_video_worker(job.video_path, job.roi_polygon)
        return VideoJobResult(
            job=job,
            events=events,
            frames_processed=frames,
            triggers_fired=triggers,
        )
    except BaseException as exc:  # pragma: no cover - defensive
        return VideoJobResult(
            job=job,
            events=[],
            frames_processed=0,
            triggers_fired=0,
            error=exc,
        )


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


async def run_videos_in_parallel(
    jobs: list[VideoJob],
    *,
    max_concurrent: int | None = None,
) -> list[VideoJobResult]:
    """Run several :class:`VideoJob` instances in parallel via the
    shared :class:`ProcessPoolExecutor`.

    Each job is dispatched with ``loop.run_in_executor`` and the
    futures are awaited together with ``asyncio.gather``. We pass
    ``return_exceptions=False`` so an unhandled crash is surfaced
    immediately; if you want best-effort semantics, wrap your own
    try/except around the worker call site.

    Args:
        jobs: list of :class:`VideoJob`. Empty list short-circuits
            to ``[]``.
        max_concurrent: optional cap on jobs in flight. Defaults to
            ``None`` (the pool's ``max_workers`` already serialises
            beyond that). When provided, a Semaphore gates the
            dispatch loop.
    """
    if not jobs:
        return []

    pool = _get_process_pool()
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_concurrent) if max_concurrent else None

    async def _dispatch(job: VideoJob) -> VideoJobResult:
        if semaphore is not None:
            async with semaphore:
                fut = loop.run_in_executor(
                    pool, process_video_worker, job.video_path, job.roi_polygon
                )
                events, frames, triggers = await fut
        else:
            fut = loop.run_in_executor(
                pool, process_video_worker, job.video_path, job.roi_polygon
            )
            events, frames, triggers = await fut
        return VideoJobResult(
            job=job,
            events=events,
            frames_processed=frames,
            triggers_fired=triggers,
        )

    return await asyncio.gather(*[_dispatch(j) for j in jobs])


async def run_videos_in_thread_pool(
    jobs: list[VideoJob],
) -> list[VideoJobResult]:
    """Run several :class:`VideoJob` instances in a thread pool.

    This is the GIL-bound baseline the benchmark compares against.
    Each :class:`VideoJob` is dispatched to a worker thread which
    builds its own detector stack and processes the clip.
    """
    if not jobs:
        return []

    pool = _get_thread_pool()
    loop = asyncio.get_running_loop()

    async def _dispatch(job: VideoJob) -> Any:
        return await loop.run_in_executor(pool, _wrap_job, job)

    return await asyncio.gather(*[_dispatch(j) for j in jobs])


def is_picklable(value: Any) -> bool:
    """Test helper: round-trip ``value`` through pickle.dumps/loads.

    The check exists because ``ProcessPoolExecutor`` only works when
    every argument and return value can cross the process boundary.
    Lifting the trigger/ArUco objects into the worker (see
    :func:`process_video_worker`) means we never need to ship a
    live detector — but if a future caller adds one by mistake, this
    helper catches it in a unit test rather than at runtime.
    """
    try:
        pickle.loads(pickle.dumps(value))
    except Exception:
        return False
    return True


__all__ = [
    "VideoJob",
    "VideoJobResult",
    "process_video_worker",
    "reset_executors",
    "run_video_in_process_pool",
    "run_videos_in_parallel",
    "run_videos_in_thread_pool",
    "is_picklable",
]
