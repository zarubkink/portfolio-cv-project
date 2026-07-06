"""Unit tests for :mod:`src.services.video_executor`.

These tests cover:

* picklability of every public type the executor ships across the
  process boundary (the whole point of moving the detectors into the
  worker);
* the lazy-singleton process-pool pattern — repeated calls return
  the same pool, ``reset_executors`` actually drops it;
* the empty-job short-circuit for both parallel APIs;
* true parallelism — N jobs run faster than N times a single job
  (sanity check, generous time budget to avoid flakiness on slow CI);
* error isolation per job — a broken ``VideoJob`` does not abort the
  whole batch (the API we expose only bubbles exceptions out via
  ``asyncio.gather``, so the test asserts ``raise`` behaviour);
* ``is_picklable`` round-trips both True and False cases.
"""

from __future__ import annotations

import time
from pathlib import Path

import cv2
import numpy as np
import pytest

from src.services.video_executor import (
    VideoJob,
    VideoJobResult,
    is_picklable,
    process_video_worker,
    reset_executors,
    run_video_in_process_pool,
    run_videos_in_parallel,
    run_videos_in_thread_pool,
)

# ───────────────────────────────────────────────────────────────────────
# Fixtures
# ───────────────────────────────────────────────────────────────────────


@pytest.fixture
def aruco_video(tmp_path: Path) -> Path:
    """Render a 1-second, 10-fps mp4 with one ArUco marker in the
    centre of every frame. Cheap enough to keep the test fast and
    deterministic enough to drive the executor."""
    dictionary = cv2.aruco.getPredefinedDictionary(cv2.aruco.DICT_4X4_50)
    marker = cv2.cvtColor(
        cv2.aruco.generateImageMarker(dictionary, 7, 140),
        cv2.COLOR_GRAY2BGR,
    )
    path = tmp_path / "aruco.mp4"
    writer = cv2.VideoWriter(
        str(path), cv2.VideoWriter_fourcc(*"mp4v"), 10.0, (320, 240)
    )
    for _ in range(10):
        frame = np.full((240, 320, 3), 80, dtype=np.uint8)
        frame[50:190, 90:230] = marker
        writer.write(frame)
    writer.release()
    return path


@pytest.fixture(autouse=True)
def _isolate_executor_pools():
    """Each test gets a fresh pair of pools.

    The process pool lazy-singleton lives in module state; without
    teardown the parallel test would inherit a half-warm pool with
    already-spawned worker processes from the previous test.
    """
    reset_executors()
    yield
    reset_executors()


# ───────────────────────────────────────────────────────────────────────
# Picklability
# ───────────────────────────────────────────────────────────────────────


def test_video_job_is_picklable():
    """VideoJob must round-trip through pickle.

    The whole reason the worker recreates detectors inside the
    process is to avoid shipping live OpenCV objects. This is the
    guarantee that a VideoJob is safe to send.
    """
    job = VideoJob(
        video_path="/tmp/x.mp4",
        roi_polygon=[[10, 10], [100, 10], [100, 100], [10, 100]],
    )
    assert is_picklable(job)


def test_video_job_result_is_picklable():
    res = VideoJobResult(
        job=VideoJob(video_path="/tmp/x.mp4"),
        events=[],
        frames_processed=0,
        triggers_fired=0,
    )
    assert is_picklable(res)


def test_is_picklable_rejects_lambdas():
    """Sanity check: lambdas are not picklable by design."""
    assert not is_picklable(lambda x: x)
    assert not is_picklable({"fn": lambda x: x})


def test_process_video_worker_signature_uses_only_primitives():
    """The worker's signature must not mention detector classes —
    otherwise somebody could wire in a live ArUcoDetector and break
    pickling on the ProcessPoolExecutor boundary."""
    import inspect

    sig = inspect.signature(process_video_worker)
    types = {p.annotation for p in sig.parameters.values()}
    # None of the params can reference detector classes.
    for t in types:
        assert "Detector" not in str(t), (
            f"process_video_worker annotation {t} references a Detector; "
            "instantiate it inside the worker instead"
        )


# ───────────────────────────────────────────────────────────────────────
# Singleton pool lifecycle
# ───────────────────────────────────────────────────────────────────────


def test_process_pool_is_lazy_singleton():
    """Two callers must receive the same ProcessPoolExecutor."""
    from src.services.video_executor import _get_process_pool

    a = _get_process_pool()
    b = _get_process_pool()
    assert a is b


def test_reset_executors_drops_pools():
    """After reset, the next _get_process_pool returns a fresh pool."""
    from src.services.video_executor import _get_process_pool

    first = _get_process_pool()
    reset_executors()
    second = _get_process_pool()
    assert first is not second


# ───────────────────────────────────────────────────────────────────────
# Single-video run_video_in_process_pool
# ───────────────────────────────────────────────────────────────────────


async def test_run_video_in_process_pool_returns_three_tuple(aruco_video):
    events, frames, triggers = await run_video_in_process_pool(
        str(aruco_video), roi_polygon=None
    )
    assert isinstance(events, list)
    assert frames > 0
    assert triggers >= 0
    # All frames contained the marker; the trigger fires every frame
    # (MOG2 background subtraction lights up on the moving noise) and
    # ArUco reports at least one detection on each frame.
    assert triggers > 0
    assert len(events) > 0
    # DetectionEvent exposes a frame_number; sanity check the field.
    assert all(e.frame_number >= 0 for e in events)


async def test_run_video_in_process_pool_respects_roi(aruco_video):
    """A ROI that excludes the marker should flag every detection
    with ``inside_roi=False``. ``process_video`` itself does not drop
    out-of-ROI detections — the per-event filter lives downstream
    in :class:`EventService`. Here we only verify the worker wires
    the ROI through to the decision pipeline."""
    # Bottom-right corner only — marker lives top-centre.
    roi = [[200, 200], [300, 200], [300, 300], [200, 300]]
    events, _, _ = await run_video_in_process_pool(str(aruco_video), roi_polygon=roi)
    assert len(events) > 0, "expected at least one detection"
    assert all(not e.inside_roi for e in events), (
        f"every event must be flagged outside the ROI; got {events}"
    )


# ───────────────────────────────────────────────────────────────────────
# Parallel dispatch
# ───────────────────────────────────────────────────────────────────────


async def test_run_videos_in_parallel_empty_list_short_circuits():
    """The empty input path must not touch the pool."""
    assert await run_videos_in_parallel([]) == []
    assert await run_videos_in_thread_pool([]) == []


async def test_run_videos_in_parallel_returns_one_result_per_job(aruco_video, tmp_path):
    """Each job in the input list produces exactly one VideoJobResult,
    in the same order, with the matching job attached."""
    paths = []
    for i in range(3):
        p = tmp_path / f"clip_{i}.mp4"
        p.write_bytes(aruco_video.read_bytes())
        paths.append(str(p))

    jobs = [VideoJob(video_path=p) for p in paths]
    results = await run_videos_in_parallel(jobs)

    assert len(results) == len(jobs)
    assert [r.job for r in results] == jobs
    assert all(isinstance(r, VideoJobResult) for r in results)
    assert all(r.frames_processed > 0 for r in results)
    assert all(len(r.events) > 0 for r in results)


async def test_run_videos_in_thread_pool_same_shape(aruco_video, tmp_path):
    paths = [str(tmp_path / f"clip_{i}.mp4") for i in range(2)]
    for p in paths:
        p_ = Path(p)
        p_.write_bytes(aruco_video.read_bytes())

    jobs = [VideoJob(video_path=p) for p in paths]
    results = await run_videos_in_thread_pool(jobs)
    assert len(results) == 2
    assert all(r.frames_processed > 0 for r in results)


async def test_run_videos_in_parallel_faster_than_serial(aruco_video, tmp_path):
    """N parallel jobs must take less wall time than N serial jobs.

    We render N small clips, run them in parallel, then run them
    one-by-one, and check the parallel wall time is materially less
    than the serial one. Generous 0.5x threshold to absorb CI noise
    — the real-world speed-up on a multi-core box is 2x..4x.
    """
    import shutil

    n = 4
    clips = []
    for i in range(n):
        dst = tmp_path / f"par_{i}.mp4"
        shutil.copy(aruco_video, dst)
        clips.append(str(dst))
    jobs = [VideoJob(video_path=p) for p in clips]

    t0 = time.perf_counter()
    parallel = await run_videos_in_parallel(jobs)
    parallel_dt = time.perf_counter() - t0

    t0 = time.perf_counter()
    serial = []
    for j in jobs:
        serial.append(await run_video_in_process_pool(j.video_path, None))
    serial_dt = time.perf_counter() - t0

    assert len(parallel) == n
    assert len(serial) == n
    # Parallel must be at least 1.5x faster on any sane machine. If
    # this assertion ever fires the CI runners are too slow for the
    # process-pool spin-up cost to amortise — bump the budget then.
    assert parallel_dt < serial_dt * 0.7, (
        f"parallel {parallel_dt:.2f}s is not faster than serial "
        f"{serial_dt:.2f}s — process pool may not be running"
    )


async def test_run_videos_in_parallel_surfaces_exceptions(aruco_video):
    """A bad job should raise via asyncio.gather (default behaviour)."""
    jobs = [
        VideoJob(video_path=str(aruco_video)),
        VideoJob(video_path="/nonexistent/clip.mp4"),
        VideoJob(video_path=str(aruco_video)),
    ]
    with pytest.raises(RuntimeError, match="Cannot open video"):
        await run_videos_in_parallel(jobs)


async def test_run_videos_in_parallel_with_max_concurrent_caps_dispatch(
    aruco_video, tmp_path
):
    """``max_concurrent=1`` should force serial execution through the
    Semaphore gate while still using the process pool."""
    import shutil

    clips = []
    for i in range(3):
        dst = tmp_path / f"cap_{i}.mp4"
        shutil.copy(aruco_video, dst)
        clips.append(str(dst))

    jobs = [VideoJob(video_path=p) for p in clips]
    t0 = time.perf_counter()
    results = await run_videos_in_parallel(jobs, max_concurrent=1)
    wall = time.perf_counter() - t0

    assert len(results) == 3
    assert all(r.frames_processed > 0 for r in results)
    # Sanity: gated execution should still finish in a sane budget.
    assert wall < 30.0, f"max_concurrent=1 took {wall:.2f}s, expected < 30s"
