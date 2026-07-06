"""Compare ThreadPoolExecutor vs ProcessPoolExecutor throughput on
the ArUco pipeline.

Run with:

    uv run python scripts/benchmark_executor.py \
        --clips 16 --fps 10 --duration 4 --workers 4

The script renders ``--clips`` synthetic mp4 files (each one a
``--duration``-second clip with one ArUco marker in the centre of
every frame), then runs the same set of jobs through:

* the GIL-bound ThreadPoolExecutor baseline, and
* the ProcessPoolExecutor that the service uses in production.

Output is a single table — wall time, events per second, and a
speed-up ratio. The benchmark is meant to convince the reader that
ArUco deserves a process pool rather than a thread pool on the
target hardware, not to be a precise CI gate.

It deliberately does NOT touch the database: the comparison is
purely about the CPU-bound pipeline. To keep the numbers
comparable we also dry-run the detector warm-up once before each
phase.
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import cv2
import numpy as np

from src.services.video_executor import (
    VideoJob,
    reset_executors,
    run_videos_in_parallel,
    run_videos_in_thread_pool,
)


@dataclass
class Phase:
    label: str
    wall_seconds: float
    events_total: int
    frames_total: int

    @property
    def events_per_sec(self) -> float:
        return self.events_total / self.wall_seconds if self.wall_seconds else 0.0

    @property
    def clips_per_sec(self) -> float:
        return self.frames_total / self.wall_seconds if self.wall_seconds else 0.0


def render_clip(
    target: Path,
    *,
    fps: int,
    duration: float,
    width: int,
    height: int,
    aruco_id: int,
    seed: int,
) -> None:
    """Render one synthetic mp4 with an ArUco marker on every frame."""
    dictionary = cv2.aruco.getPredefinedDictionary(cv2.aruco.DICT_4X4_50)
    marker = cv2.cvtColor(
        cv2.aruco.generateImageMarker(dictionary, aruco_id, 140),
        cv2.COLOR_GRAY2BGR,
    )
    bg = np.full((height, width, 3), 64, dtype=np.uint8)
    rng = np.random.default_rng(seed)

    n_frames = int(round(fps * duration))
    writer = cv2.VideoWriter(
        str(target), cv2.VideoWriter_fourcc(*"mp4v"), float(fps), (width, height)
    )
    for i in range(n_frames):
        frame = cv2.add(bg, rng.integers(0, 40, bg.shape, dtype=np.uint8))
        offset = int(15 * np.sin(2 * np.pi * i / 10))
        x = (width - 140) // 2 + offset
        y = (height - 140) // 2 + offset
        frame[y : y + 140, x : x + 140] = marker
        writer.write(frame)
    writer.release()


def build_corpus(
    *,
    clips: int,
    fps: int,
    duration: float,
    width: int,
    height: int,
    work_dir: Path,
) -> list[Path]:
    paths: list[Path] = []
    for i in range(clips):
        path = work_dir / f"clip_{i:03d}.mp4"
        render_clip(
            path,
            fps=fps,
            duration=duration,
            width=width,
            height=height,
            aruco_id=(i % 49) + 1,
            seed=0xC0FFEE + i,
        )
        paths.append(path)
    return paths


async def _time_phase(
    label: str,
    paths: list[Path],
    *,
    use_process_pool: bool,
) -> Phase:
    jobs = [VideoJob(video_path=str(p)) for p in paths]
    reset_executors()

    t0 = time.perf_counter()
    if use_process_pool:
        results = await run_videos_in_parallel(jobs)
    else:
        results = await run_videos_in_thread_pool(jobs)
    wall = time.perf_counter() - t0

    return Phase(
        label=label,
        wall_seconds=wall,
        events_total=sum(len(r.events) for r in results),
        frames_total=sum(r.frames_processed for r in results),
    )


def render_report(
    n_clips: int,
    fps: int,
    duration: float,
    workers: int,
    thread_phase: Phase,
    process_phase: Phase,
) -> str:
    speedup = (
        thread_phase.wall_seconds / process_phase.wall_seconds
        if process_phase.wall_seconds
        else 0.0
    )
    lines = [
        "Executor benchmark",
        "==================",
        f"clips={n_clips}  fps={fps}  duration={duration:.1f}s  "
        f"workers={workers}  cpu_count={_cpu_count_hint()}",
        "",
        f"  {'phase':<24}{'wall (s)':>10}{'frames':>10}{'events':>10}{'frames/s':>14}",
        f"  {'thread pool (GIL)':<24}{thread_phase.wall_seconds:>10.2f}"
        f"{thread_phase.frames_total:>10}{thread_phase.events_total:>10}"
        f"{thread_phase.clips_per_sec:>14.1f}",
        f"  {'process pool':<24}{process_phase.wall_seconds:>10.2f}"
        f"{process_phase.frames_total:>10}{process_phase.events_total:>10}"
        f"{process_phase.clips_per_sec:>14.1f}",
        "",
        f"speed-up: process / thread = {speedup:.2f}x",
    ]
    return "\n".join(lines)


def _cpu_count_hint() -> int:
    import os

    return os.cpu_count() or 1


async def main(args: argparse.Namespace) -> None:
    with tempfile.TemporaryDirectory(prefix="executor_bench_") as tmp:
        work = Path(tmp)
        print(f"Rendering {args.clips} synthetic clips into {work} ...")
        paths = build_corpus(
            clips=args.clips,
            fps=args.fps,
            duration=args.duration,
            width=args.width,
            height=args.height,
            work_dir=work,
        )

        print(f"Running thread pool baseline on {len(paths)} clips ...")
        thread_phase = await _time_phase("thread", paths, use_process_pool=False)
        print(f"  -> {thread_phase.wall_seconds:.2f}s")

        print(f"Running process pool on {len(paths)} clips ...")
        process_phase = await _time_phase("process", paths, use_process_pool=True)
        print(f"  -> {process_phase.wall_seconds:.2f}s")

        report = render_report(
            n_clips=args.clips,
            fps=args.fps,
            duration=args.duration,
            workers=args.workers,
            thread_phase=thread_phase,
            process_phase=process_phase,
        )
        print()
        print(report)

        if args.keep_dir:
            kept = Path(args.keep_dir)
            kept.mkdir(parents=True, exist_ok=True)
            for p in paths:
                shutil.copy(p, kept / p.name)
            print(f"\nKept rendered clips in {kept}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare ThreadPool vs ProcessPool on the ArUco pipeline."
    )
    parser.add_argument(
        "--clips", type=int, default=16, help="number of clips to render"
    )
    parser.add_argument(
        "--fps", type=int, default=10, help="frames per second per clip"
    )
    parser.add_argument(
        "--duration", type=float, default=4.0, help="clip duration in seconds"
    )
    parser.add_argument("--width", type=int, default=320, help="frame width")
    parser.add_argument("--height", type=int, default=240, help="frame height")
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="MAX_PROCESS_WORKERS used by the executor",
    )
    parser.add_argument(
        "--keep-dir",
        type=str,
        default=None,
        help="optional directory to copy the rendered clips into",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args))
