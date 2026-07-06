"""Smoke test for the retry scheduler.

Inserts a FAILED video and a stale PROCESSING video, then drives the
scheduler tick endpoint until the failed video either recovers or
exceeds the retry cap and ends up INVALID.
"""

from __future__ import annotations

import asyncio
import sys
import urllib.request
from pathlib import Path

from sqlalchemy import text

from src.dependencies import engine

VIDEO_STORAGE = Path("./data/videos")
MAX_TICKS = 5
TICK_INTERVAL_SECONDS = 0.5


async def _ensure_storage_files() -> None:
    """Drop a tiny but valid mp4 in each path so OpenCV can open it.

    The actual processing will still fail (no tractor + no roi) but at
    least the cv2.VideoCapture step succeeds — that gives us the
    ``mark_permanently_failed`` path that depends on a real retry cycle.
    """
    import cv2
    import numpy as np

    VIDEO_STORAGE.mkdir(parents=True, exist_ok=True)
    for name in ("scheduler_test.mp4", "stuck.mp4"):
        path = VIDEO_STORAGE / name
        if path.exists() and path.stat().st_size > 0:
            continue
        fourcc = cv2.VideoWriter.fourcc(*"mp4v")
        writer = cv2.VideoWriter(str(path), fourcc, 10.0, (320, 240))
        for _ in range(5):
            frame = np.zeros((240, 320, 3), dtype=np.uint8)
            writer.write(frame)
        writer.release()


async def _read_video_ids() -> list[int]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text("SELECT id FROM video_files ORDER BY id DESC LIMIT 3")
            )
        ).all()
    return [r[0] for r in rows]


async def _reset_failed_video(video_id: int, retry_count: int) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                UPDATE video_files
                SET status = 'FAILED',
                    retry_count = :retry_count,
                    error_message = 'forced failure for smoke test',
                    storage_uri = :storage_uri,
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "id": video_id,
                "retry_count": retry_count,
                "storage_uri": "data/videos/scheduler_test.mp4",
            },
        )


async def _reset_stale_processing(video_id: int) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                UPDATE video_files
                SET status = 'PROCESSING',
                    updated_at = NOW() - INTERVAL '60 minutes',
                    error_message = 'stuck',
                    storage_uri = :storage_uri
                WHERE id = :id
                """
            ),
            {
                "id": video_id,
                "storage_uri": "data/videos/stuck.mp4",
            },
        )


async def _read_row(video_id: int) -> dict | None:
    async with engine.connect() as conn:
        res = await conn.execute(
            text(
                "SELECT id, status, retry_count, error_message, storage_uri "
                "FROM video_files WHERE id = :id"
            ),
            {"id": video_id},
        )
        row = res.first()
        return dict(row._mapping) if row else None


async def _trigger_tick() -> dict:
    """Hit POST /v1/admin/scheduler/tick synchronously and return the JSON body."""
    req = urllib.request.Request(
        "http://localhost:8000/v1/admin/scheduler/tick", method="POST"
    )
    with urllib.request.urlopen(req) as resp:
        import json

        return json.loads(resp.read().decode("utf-8"))


async def main() -> int:
    await _ensure_storage_files()
    ids = await _read_video_ids()
    if len(ids) < 2:
        print("Not enough video_files rows to test — run the pipeline first.")
        return 1
    failed_id = ids[0]
    stuck_id = ids[1]
    print(f"using failed_id={failed_id}, stuck_id={stuck_id}")

    await _reset_failed_video(failed_id, retry_count=0)
    await _reset_stale_processing(stuck_id)

    print("Before tick:")
    print(f"  failed_id={failed_id}: {await _read_row(failed_id)}")
    print(f"  stuck_id={stuck_id}: {await _read_row(stuck_id)}")

    all_passed = True
    for i in range(MAX_TICKS):
        result = await _trigger_tick()
        await asyncio.sleep(TICK_INTERVAL_SECONDS)
        failed_after = await _read_row(failed_id)
        stuck_after = await _read_row(stuck_id)
        print(
            f"tick {i + 1}: response={result}  failed={failed_after['status']}"
            f"/retry={failed_after['retry_count']}  stuck={stuck_after['status']}"
            f"/retry={stuck_after['retry_count']}"
        )
        if stuck_after["status"] == "PROCESSING" and i == 0:
            print("FAIL: stuck video not flipped to FAILED on first tick")
            all_passed = False
        if failed_after["status"] == "INVALID":
            print(f"PASS: failed video reached INVALID after {i + 1} ticks")
            break
        if failed_after["status"] == "COMPLETED":
            print("PASS: failed video recovered to COMPLETED on retry")
            break
    else:
        print(
            f"WARN: failed video still {failed_after['status']} after {MAX_TICKS} ticks"
        )
        all_passed = False

    final_failed = await _read_row(failed_id)
    final_stuck = await _read_row(stuck_id)
    print(f"FINAL failed: {final_failed}")
    print(f"FINAL stuck: {final_stuck}")

    if final_stuck["status"] == "INVALID":
        print("PASS: stuck video eventually moved to INVALID after exceeding retries")
    elif final_stuck["status"] == "COMPLETED":
        print("PASS: stuck video recovered to COMPLETED")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
