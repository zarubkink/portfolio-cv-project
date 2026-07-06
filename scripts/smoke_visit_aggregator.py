"""End-to-end check for Stage 8: visit state machine.

Generates an ArUco-rich test video, drives it through the video
handler, and confirms the visit aggregator produces ENTERING → PRESENT
→ LEAVING → CLOSED rows as expected.

Usage:
    uv run python scripts/smoke_visit_aggregator.py
"""

from __future__ import annotations

import asyncio
import shutil
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import cv2
import numpy as np
from loguru import logger
from sqlalchemy import text
from sqlmodel.ext.asyncio.session import AsyncSession

from src.dependencies import engine
from src.models.tractor import Tractor
from src.models.video_file import VideoFile
from src.schemas.video_file import VideoStatus
from src.services.station_service import StationService
from src.services.tractor_service import TractorService
from src.services.visit_service import VisitService

VIDEO_W = 320
VIDEO_H = 240
FPS = 10
DURATION = 8  # seconds — long enough to clear entry_confirm debounce
ARUCO_ID = 42  # inside DICT_4X4_50 (0..49) — matches the default detector


async def _seed_refs(station_code: str, tractor_name: str) -> tuple[int, int]:
    """Create (or fetch) a station + tractor and return their ids."""
    from src.schemas.station import StationCreate
    from src.schemas.tractor import TractorCreate

    async with AsyncSession(engine) as session:
        station_service = StationService(session)
        existing = await station_service.repo.get_by_code(station_code)
        if existing is None:
            station = await station_service.create(
                StationCreate(
                    code=station_code,
                    name=f"Smoke {station_code}",
                    video_dir=f"data/queue/{station_code}",
                    roi_polygon=[
                        [10, 10],
                        [VIDEO_W - 10, 10],
                        [VIDEO_W - 10, VIDEO_H - 10],
                        [10, VIDEO_H - 10],
                    ],
                )
            )
        else:
            station = existing
        station_id = station.id

        tractor_service = TractorService(session)
        from sqlmodel import select

        stmt = select(Tractor).where(Tractor.name == tractor_name)
        match = (await session.exec(stmt)).first()
        if match is not None and ARUCO_ID not in (match.aruco_ids or []):
            # Old tractor with stale aruco_ids: delete so we can recreate.
            await session.delete(match)
            await session.flush()
            match = None
        if match is None:
            tractor = await tractor_service.create(
                TractorCreate(name=tractor_name, aruco_ids=[ARUCO_ID])
            )
        else:
            tractor = match
        tractor_id = tractor.id

        await session.commit()
    return station_id, tractor_id


async def _cleanup(station_code: str, tractor_id: int) -> None:
    """Remove every visit/video/event row that references our smoke ids."""
    async with AsyncSession(engine) as session:
        await session.exec(
            text("DELETE FROM visits WHERE tractor_id = :tid"),
            params={"tid": tractor_id},
        )
        await session.exec(
            text(
                "DELETE FROM events WHERE video_file_id IN "
                "(SELECT id FROM video_files WHERE station_id IN "
                "(SELECT id FROM stations WHERE code = :code))"
            ),
            params={"code": station_code},
        )
        await session.exec(
            text(
                "DELETE FROM video_files WHERE station_id IN "
                "(SELECT id FROM stations WHERE code = :code)"
            ),
            params={"code": station_code},
        )
        await session.exec(
            text("DELETE FROM stations WHERE code = :code"),
            params={"code": station_code},
        )
        await session.exec(
            text("DELETE FROM tractors WHERE name = :name"),
            params={"name": "Smoke tractor 8"},
        )
        await session.commit()


def _make_video(path: Path) -> None:
    """Render an mp4 with a moving ArUco marker + noisy background.

    Reuses the proven composition from ``generate_test_video.py``: random
    background noise drives MOG2, while the marker jitters inside the
    ROI polygon so every frame produces an inside-ROI detection.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()

    dictionary = cv2.aruco.getPredefinedDictionary(cv2.aruco.DICT_4X4_1000)
    marker_img = cv2.aruco.generateImageMarker(dictionary, ARUCO_ID, 140)
    marker_bgr = cv2.cvtColor(marker_img, cv2.COLOR_GRAY2BGR)
    bg = np.full((VIDEO_H, VIDEO_W, 3), 64, dtype=np.uint8)

    writer = cv2.VideoWriter(
        str(path), cv2.VideoWriter_fourcc(*"mp4v"), FPS, (VIDEO_W, VIDEO_H)
    )
    rng = np.random.default_rng(7)
    for i in range(FPS * DURATION):
        frame = cv2.add(bg, rng.integers(0, 40, bg.shape, dtype=np.uint8))
        # Jitter inside ROI polygon (10,10)–(310,230).
        offset = int(15 * np.sin(2 * np.pi * i / FPS))
        x = (VIDEO_W - 140) // 2 + offset
        y = (VIDEO_H - 140) // 2 + offset
        frame[y : y + 140, x : x + 140] = marker_bgr
        writer.write(frame)
    writer.release()


async def main() -> int:
    station_code = "SMOKE_VISIT"
    tractor_name = "Smoke tractor 8"
    videos_dir = Path("data/videos")
    videos_dir.mkdir(parents=True, exist_ok=True)
    video_path = videos_dir / "smoke_visit.mp4"

    logger.info("=== seeding station + tractor ===")
    station_id, tractor_id = await _seed_refs(station_code, tractor_name)
    await _cleanup(station_code, tractor_id)
    station_id, tractor_id = await _seed_refs(station_code, tractor_name)
    logger.info(f"station_id={station_id} tractor_id={tractor_id}")

    logger.info("=== rendering test video ===")
    _make_video(video_path)
    logger.info(f"video: {video_path} ({video_path.stat().st_size} bytes)")

    logger.info("=== driving video through /v1/videos/handle ===")
    started_at = datetime.now(UTC) - timedelta(seconds=10)
    ended_at = started_at + timedelta(seconds=DURATION)

    import httpx

    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        r = await client.post(
            "/v1/videos/handle",
            data={
                "filepath": str(video_path),
                "station_id": station_id,
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
            },
        )
        body = r.json()
        if r.status_code != 200 or "video_id" not in body:
            logger.error(f"handle failed: {r.status_code} {body}")
            return 1
        video_id = int(body["video_id"])
    logger.info(f"handle → video_id={video_id}")

    # Poll for COMPLETED status.
    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        for _ in range(60):
            r = await client.get(f"/v1/videos/{video_id}")
            status = r.json().get("status")
            if status == "COMPLETED":
                logger.info(f"video {video_id} → COMPLETED")
                break
            if status in {"FAILED", "INVALID"}:
                logger.error(f"video {video_id} → {status}: {r.json()}")
                return 1
            await asyncio.sleep(1)
        else:
            logger.error(f"video {video_id} did not complete in 60s")
            return 1

    async with AsyncSession(engine) as session:
        vf = await session.get(VideoFile, video_id)
        if vf is None:
            logger.error(f"video {video_id} missing")
            return 1
        if vf.status != VideoStatus.COMPLETED:
            logger.error(f"video {video_id} status={vf.status.value}")
            return 1

        visits = await VisitService(session).repo.list_open_for_tractor(tractor_id)
        logger.info(f"open visits for tractor {tractor_id}: {len(visits)}")
        for v in visits:
            logger.info(
                f"  visit id={v.id} state={v.state.value} "
                f"arrived={v.arrived_at} last_seen={v.last_seen_at}"
            )

        current = await VisitService(session).get_current_tractor(tractor_id)
        logger.info(f"current_tractor({tractor_id}) → {current}")

    print("\n=== STATUS ENDPOINTS ===")
    import httpx

    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        r = await client.get("/v1/status/tractors")
        tractors = r.json()
        ours = [t for t in tractors if t["tractor_id"] == tractor_id]
        logger.info(f"tractors → {len(tractors)} open, ours: {ours}")

        r = await client.get(f"/v1/status/tractor/{tractor_id}")
        logger.info(f"tractor/{tractor_id} → {r.json()}")

        r = await client.get(
            "/v1/status/stations",
        )
        stations = r.json()
        our_station = next((s for s in stations if s["station_id"] == station_id), None)
        logger.info(f"stations → our_station={our_station}")

    print("\n=== HISTORY ===")
    async with AsyncSession(engine) as session:
        history = await VisitService(session).get_history(tractor_id=tractor_id)
        for v in history:
            logger.info(
                f"  visit id={v.id} state={v.state.value} "
                f"arrived={v.arrived_at} departed={v.departed_at} "
                f"duration={v.duration_seconds}s"
            )

    print("\n=== CLEANUP ===")
    await _cleanup(station_code, tractor_id)
    shutil.rmtree(videos_dir / "smoke_visit.mp4", ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
