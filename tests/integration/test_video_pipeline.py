"""End-to-end pipeline test.

Runs against the live compose stack:

1. Render a synthetic ArUco-rich MP4.
2. POST it to /v1/videos/handle.
3. Poll the API until the video is COMPLETED.
4. Confirm a visit row was created and is in ENTERING.
5. Subscribe to /v1/status/stream and assert the SSE channel emitted
   at least one ``visit_state_change`` message during the run.
6. Confirm the status endpoints reflect the same visit.

The test cleans up its own station, tractor, events, video and visit
rows so re-runs don't accumulate garbage.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.dependencies import engine
from src.models.tractor import Tractor
from src.schemas.station import StationCreate
from src.schemas.tractor import TractorCreate
from src.services.station_service import StationService
from src.services.tractor_service import TractorService

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _seed(
    session: AsyncSession,
    *,
    station_code: str,
    tractor_name: str,
    aruco_id: int,
):
    """Create or refresh a station and a tractor with our test ids.

    Captures ``station.id`` and ``tractor.id`` *before* commit because
    SQLAlchemy expires all attributes on commit, which makes a
    subsequent attribute access try to lazy-load off a closed
    transaction and raise ``MissingGreenlet``.

    Also removes any prior tractor whose ``primary_aruco_id`` matches
    ``aruco_id`` — a previous run may have crashed before its cleanup
    ran, leaving the unique constraint blocking re-use.
    """
    station_svc = StationService(session)
    existing = await station_svc.repo.get_by_code(station_code)
    if existing is not None:
        await session.delete(existing)
        await session.flush()

    station = await station_svc.create(
        StationCreate(
            code=station_code,
            name=f"Integration {station_code}",
            video_dir=f"data/queue/{station_code}",
            roi_polygon=[
                [10, 10],
                [310, 10],
                [310, 230],
                [10, 230],
            ],
        )
    )
    station_id = station.id

    prior_tractor = (
        await session.exec(select(Tractor).where(Tractor.primary_aruco_id == aruco_id))
    ).first()
    if prior_tractor is not None:
        await session.delete(prior_tractor)
        await session.flush()

    stmt = select(Tractor).where(Tractor.name == tractor_name)
    match = (await session.exec(stmt)).first()
    if match is not None:
        await session.delete(match)
        await session.flush()

    tractor_svc = TractorService(session)
    tractor = await tractor_svc.create(
        TractorCreate(name=tractor_name, aruco_ids=[aruco_id])
    )
    tractor_id = tractor.id

    await session.commit()
    return station_id, tractor_id


async def _purge(*, station_code: str, tractor_id: int):
    """Tear down everything the test created.

    Uses a single ``TRUNCATE ... CASCADE`` per child table inside an
    explicit ``engine.begin()`` so the deletes commit independently
    of the ORM session state. We retry the dependent deletes with
    back-off because the API server may briefly hold open rows while
    a background worker finishes committing its transaction.
    """
    import asyncio

    from src.dependencies import engine

    for _ in range(8):
        async with engine.begin() as conn:  # type: AsyncConnection
            await conn.execute(
                text("DELETE FROM visits WHERE tractor_id = :tid"),
                {"tid": tractor_id},
            )
            await conn.execute(
                text(
                    "DELETE FROM events WHERE video_file_id IN "
                    "(SELECT id FROM video_files WHERE station_id IN "
                    "  (SELECT id FROM stations WHERE code = :code))"
                ),
                {"code": station_code},
            )
            await conn.execute(
                text(
                    "DELETE FROM video_files WHERE station_id IN "
                    "(SELECT id FROM stations WHERE code = :code)"
                ),
                {"code": station_code},
            )
            await conn.execute(
                text("DELETE FROM stations WHERE code = :code"),
                {"code": station_code},
            )
            await conn.execute(
                text("DELETE FROM tractors WHERE id = :tid"),
                {"tid": tractor_id},
            )

        # Did we actually clear the dependents?
        async with engine.connect() as conn:
            leftover = await conn.execute(
                text(
                    "SELECT count(*) FROM video_files WHERE station_id IN "
                    "(SELECT id FROM stations WHERE code = :code)"
                ),
                {"code": station_code},
            )
            if leftover.scalar() == 0:
                return
        await asyncio.sleep(0.5)

    # One last attempt without swallowing — surface the FK error
    # so the test fails loudly if cleanup genuinely can't run.
    async with engine.begin() as conn:
        await conn.execute(
            text("DELETE FROM stations WHERE code = :code"),
            {"code": station_code},
        )


async def _drain_sse_into_queue(
    client: httpx.AsyncClient,
    *,
    path: str,
    queue: asyncio.Queue,
) -> None:
    """Background task: push every SSE event into ``queue``.

    The task exits when the connection closes (client disconnects).
    Reads raw bytes instead of ``aiter_lines`` because httpx buffers
    line-by-line delivery across SSE frames in some versions, which
    can hide the initial ``ready`` handshake until the next event
    arrives.
    """
    buffer = b""
    async with client.stream("GET", path, timeout=None) as response:
        async for chunk in response.aiter_bytes():
            buffer += chunk
            # sse_starlette emits CRLF-delimited frames; normalise so we
            # can split on a single separator regardless of the
            # upstream convention.
            buffer = buffer.replace(b"\r\n", b"\n")
            while b"\n\n" in buffer:
                frame, buffer = buffer.split(b"\n\n", 1)
                for line in frame.splitlines():
                    if line.startswith(b"data:"):
                        try:
                            queue.put_nowait(
                                json.loads(line[len(b"data:") :].decode().strip())
                            )
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            pass


async def _wait_for_sse_events(
    queue: asyncio.Queue,
    *,
    timeout: float,
    min_count: int,
) -> list[dict]:
    """Block up to ``timeout`` seconds waiting for ``min_count`` events."""
    events: list[dict] = []
    deadline = asyncio.get_event_loop().time() + timeout
    while len(events) < min_count:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            break
        try:
            evt = await asyncio.wait_for(queue.get(), timeout=remaining)
            events.append(evt)
        except TimeoutError:
            break
    return events


async def test_video_pipeline_creates_visit_and_emits_sse(
    api_reachable,
    db_engine,
    api_client: httpx.AsyncClient,
    tmp_path: Path,
    unique_code: str,
):
    import cv2
    import numpy as np

    station_code = unique_code
    tractor_name = f"{station_code} tractor"
    # Pick an ArUco id from {7..10, 17..20, 27..41, 43..49}: the
    # union of IDs the seed_reference script and the smoke script
    # do NOT use. DICT_4X4_50 supports 0..49, so the marker renders
    # and the api's detector recognises it.
    _safe_ids = [
        7,
        8,
        9,
        10,
        17,
        18,
        19,
        20,
        27,
        28,
        29,
        30,
        31,
        32,
        33,
        34,
        35,
        36,
        37,
        38,
        39,
        40,
        41,
        43,
        44,
        45,
        46,
        47,
        48,
        49,
    ]
    seed_int = int(unique_code.split("_")[1][:4], 16)
    aruco_id = _safe_ids[seed_int % len(_safe_ids)]
    station_id = tractor_id = None

    video_path = tmp_path / f"integration_aruco_{aruco_id}.mp4"
    dictionary = cv2.aruco.getPredefinedDictionary(cv2.aruco.DICT_4X4_50)
    marker = cv2.cvtColor(
        cv2.aruco.generateImageMarker(dictionary, aruco_id, 140),
        cv2.COLOR_GRAY2BGR,
    )
    bg = np.full((240, 320, 3), 64, dtype=np.uint8)
    # Use unique_code as the RNG seed so the SHA-256 hash of the
    # rendered mp4 differs between test runs and the server's dedup
    # check does not reject a re-run with 406.
    seed = int.from_bytes(unique_code.encode(), "big") % (2**32)
    rng = np.random.default_rng(seed)
    writer = cv2.VideoWriter(
        str(video_path), cv2.VideoWriter_fourcc(*"mp4v"), 10.0, (320, 240)
    )
    for i in range(60):
        frame = cv2.add(bg, rng.integers(0, 40, bg.shape, dtype=np.uint8))
        offset = int(15 * np.sin(2 * np.pi * i / 10))
        x = (320 - 140) // 2 + offset
        y = (240 - 140) // 2 + offset
        frame[y : y + 140, x : x + 140] = marker
        writer.write(frame)
    writer.release()

    sse_queue: asyncio.Queue = asyncio.Queue()
    sse_task: asyncio.Task | None = None
    collected: list[dict] = []
    try:
        async with AsyncSession(engine) as session:
            station_id, tractor_id = await _seed(
                session,
                station_code=station_code,
                tractor_name=tractor_name,
                aruco_id=aruco_id,
            )

        # Subscribe BEFORE submitting so we capture the ENTERING event.
        sse_task = asyncio.create_task(
            _drain_sse_into_queue(api_client, path="/v1/status/stream", queue=sse_queue)
        )
        # Give the SSE handshake time to land in the queue. The first
        # message is ``event: ready`` and arrives as soon as
        # sse_starlette starts iterating the upstream generator, which
        # can take a beat after the HTTP response headers come back.
        collected.extend(
            await _wait_for_sse_events(sse_queue, timeout=10.0, min_count=1)
        )

        started_at = datetime.now(UTC) - timedelta(seconds=10)
        ended_at = started_at + timedelta(seconds=6)

        with video_path.open("rb") as video_file:
            r = await api_client.post(
                "/v1/videos/upload",
                data={
                    "station_id": station_id,
                    "started_at": started_at.isoformat(),
                    "ended_at": ended_at.isoformat(),
                },
                files={
                    "file": (
                        video_path.name,
                        video_file,
                        "video/mp4",
                    ),
                },
            )
        assert r.status_code == 201, r.text
        body = r.json()
        video_id = int(body["id"])

        # Poll for COMPLETED.
        for _ in range(60):
            r = await api_client.get(f"/v1/videos/{video_id}")
            assert r.status_code == 200, r.text
            status = r.json().get("status")
            if status == "COMPLETED":
                break
            if status in {"FAILED", "INVALID"}:
                pytest.fail(f"video {video_id} → {status}: {r.text}")
            await asyncio.sleep(1)
        else:
            pytest.fail(f"video {video_id} did not complete in 60s")

        # Give the API server a moment to fully release the visit
        # transaction before cleanup. The visit was committed as
        # part of setting COMPLETED, but there can be a brief window
        # where a concurrent stale-check or scheduler tick is still
        # touching the same row.
        await asyncio.sleep(1.0)

        r = await api_client.get("/v1/status/tractors")
        assert r.status_code == 200
        tractors = r.json()
        ours = [t for t in tractors if t["tractor_id"] == tractor_id]
        assert ours, f"no open visit for tractor {tractor_id}: {tractors}"
        assert ours[0]["state"] in {"ENTERING", "PRESENT"}

        # Drain whatever visit_state_change events arrived. We need at
        # least one for this tractor; the channel may also carry events
        # from unrelated visits left over from previous runs.
        collected.extend(
            await _wait_for_sse_events(sse_queue, timeout=15.0, min_count=1)
        )
        ready = [e for e in collected if e.get("status") == "listening"]
        state_changes = [
            e
            for e in collected
            if e.get("type") == "visit_state_change"
            and e.get("tractor_id") == tractor_id
        ]
        assert ready, f"missing 'ready' handshake in {collected}"
        assert state_changes, (
            f"no visit_state_change for tractor {tractor_id} in {collected}"
        )
        visit_ids = {e["visit_id"] for e in state_changes}
        assert len(visit_ids) == 1, f"split visit ids: {visit_ids}"
    finally:
        if sse_task is not None:
            sse_task.cancel()
            try:
                await sse_task
            except (asyncio.CancelledError, Exception):
                pass
        if station_id is not None and tractor_id is not None:
            await _purge(
                station_code=station_code,
                tractor_id=tractor_id,
            )
