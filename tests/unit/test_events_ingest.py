"""Unit tests for the ``POST /v1/events/ingest`` endpoint.

The endpoint wires together three collaborators:
:func:`StationRepository.get_by_code`,
:func:`VideoService.get_or_create_edge_sentinel` and
:func:`EventService.create_many`. The tests below patch those
collaborators with ``AsyncMock`` so the route can be exercised
without a DB.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.router.v1.events import ingest_edge_batch
from src.schemas.edge_ingest import EdgeBatchIn, EdgeBatchOut, EdgeEventIn


def _payload(*, n_events: int = 1) -> EdgeBatchIn:
    started = datetime(2026, 7, 7, 10, 0, 0)
    ended = datetime(2026, 7, 7, 10, 0, 30)
    events = [
        EdgeEventIn(
            aruco_id=i,
            confidence=0.99,
            inside_roi=True,
            frame_number=i,
            timestamp_in_video=i * 0.1,
        )
        for i in range(n_events)
    ]
    return EdgeBatchIn(
        station_code="STATION_01",
        started_at=started,
        ended_at=ended,
        events=events,
    )


@pytest.fixture
def session() -> MagicMock:
    return MagicMock()


# ───────────────────────────────────────────────────────────────────────
# Happy path
# ───────────────────────────────────────────────────────────────────────


async def test_ingest_writes_events_for_known_station(session):
    station = SimpleNamespace(id=1, code="STATION_01")
    sentinel = SimpleNamespace(id=42)

    with (
        patch("src.router.v1.events.StationRepository") as station_repo_cls,
        patch("src.router.v1.events.VideoService") as video_service_cls,
        patch("src.router.v1.events.EventService") as event_service_cls,
    ):
        station_repo = MagicMock()
        station_repo.get_by_code = AsyncMock(return_value=station)
        station_repo_cls.return_value = station_repo

        video_service = MagicMock()
        video_service.get_or_create_edge_sentinel = AsyncMock(
            return_value=(sentinel, True)
        )
        video_service_cls.return_value = video_service

        event_service = MagicMock()
        event_service.create_many = AsyncMock(
            return_value=[SimpleNamespace(id=i) for i in range(3)]
        )
        event_service_cls.return_value = event_service

        result = await ingest_edge_batch(_payload(n_events=3), session)

    assert isinstance(result, EdgeBatchOut)
    assert result.video_file_id == 42
    assert result.events_created == 3
    assert result.sentinel_created is True

    video_service.get_or_create_edge_sentinel.assert_awaited_once()
    event_service.create_many.assert_awaited_once()
    payloads = event_service.create_many.await_args.args[0]
    assert len(payloads) == 3
    assert all(p.video_file_id == 42 for p in payloads)


async def test_ingest_with_no_events_still_returns_sentinel(session):
    station = SimpleNamespace(id=1, code="STATION_01")
    sentinel = SimpleNamespace(id=99)

    with (
        patch("src.router.v1.events.StationRepository") as station_repo_cls,
        patch("src.router.v1.events.VideoService") as video_service_cls,
        patch("src.router.v1.events.EventService") as event_service_cls,
    ):
        station_repo = MagicMock()
        station_repo.get_by_code = AsyncMock(return_value=station)
        station_repo_cls.return_value = station_repo

        video_service = MagicMock()
        video_service.get_or_create_edge_sentinel = AsyncMock(
            return_value=(sentinel, False)
        )
        video_service_cls.return_value = video_service

        event_service = MagicMock()
        event_service.create_many = AsyncMock(return_value=[])
        event_service_cls.return_value = event_service

        result = await ingest_edge_batch(_payload(n_events=0), session)

    assert result.events_created == 0
    assert result.sentinel_created is False
    event_service.create_many.assert_not_awaited()


async def test_ingest_reuse_path_reports_sentinel_created_false(session):
    station = SimpleNamespace(id=1, code="STATION_01")
    sentinel = SimpleNamespace(id=99)

    with (
        patch("src.router.v1.events.StationRepository") as station_repo_cls,
        patch("src.router.v1.events.VideoService") as video_service_cls,
        patch("src.router.v1.events.EventService") as event_service_cls,
    ):
        station_repo = MagicMock()
        station_repo.get_by_code = AsyncMock(return_value=station)
        station_repo_cls.return_value = station_repo

        video_service = MagicMock()
        video_service.get_or_create_edge_sentinel = AsyncMock(
            return_value=(sentinel, False)
        )
        video_service_cls.return_value = video_service

        event_service = MagicMock()
        event_service.create_many = AsyncMock(return_value=[SimpleNamespace(id=1)])
        event_service_cls.return_value = event_service

        result = await ingest_edge_batch(_payload(n_events=1), session)

    assert result.video_file_id == 99
    assert result.sentinel_created is False


# ───────────────────────────────────────────────────────────────────────
# Error paths
# ───────────────────────────────────────────────────────────────────────


async def test_ingest_404_when_station_unknown(session):
    with patch("src.router.v1.events.StationRepository") as station_repo_cls:
        station_repo = MagicMock()
        station_repo.get_by_code = AsyncMock(return_value=None)
        station_repo_cls.return_value = station_repo

        with pytest.raises(HTTPException) as exc:
            await ingest_edge_batch(_payload(n_events=1), session)

    assert exc.value.status_code == 404
    assert "STATION_01" in str(exc.value.detail)


# ───────────────────────────────────────────────────────────────────────
# Field plumbing
# ───────────────────────────────────────────────────────────────────────


async def test_ingest_event_payload_preserves_aruco_and_bbox(session):
    station = SimpleNamespace(id=1, code="STATION_01")
    sentinel = SimpleNamespace(id=42)

    payload = EdgeBatchIn(
        station_code="STATION_01",
        started_at=datetime(2026, 7, 7, 10, 0, 0),
        ended_at=datetime(2026, 7, 7, 10, 0, 30),
        events=[
            EdgeEventIn(
                aruco_id=7,
                confidence=0.5,
                bbox={"x": 1, "y": 2, "w": 3, "h": 4},
                inside_roi=False,
                frame_number=12,
                timestamp_in_video=0.4,
                detector_metadata={"triggered_by_mog2": True},
            )
        ],
    )

    with (
        patch("src.router.v1.events.StationRepository") as station_repo_cls,
        patch("src.router.v1.events.VideoService") as video_service_cls,
        patch("src.router.v1.events.EventService") as event_service_cls,
    ):
        station_repo = MagicMock()
        station_repo.get_by_code = AsyncMock(return_value=station)
        station_repo_cls.return_value = station_repo

        video_service = MagicMock()
        video_service.get_or_create_edge_sentinel = AsyncMock(
            return_value=(sentinel, True)
        )
        video_service_cls.return_value = video_service

        event_service = MagicMock()
        event_service.create_many = AsyncMock(return_value=[SimpleNamespace(id=1)])
        event_service_cls.return_value = event_service

        await ingest_edge_batch(payload, session)

    forwarded = event_service.create_many.await_args.args[0]
    assert len(forwarded) == 1
    evt = forwarded[0]
    assert evt.video_file_id == 42
    assert evt.aruco_id == 7
    assert evt.confidence == 0.5
    assert evt.bbox == {"x": 1, "y": 2, "w": 3, "h": 4}
    assert evt.inside_roi is False
    assert evt.frame_number == 12
    assert evt.timestamp_in_video == 0.4
    assert evt.detector_metadata == {"triggered_by_mog2": True}
    # wall_clock_at must come from the batch's ended_at, not from now().
    assert evt.wall_clock_at == datetime(2026, 7, 7, 10, 0, 30)


def test_payload_rejects_empty_station_code():
    with pytest.raises(ValueError):
        EdgeBatchIn(
            station_code="",
            started_at=datetime.now(UTC),
            ended_at=datetime.now(UTC),
            events=[],
        )


def test_payload_rejects_negative_frame_number():
    with pytest.raises(ValueError):
        EdgeEventIn(
            frame_number=-1,
            timestamp_in_video=0.0,
        )
