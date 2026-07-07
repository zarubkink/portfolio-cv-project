"""Unit tests for :func:`VideoService.get_or_create_edge_sentinel`.

The factory runs two paths:

* **Create** — no row with the station's sentinel hash exists; a new
  ``VideoFile`` is created with ``status=PROCESSING``,
  ``storage_uri="edge://<code>"`` and a deterministic
  ``content_hash``.
* **Reuse** — a row already exists; ``started_at`` may move earlier,
  ``ended_at`` may move later, but neither column ever moves
  backwards.

Both paths are exercised here with a mocked ``VideoFileRepository``;
the ``AsyncSession`` is also a ``MagicMock`` so the service code
can run without a real DB.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from src.services.video_service import (
    VideoService,
    _edge_sentinel_hash,
    _edge_sentinel_uri,
)


def _existing_sentinel(*, started_at: datetime, ended_at: datetime) -> SimpleNamespace:
    return SimpleNamespace(
        id=99,
        storage_uri=_edge_sentinel_uri("STATION_01"),
        started_at=started_at,
        ended_at=ended_at,
        station_id=1,
    )


@pytest.fixture
def service() -> VideoService:
    session = MagicMock()
    repo = MagicMock()
    repo.get_by_hash = AsyncMock(return_value=None)
    repo.create = AsyncMock(side_effect=lambda data: SimpleNamespace(id=42, **data))
    repo.update = AsyncMock(
        side_effect=lambda vf, data: SimpleNamespace(
            id=vf.id,
            storage_uri=vf.storage_uri,
            **{
                **{
                    k: getattr(vf, k, None)
                    for k in ("started_at", "ended_at", "station_id")
                },
                **data,
            },
        )
    )
    svc = VideoService(session)
    svc.repo = repo
    return svc


# ───────────────────────────────────────────────────────────────────────
# Hash/URI helpers
# ───────────────────────────────────────────────────────────────────────


def test_edge_sentinel_uri_is_stable():
    assert _edge_sentinel_uri("STATION_01") == "edge://STATION_01"


def test_edge_sentinel_hash_is_sha256_of_uri():
    import hashlib

    expected = hashlib.sha256(b"edge://STATION_01").digest()
    assert _edge_sentinel_hash("STATION_01") == expected


def test_edge_sentinel_hash_differs_per_station():
    assert _edge_sentinel_hash("A") != _edge_sentinel_hash("B")


# ───────────────────────────────────────────────────────────────────────
# Create path
# ───────────────────────────────────────────────────────────────────────


async def test_create_path_writes_sentinel_row(service):
    """First call must insert a sentinel with the sentinel URI."""
    started = datetime(2026, 7, 7, 10, 0, 0, tzinfo=UTC)
    ended = started + timedelta(seconds=30)

    sentinel, created = await service.get_or_create_edge_sentinel(
        station_id=1,
        station_code="STATION_01",
        started_at=started,
        ended_at=ended,
    )

    assert created is True
    assert sentinel.id == 42
    assert sentinel.storage_uri == "edge://STATION_01"
    assert sentinel.content_hash == _edge_sentinel_hash("STATION_01")
    assert sentinel.status.value == "PROCESSING"
    # naive UTC stored in TIMESTAMP WITHOUT TIME ZONE
    assert sentinel.started_at == started.replace(tzinfo=None)
    assert sentinel.ended_at == ended.replace(tzinfo=None)
    service.repo.create.assert_awaited_once()
    service.repo.update.assert_not_awaited()


async def test_create_path_rejects_inverted_window(service):
    started = datetime(2026, 7, 7, 10, 0, 30, tzinfo=UTC)
    ended = datetime(2026, 7, 7, 10, 0, 0, tzinfo=UTC)
    with pytest.raises(HTTPException) as exc:
        await service.get_or_create_edge_sentinel(
            station_id=1,
            station_code="STATION_01",
            started_at=started,
            ended_at=ended,
        )
    assert exc.value.status_code == 422


# ───────────────────────────────────────────────────────────────────────
# Reuse path
# ───────────────────────────────────────────────────────────────────────


async def test_reuse_path_extends_ended_at_forward():
    existing = _existing_sentinel(
        started_at=datetime(2026, 7, 7, 10, 0, 0),
        ended_at=datetime(2026, 7, 7, 10, 0, 30),
    )
    session = MagicMock()
    repo = MagicMock()
    repo.get_by_hash = AsyncMock(return_value=existing)
    repo.update = AsyncMock(return_value=existing)
    service = VideoService(session)
    service.repo = repo

    later_ended = datetime(2026, 7, 7, 10, 0, 45)
    sentinel, created = await service.get_or_create_edge_sentinel(
        station_id=1,
        station_code="STATION_01",
        started_at=existing.started_at.replace(tzinfo=UTC),
        ended_at=later_ended.replace(tzinfo=UTC),
    )

    assert created is False
    assert sentinel is existing
    update_args = repo.update.await_args.args[1]
    assert update_args["ended_at"] == later_ended  # extended forward
    assert update_args["started_at"] == existing.started_at  # unchanged


async def test_reuse_path_never_moves_started_at_backwards():
    existing = _existing_sentinel(
        started_at=datetime(2026, 7, 7, 10, 0, 0),
        ended_at=datetime(2026, 7, 7, 10, 0, 30),
    )
    session = MagicMock()
    repo = MagicMock()
    repo.get_by_hash = AsyncMock(return_value=existing)
    repo.update = AsyncMock(return_value=existing)
    service = VideoService(session)
    service.repo = repo

    earlier_started = datetime(2026, 7, 7, 9, 59, 50)
    sentinel, created = await service.get_or_create_edge_sentinel(
        station_id=1,
        station_code="STATION_01",
        started_at=earlier_started.replace(tzinfo=UTC),
        ended_at=existing.ended_at.replace(tzinfo=UTC),
    )
    assert created is False
    update_args = repo.update.await_args.args[1]
    # Earlier started_at is allowed (we always want the earliest seen).
    assert update_args["started_at"] == earlier_started


async def test_reuse_path_with_late_ended_does_not_clobber_window():
    """If a late batch reports an ended_at before the stored one,
    the stored value must win (max() not min())."""
    existing = _existing_sentinel(
        started_at=datetime(2026, 7, 7, 10, 0, 0),
        ended_at=datetime(2026, 7, 7, 10, 5, 0),
    )
    session = MagicMock()
    repo = MagicMock()
    repo.get_by_hash = AsyncMock(return_value=existing)
    repo.update = AsyncMock(return_value=existing)
    service = VideoService(session)
    service.repo = repo

    early_ended = datetime(2026, 7, 7, 10, 0, 10)
    await service.get_or_create_edge_sentinel(
        station_id=1,
        station_code="STATION_01",
        started_at=existing.started_at.replace(tzinfo=UTC),
        ended_at=early_ended.replace(tzinfo=UTC),
    )
    update_args = repo.update.await_args.args[1]
    # max(existing.ended_at, new.ended_at) -> existing wins.
    assert update_args["ended_at"] == existing.ended_at


async def test_reuse_path_uses_naive_utc_for_storage():
    """tz-aware datetimes must be converted to naive UTC before
    hitting the TIMESTAMP WITHOUT TIME ZONE column."""
    existing = _existing_sentinel(
        started_at=datetime(2026, 7, 7, 10, 0, 0),
        ended_at=datetime(2026, 7, 7, 10, 0, 30),
    )
    session = MagicMock()
    repo = MagicMock()
    repo.get_by_hash = AsyncMock(return_value=existing)
    repo.update = AsyncMock(return_value=existing)
    service = VideoService(session)
    service.repo = repo

    aware_started = datetime(2026, 7, 7, 10, 0, 0, tzinfo=UTC)
    aware_ended = datetime(2026, 7, 7, 10, 0, 45, tzinfo=UTC)
    await service.get_or_create_edge_sentinel(
        station_id=1,
        station_code="STATION_01",
        started_at=aware_started,
        ended_at=aware_ended,
    )
    update_args = repo.update.await_args.args[1]
    assert update_args["started_at"].tzinfo is None
    assert update_args["ended_at"].tzinfo is None
