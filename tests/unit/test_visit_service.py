"""Unit tests for the visit state machine.

Mocks :class:`AsyncSession` and :class:`VisitRepository` so the state
transitions can be exercised in isolation, without a real PostgreSQL.
The repo's race-handling path (IntegrityError → re-fetch) gets its own
test via :class:`MagicMock` side_effects.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

from src.schemas.visit import VisitState
from src.services.visit_service import VisitService


def _make_visit(
    *,
    id: int | None = 1,
    tractor_id: int = 1,
    station_id: int = 1,
    state: VisitState = VisitState.ENTERING,
    last_seen_at: datetime | None = None,
    arrived_at: datetime | None = None,
    departed_at: datetime | None = None,
    entry_seen_seconds: float = 0.0,
    created_at: datetime | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=id,
        tractor_id=tractor_id,
        station_id=station_id,
        state=state,
        last_seen_at=last_seen_at,
        arrived_at=arrived_at,
        departed_at=departed_at,
        entry_seen_seconds=entry_seen_seconds,
        entry_event_id=None,
        exit_event_id=None,
        last_event_id=None,
        created_at=created_at or datetime.now(UTC),
        updated_at=datetime.now(UTC),
        duration_seconds=None,
    )


def _make_event(
    *,
    frame: int,
    tractor_id: int,
    inside_roi: bool = True,
    wall_clock: datetime | None = None,
    event_id: int | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=event_id,
        frame_number=frame,
        tractor_id=tractor_id,
        aruco_id=99,
        inside_roi=inside_roi,
        wall_clock_at=wall_clock or datetime.now(UTC),
    )


@pytest.fixture
def fake_session():
    s = MagicMock()
    s.add = MagicMock()
    s.flush = AsyncMock(return_value=None)
    s.commit = AsyncMock(return_value=None)
    s.delete = AsyncMock(return_value=None)
    s.rollback = AsyncMock(return_value=None)
    s.exec = AsyncMock()
    return s


@pytest.fixture
def service(fake_session):
    """A VisitService wired to the fake session."""
    return VisitService(fake_session)


@pytest.mark.unit
async def test_apply_state_transition_entering_to_present(service):
    """ENTERING flips to PRESENT once entry_seen_seconds reaches the threshold."""
    visit = _make_visit(
        state=VisitState.ENTERING,
        last_seen_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
        entry_seen_seconds=0.5,
    )
    event_time = datetime(2026, 1, 1, 12, 0, 2, tzinfo=UTC)
    event = _make_event(frame=1, tractor_id=1, wall_clock=event_time)

    await service._apply_state_transition(visit, event_time, event)

    assert visit.state == VisitState.PRESENT
    assert visit.entry_seen_seconds >= 1.0
    assert visit.arrived_at == event_time
    assert visit.last_seen_at == event_time


@pytest.mark.unit
async def test_apply_state_transition_present_advances_last_seen(service):
    """PRESENT visits only update last_seen_at, state does not change."""
    visit = _make_visit(
        state=VisitState.PRESENT,
        last_seen_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
        arrived_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
    )
    event_time = datetime(2026, 1, 1, 12, 0, 5, tzinfo=UTC)
    event = _make_event(frame=5, tractor_id=1, wall_clock=event_time)

    await service._apply_state_transition(visit, event_time, event)

    assert visit.state == VisitState.PRESENT
    assert visit.last_seen_at == event_time


@pytest.mark.unit
async def test_apply_state_transition_leaving_returns_to_present(service):
    """LEAVING with a fresh detection reverts to PRESENT."""
    visit = _make_visit(
        state=VisitState.LEAVING,
        last_seen_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
    )
    event_time = datetime(2026, 1, 1, 12, 0, 3, tzinfo=UTC)
    event = _make_event(frame=3, tractor_id=1, wall_clock=event_time)

    await service._apply_state_transition(visit, event_time, event)

    assert visit.state == VisitState.PRESENT
    assert visit.last_seen_at == event_time


@pytest.mark.unit
async def test_maybe_close_visit_entering_timeout_deletes(service, fake_session):
    """ENTERING older than 3*entry_confirm is deleted as a false positive."""
    visit = _make_visit(
        state=VisitState.ENTERING,
        created_at=datetime.now(UTC) - timedelta(seconds=300),
    )

    await service._maybe_close_visit(visit, datetime.now(UTC))

    fake_session.delete.assert_awaited_once_with(visit)


@pytest.mark.unit
async def test_maybe_close_visit_present_to_leaving(service):
    """PRESENT with a stale last_seen flips to LEAVING."""
    visit = _make_visit(
        state=VisitState.PRESENT,
        last_seen_at=datetime.now(UTC) - timedelta(seconds=15),
        arrived_at=datetime.now(UTC) - timedelta(seconds=30),
    )
    await service._maybe_close_visit(visit, datetime.now(UTC))
    assert visit.state == VisitState.LEAVING


@pytest.mark.unit
async def test_maybe_close_visit_leaving_to_closed(service):
    """LEAVING twice the exit window lands in CLOSED with departed_at set."""
    now_naive = datetime.now(UTC).replace(tzinfo=None)
    last_seen = now_naive - timedelta(seconds=30)
    arrived = last_seen - timedelta(seconds=60)
    visit = _make_visit(
        state=VisitState.LEAVING,
        last_seen_at=last_seen,
        arrived_at=arrived,
    )
    await service._maybe_close_visit(visit, datetime.now(UTC))
    assert visit.state == VisitState.CLOSED
    assert visit.departed_at is not None
    assert visit.departed_at > arrived


@pytest.mark.unit
async def test_create_or_get_open_visit_race_adoption(service, fake_session):
    """IntegrityError on the partial UNIQUE index falls back to the existing row."""
    fake_session.flush.side_effect = IntegrityError(
        "INSERT", {}, Exception("duplicate key")
    )
    existing = _make_visit(id=42, state=VisitState.ENTERING)

    service.repo.get_open_for_pair = AsyncMock(return_value=existing)

    result = await service._create_or_get_open_visit(
        tractor_id=1,
        station_id=1,
        frame_time=datetime.now(UTC),
        event_id=None,
    )

    fake_session.rollback.assert_awaited_once()
    assert result is existing


@pytest.mark.unit
async def test_process_video_for_visits_creates_entering(service, fake_session):
    """No prior open visit → first event creates an ENTERING visit."""
    service.repo.list_open_for_station = AsyncMock(return_value=[])

    video = SimpleNamespace(id=10, station_id=7)
    events = [_make_event(frame=1, tractor_id=3)]

    await service.process_video_for_visits(video, events)

    fake_session.add.assert_called()
    fake_session.commit.assert_awaited_once()


@pytest.mark.unit
async def test_process_video_for_visits_skips_unknown_tractor(service, fake_session):
    """Events with tractor_id=None are dropped from aggregation."""
    service.repo.list_open_for_station = AsyncMock(return_value=[])

    video = SimpleNamespace(id=10, station_id=7)
    events = [
        _make_event(frame=1, tractor_id=None),
        _make_event(frame=2, tractor_id=None, inside_roi=False),
    ]

    await service.process_video_for_visits(video, events)

    fake_session.add.assert_not_called()
    fake_session.commit.assert_awaited_once()


@pytest.mark.unit
async def test_process_video_for_visits_video_without_station(service, fake_session):
    """If video has no station_id, the aggregator bails out early."""
    service.repo.list_open_for_station = AsyncMock()
    video = SimpleNamespace(id=10, station_id=None)
    events = [_make_event(frame=1, tractor_id=3)]

    await service.process_video_for_visits(video, events)

    service.repo.list_open_for_station.assert_not_called()


@pytest.mark.unit
async def test_process_video_for_visits_stale_present_revives_via_new_event(service):
    """PRESENT with gap ≥ exit_confirm flips to LEAVING; the next in-ROI
    detection immediately revives the visit back to PRESENT."""
    arrived = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    stale_present = _make_visit(
        id=1,
        tractor_id=1,
        station_id=1,
        state=VisitState.PRESENT,
        arrived_at=arrived,
        last_seen_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
    )

    service.repo.list_open_for_station = AsyncMock(return_value=[stale_present])

    video = SimpleNamespace(id=10, station_id=1)
    fresh_event_time = datetime(2026, 1, 1, 12, 0, 12, tzinfo=UTC)
    events = [_make_event(frame=15, tractor_id=1, wall_clock=fresh_event_time)]

    await service.process_video_for_visits(video, events)

    # Gap = 12s ≥ exit_confirm(10s) → LEAVING, then new detection → PRESENT.
    assert stale_present.state == VisitState.PRESENT


@pytest.mark.unit
async def test_process_video_for_visits_stale_present_flips_then_closes(service):
    """A PRESENT visit whose last_seen is older than 2*exit_confirm
    closes itself before the next event is processed."""
    arrived = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    stale_present = _make_visit(
        id=1,
        tractor_id=1,
        station_id=1,
        state=VisitState.PRESENT,
        arrived_at=arrived,
        last_seen_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
    )

    service.repo.list_open_for_station = AsyncMock(return_value=[stale_present])

    video = SimpleNamespace(id=10, station_id=1)
    fresh_event_time = datetime(2026, 1, 1, 12, 0, 30, tzinfo=UTC)
    events = [_make_event(frame=15, tractor_id=1, wall_clock=fresh_event_time)]

    await service.process_video_for_visits(video, events)

    # Gap = 30s ≥ exit_confirm(10s) → LEAVING, then ≥ 2*exit_confirm(20s) → CLOSED.
    assert stale_present.state == VisitState.CLOSED


@pytest.mark.unit
async def test_check_stale_visits_closes_active(service, fake_session):
    """Stale PRESENT/LEAVING rows should be closed by check_stale_visits."""
    now = datetime.now(UTC)
    stale_present = _make_visit(
        state=VisitState.PRESENT,
        last_seen_at=now - timedelta(seconds=300),
        arrived_at=now - timedelta(seconds=600),
    )

    service.repo.list_active = AsyncMock(return_value=[stale_present])

    closed = await service.check_stale_visits()

    assert closed == 1
    assert stale_present.state == VisitState.CLOSED


@pytest.mark.unit
async def test_recover_open_visits_deletes_stale_entering(service, fake_session):
    """ENTERING older than recovery_grace is deleted on startup."""
    stale = _make_visit(
        state=VisitState.ENTERING,
        created_at=datetime.now(UTC) - timedelta(seconds=600),
    )
    fresh = _make_visit(
        id=2,
        state=VisitState.PRESENT,
        arrived_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    service.repo.list_open = AsyncMock(return_value=[stale, fresh])

    summary = await service.recover_open_visits()

    assert summary["deleted_enterings"] == 1
    fake_session.delete.assert_awaited_once_with(stale)


@pytest.mark.unit
async def test_get_current_tractors_maps_dwell(service):
    visits = [
        _make_visit(
            id=1,
            tractor_id=1,
            station_id=7,
            state=VisitState.PRESENT,
            arrived_at=datetime.now(UTC) - timedelta(seconds=42),
        ),
        _make_visit(
            id=2,
            tractor_id=2,
            station_id=7,
            state=VisitState.ENTERING,
            arrived_at=None,
            last_seen_at=datetime.now(UTC),
        ),
    ]
    service.repo.list_open = AsyncMock(return_value=visits)

    out = await service.get_current_tractors()

    assert len(out) == 2
    dwell = next(o for o in out if o["tractor_id"] == 1)["current_dwell_seconds"]
    assert dwell is not None and dwell >= 40


@pytest.mark.unit
async def test_get_current_tractor_returns_absent(service):
    service.repo.list_open_for_tractor = AsyncMock(return_value=[])

    out = await service.get_current_tractor(99)

    assert out == {"tractor_id": 99, "state": "ABSENT"}
