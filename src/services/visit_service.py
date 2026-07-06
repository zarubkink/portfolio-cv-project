"""Business logic for the ``visits`` table — visit state machine.

Implements the four-state lifecycle described in PROJECT_SCAFFOLD_PROMPT.md
section 3.8:

    ABSENT  →  ENTERING  →  PRESENT  →  LEAVING  →  CLOSED

``ABSENT`` is the implicit "no row" state. The remaining four are stored
in ``visits.state``. The repository enforces "at most one open visit per
(tractor, station)" via the partial UNIQUE index ``uq_visit_open``; the
service handles the IntegrityError on the loser side.

The state machine is driven by :meth:`process_video_for_visits`, which
the video handler calls after inserting events. A periodic
:meth:`check_stale_visits` task (started in the FastAPI lifespan)
advances PRESENT/LEAVING visits to CLOSED once the tractor has been
gone for ``exit_confirm_seconds``. On startup, :meth:`recover_open_visits`
deletes stale ENTERING visits that survived a crash.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

from loguru import logger
from sqlalchemy.exc import IntegrityError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.config.visit import visit_settings
from src.models.event import Event
from src.models.station import Station
from src.models.tractor import Tractor
from src.models.video_file import VideoFile
from src.models.visit import Visit
from src.repositories.visit import VisitRepository
from src.schemas.visit import VisitState


def _to_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


class VisitService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.repo = VisitRepository(session)
        self.entry_confirm = visit_settings.entry_confirm_seconds
        self.exit_confirm = visit_settings.exit_confirm_seconds

    # ───────────────────────────────────────────────────────────────
    # Public entry point called from the video handler
    # ───────────────────────────────────────────────────────────────

    async def process_video_for_visits(
        self,
        video: VideoFile,
        events: Iterable[Event],
    ) -> None:
        """Apply the state machine to every (tractor, station) event.

        Events are sorted by ``frame_number`` so that earlier frames are
        processed first. Events whose ``tractor_id`` is ``None`` (unknown
        ArUco) or whose ``inside_roi`` is ``False`` are skipped — they
        cannot contribute to a visit. The aggregator works in
        ``wall_clock_at`` terms so a tractor that crosses video
        boundaries stays in the same visit.
        """
        event_list = sorted(events, key=lambda e: e.frame_number)
        if not event_list:
            return

        station_id = video.station_id
        if station_id is None:
            logger.warning(
                f"video {video.id} has no station_id; skipping visit aggregation"
            )
            return

        active_visits = await self._get_open_visits_for_station(station_id)
        last_seen: dict[tuple[int, int], datetime] = {
            pair: v.last_seen_at for pair, v in active_visits.items()
        }

        for event in event_list:
            if event.tractor_id is None or not event.inside_roi:
                continue

            pair = (event.tractor_id, station_id)
            frame_time = event.wall_clock_at
            visit = active_visits.get(pair)

            # Close out a stale open visit if we've gone quiet for too long.
            if visit is not None and visit.state == VisitState.PRESENT:
                gap = (frame_time - last_seen[pair]).total_seconds()
                if gap >= self.exit_confirm:
                    visit.state = VisitState.LEAVING
                    await self._maybe_close_visit(visit, frame_time)
                    last_seen[pair] = frame_time

            if visit is None or visit.state == VisitState.CLOSED:
                visit = await self._create_or_get_open_visit(
                    tractor_id=pair[0],
                    station_id=station_id,
                    frame_time=frame_time,
                    event_id=event.id,
                )
                active_visits[pair] = visit
                last_seen[pair] = frame_time
            else:
                await self._apply_state_transition(visit, frame_time, event)
                last_seen[pair] = frame_time

        # After the loop, force-close anything still left open.
        final_time = event_list[-1].wall_clock_at
        for visit in active_visits.values():
            if visit.state != VisitState.CLOSED:
                await self._maybe_close_visit(visit, final_time)

        await self.session.commit()

    # ───────────────────────────────────────────────────────────────
    # State machine internals
    # ───────────────────────────────────────────────────────────────

    async def _apply_state_transition(
        self,
        visit: Visit,
        frame_time: datetime,
        event: Event,
    ) -> None:
        now = _to_naive_utc(frame_time)
        if visit.state == VisitState.ENTERING:
            anchor = _to_naive_utc(visit.last_seen_at or visit.created_at or frame_time)
            elapsed = (now - anchor).total_seconds()
            visit.entry_seen_seconds = (visit.entry_seen_seconds or 0.0) + elapsed
            visit.last_seen_at = frame_time
            visit.last_event_id = event.id
            if visit.entry_seen_seconds >= self.entry_confirm:
                visit.state = VisitState.PRESENT
                visit.arrived_at = frame_time
                visit.entry_event_id = event.id
                logger.info(
                    f"Visit {visit.id} → PRESENT (entry_seen="
                    f"{visit.entry_seen_seconds:.1f}s)"
                )
        elif visit.state == VisitState.PRESENT:
            visit.last_seen_at = frame_time
            visit.last_event_id = event.id
        elif visit.state == VisitState.LEAVING:
            visit.state = VisitState.PRESENT
            visit.last_seen_at = frame_time
            visit.last_event_id = event.id
            logger.info(f"Visit {visit.id} → PRESENT (resumed)")
        else:  # CLOSED — should not happen here, but log if it does
            logger.warning(
                f"Visit {visit.id} is CLOSED but received an event; ignoring"
            )
        self.session.add(visit)

    async def _maybe_close_visit(self, visit: Visit, current_time: datetime) -> None:
        """Drive PRESENT/LEAVING → CLOSED based on the exit timer.

        ENTERING visits older than ``3 * entry_confirm`` are deleted as
        false positives (tractor drove past without actually entering).
        """
        now = _to_naive_utc(current_time)
        if visit.state == VisitState.ENTERING:
            anchor = _to_naive_utc(visit.created_at or current_time)
            gap = (now - anchor).total_seconds()
            if gap >= self.entry_confirm * 3:
                logger.info(
                    f"Visit {visit.id} deleted (ENTERING timeout, gap={gap:.1f}s)"
                )
                await self.session.delete(visit)
            return

        if visit.state == VisitState.PRESENT:
            gap = (
                now - _to_naive_utc(visit.last_seen_at or current_time)
            ).total_seconds()
            if gap >= self.exit_confirm:
                visit.state = VisitState.LEAVING
                logger.info(f"Visit {visit.id} → LEAVING (gap={gap:.1f}s)")

        if visit.state == VisitState.LEAVING:
            gap = (
                now - _to_naive_utc(visit.last_seen_at or current_time)
            ).total_seconds()
            if gap >= 2 * self.exit_confirm:
                visit.state = VisitState.CLOSED
                visit.departed_at = _to_naive_utc(
                    visit.last_seen_at or current_time
                ) + timedelta(seconds=self.exit_confirm)
                logger.info(
                    f"Visit {visit.id} → CLOSED (duration={visit.duration_seconds}s)"
                )
        self.session.add(visit)

    # ───────────────────────────────────────────────────────────────
    # CRUD helpers
    # ───────────────────────────────────────────────────────────────

    async def _create_or_get_open_visit(
        self,
        tractor_id: int,
        station_id: int,
        frame_time: datetime,
        event_id: int | None,
    ) -> Visit | None:
        """Create an ENTERING visit, or adopt the one a concurrent worker created.

        Returns ``None`` if no visit row could be produced (e.g. tractor
        no longer exists because of a parallel delete).
        """
        try:
            visit = Visit(
                tractor_id=tractor_id,
                station_id=station_id,
                state=VisitState.ENTERING,
                last_seen_at=frame_time,
                entry_seen_seconds=0.0,
            )
            if event_id is not None:
                visit.entry_event_id = event_id
                visit.last_event_id = event_id
            self.session.add(visit)
            await self.session.flush()
            return visit
        except IntegrityError:
            await self.session.rollback()
            existing = await self.repo.get_open_for_pair(tractor_id, station_id)
            if existing is not None:
                return existing
            # The loser path lost its session on rollback; the caller
            # needs to re-query. Signal this with None and let the next
            # event retry.
            return None

    async def _get_open_visits_for_station(
        self, station_id: int
    ) -> dict[tuple[int, int], Visit]:
        rows = await self.repo.list_open_for_station(station_id)
        return {(v.tractor_id, v.station_id): v for v in rows}

    # ───────────────────────────────────────────────────────────────
    # API helpers
    # ───────────────────────────────────────────────────────────────

    async def get_current_tractors(self) -> list[dict]:
        visits = await self.repo.list_open()
        now = _to_naive_utc(datetime.now(UTC))
        return [
            {
                "tractor_id": v.tractor_id,
                "station_id": v.station_id,
                "state": v.state.value,
                "arrived_at": v.arrived_at,
                "last_seen_at": v.last_seen_at,
                "current_dwell_seconds": (
                    (now - _to_naive_utc(v.arrived_at)).total_seconds()
                    if v.arrived_at
                    else None
                ),
            }
            for v in visits
        ]

    async def get_current_tractor(self, tractor_id: int) -> dict:
        visits = await self.repo.list_open_for_tractor(tractor_id)
        visit = visits[0] if visits else None
        if visit is None:
            return {
                "tractor_id": tractor_id,
                "state": VisitState.ABSENT.value,
            }
        now = _to_naive_utc(datetime.now(UTC))
        return {
            "tractor_id": visit.tractor_id,
            "station_id": visit.station_id,
            "state": visit.state.value,
            "arrived_at": visit.arrived_at,
            "last_seen_at": visit.last_seen_at,
            "current_dwell_seconds": (
                (now - _to_naive_utc(visit.arrived_at)).total_seconds()
                if visit.arrived_at
                else None
            ),
        }

    async def get_current_stations(self) -> list[dict]:
        result = await self.session.exec(
            select(Station, Visit, Tractor)
            .outerjoin(
                Visit,
                (Visit.station_id == Station.id) & (Visit.state != VisitState.CLOSED),
            )
            .outerjoin(Tractor, Tractor.id == Visit.tractor_id)
            .where(Station.is_active == True)  # noqa: E712
        )
        rows = result.all()
        now = _to_naive_utc(datetime.now(UTC))
        grouped: dict[int, dict] = {}
        for station, visit, tractor in rows:
            entry = grouped.setdefault(
                station.id,
                {
                    "station_id": station.id,
                    "code": station.code,
                    "name": station.name,
                    "tractors": [],
                },
            )
            if visit is not None:
                entry["tractors"].append({
                    "tractor_id": tractor.id if tractor else None,
                    "tractor_name": tractor.name if tractor else None,
                    "state": visit.state.value,
                    "arrived_at": visit.arrived_at,
                    "current_dwell_seconds": (
                        (now - _to_naive_utc(visit.arrived_at)).total_seconds()
                        if visit.arrived_at
                        else None
                    ),
                })
        return list(grouped.values())

    async def get_history(
        self,
        tractor_id: int | None = None,
        station_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Visit]:
        return await self.repo.list_history(
            tractor_id=tractor_id,
            station_id=station_id,
            limit=limit,
            offset=offset,
        )

    # ───────────────────────────────────────────────────────────────
    # Lifecycle: periodic check + recovery
    # ───────────────────────────────────────────────────────────────

    async def check_stale_visits(self) -> int:
        """Close visits whose last_seen_at is older than exit_confirm."""
        visits = await self.repo.list_active()
        now = datetime.now(UTC)
        closed = 0
        for visit in visits:
            before = visit.state
            await self._maybe_close_visit(visit, now)
            if visit.state == VisitState.CLOSED and before != VisitState.CLOSED:
                closed += 1
        await self.session.commit()
        return closed

    async def recover_open_visits(self) -> dict:
        """Run on startup: delete ENTERING visits that survived a crash."""
        visits = await self.repo.list_open()
        now = _to_naive_utc(datetime.now(UTC))
        deleted_enterings = 0
        for visit in visits:
            if visit.state == VisitState.ENTERING:
                anchor = _to_naive_utc(visit.created_at or datetime.now(UTC))
                age = (now - anchor).total_seconds()
                if age > self.entry_confirm * visit_settings.recovery_grace_multiplier:
                    logger.info(
                        f"Visit {visit.id} deleted on startup (ENTERING age={age:.1f}s)"
                    )
                    await self.session.delete(visit)
                    deleted_enterings += 1
        await self.session.commit()
        return {"recovered_visits": len(visits), "deleted_enterings": deleted_enterings}


__all__ = ["VisitService"]
