"""Business logic for the ``events`` table.

Wraps :class:`EventRepository` and provides a single
:meth:`EventService.create_many` helper that the video handler calls once
per processed video to insert the full event list in one go.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loguru import logger
from sqlmodel.ext.asyncio.session import AsyncSession

from src.models.event import Event
from src.repositories.event import EventRepository
from src.schemas.event import EventCreate, EventPublic


def _to_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


class EventService:
    def __init__(self, session: AsyncSession):
        self.repo = EventRepository(session)

    async def create(self, payload: EventCreate) -> Event:
        data = payload.model_dump(exclude_unset=True)
        if "wall_clock_at" in data and data["wall_clock_at"] is not None:
            data["wall_clock_at"] = _to_naive_utc(data["wall_clock_at"])
        return await self.repo.create(data)

    async def create_many(self, payloads: list[EventCreate]) -> list[Event]:
        """Bulk insert; one flush per call to keep memory bounded."""
        if not payloads:
            return []
        events = []
        for p in payloads:
            data = p.model_dump(exclude_unset=True)
            if "wall_clock_at" in data and data["wall_clock_at"] is not None:
                data["wall_clock_at"] = _to_naive_utc(data["wall_clock_at"])
            event = Event(**data)
            self.repo.session.add(event)
            events.append(event)
        await self.repo.session.flush()
        for event in events:
            await self.repo.session.refresh(event)
        logger.info(f"Inserted {len(events)} events")
        return events

    async def list_by_video(self, video_id: int) -> list[Event]:
        return await self.repo.list_by_video(video_id)

    async def to_public(self, events: list[Event]) -> list[EventPublic]:
        return [
            EventPublic(
                id=e.id,
                video_file_id=e.video_file_id,
                tractor_id=e.tractor_id,
                wall_clock_at=e.wall_clock_at,
                created_at=e.created_at,
                event_type=e.event_type,
                detector_method=e.detector_method,
                inside_roi=e.inside_roi,
                frame_number=e.frame_number,
                timestamp_in_video=e.timestamp_in_video,
                confidence=e.confidence,
                aruco_id=e.aruco_id,
                bbox=e.bbox,
                detector_metadata=e.detector_metadata,
            )
            for e in events
        ]
