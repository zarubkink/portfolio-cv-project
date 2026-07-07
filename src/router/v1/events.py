"""``POST /v1/events/ingest`` — entry point for the MQTT consumer.

The edge device publishes detection batches to ``farm/<code>/detections``;
the consumer (a separate process) forwards each batch to this endpoint.
We keep the route on the FastAPI app instead of writing to the DB from
inside the consumer because:

* the api process owns the DB connection pool and the session lifecycle;
* the same write path is exercised by both HTTP and MQTT consumers, so
  regressions show up here too;
* tests can hit the endpoint directly without spinning up a broker.

The endpoint manufactures a "live stream sentinel" ``VideoFile`` row
per station (see :func:`VideoService.get_or_create_edge_sentinel`)
and writes the batched detections as :class:`Event` rows that point
at the sentinel. This sidesteps the ``events.video_file_id NOT NULL``
FK without a schema change.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger
from sqlmodel.ext.asyncio.session import AsyncSession

from src.dependencies import get_async_session
from src.repositories.station import StationRepository
from src.schemas.edge_ingest import EdgeBatchIn, EdgeBatchOut
from src.schemas.event import DetectorMethod, EventCreate, EventType
from src.services.event_service import EventService
from src.services.video_service import VideoService

router = APIRouter(prefix="/events", tags=["events"])


@router.post(
    "/ingest",
    response_model=EdgeBatchOut,
    status_code=status.HTTP_201_CREATED,
)
async def ingest_edge_batch(
    payload: EdgeBatchIn,
    session: AsyncSession = Depends(get_async_session),
) -> EdgeBatchOut:
    """Persist a detection batch from the edge process."""
    station = await StationRepository(session).get_by_code(payload.station_code)
    if station is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Station not found: {payload.station_code}",
        )

    video_service = VideoService(session)
    sentinel, created_now = await video_service.get_or_create_edge_sentinel(
        station_id=station.id,
        station_code=payload.station_code,
        started_at=payload.started_at,
        ended_at=payload.ended_at,
    )

    if not payload.events:
        logger.info(
            f"Edge batch for {payload.station_code} arrived empty; "
            f"sentinel id={sentinel.id} (created={created_now})"
        )
        return EdgeBatchOut(
            video_file_id=sentinel.id,
            events_created=0,
            sentinel_created=created_now,
        )

    event_service = EventService(session)
    created = await event_service.create_many([
        EventCreate(
            video_file_id=sentinel.id,
            aruco_id=evt.aruco_id,
            confidence=evt.confidence,
            bbox=evt.bbox,
            inside_roi=evt.inside_roi,
            frame_number=evt.frame_number,
            timestamp_in_video=evt.timestamp_in_video,
            wall_clock_at=payload.ended_at,
            event_type=EventType.DETECTED,
            detector_method=DetectorMethod.ARUCO,
            detector_metadata=evt.detector_metadata,
        )
        for evt in payload.events
    ])

    logger.info(
        f"Edge ingest for {payload.station_code}: "
        f"sentinel id={sentinel.id} (created={created_now}), "
        f"events written={len(created)}"
    )
    return EdgeBatchOut(
        video_file_id=sentinel.id,
        events_created=len(created),
        sentinel_created=created_now,
    )


__all__ = ["router"]
