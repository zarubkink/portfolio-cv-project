"""End-to-end integration test for the MQTT edge ingestion path.

This test exercises the live FastAPI stack (``api`` service must be
reachable) and the real PostgreSQL instance behind it. It is the
integration counterpart to the unit tests in
``tests/unit/test_events_ingest.py`` and ``test_mqtt_consumer.py``.

What it verifies:

* ``POST /v1/events/ingest`` with a synthetic batch creates exactly
  one sentinel ``VideoFile`` row (per station) and inserts every
  event row pointing at the sentinel.
* Re-posting for the same station reuses the existing sentinel
  rather than creating a duplicate.
* Sentinel lookup uses the same sha256("edge://<code>") rule
  regardless of payload arrival order.

The MQTT broker and the consumer process are not in this test's
path — the api endpoint is the boundary under test. The consumer
forwarding is exercised through the unit tests, where the broker
is stubbed.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text

from src.dependencies import engine
from src.services.video_service import _edge_sentinel_uri

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ───────────────────────────────────────────────────────────────────────
# Fixtures
# ───────────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture(loop_scope="session")
async def cleanup_station(unique_code: str):
    """Drop any rows that the unique_code station left behind.

    Uses the shared session-scoped engine directly so the teardown
    runs on the same loop as the rest of the test session — going
    through AsyncSession(engine) here would bind a new session to
    the per-function loop, which trips the
    "Future attached to a different loop" warning at teardown.
    """
    yield
    sentinel_uri = _edge_sentinel_uri(unique_code)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "DELETE FROM events WHERE video_file_id IN "
                "(SELECT id FROM video_files WHERE storage_uri = :uri)"
            ),
            {"uri": sentinel_uri},
        )
        await conn.execute(
            text("DELETE FROM video_files WHERE storage_uri = :uri"),
            {"uri": sentinel_uri},
        )
        await conn.execute(
            text("DELETE FROM stations WHERE code = :code"),
            {"code": unique_code},
        )


# ───────────────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────────────


async def _ensure_station(api_client: httpx.AsyncClient, code: str) -> int:
    """Create the test station via the public station endpoint.
    Returns the station id."""
    payload = {
        "code": code,
        "name": f"Test station {code}",
        "video_dir": f"./data/queue/{code}",
        "is_entry_zone": True,
        "is_active": True,
    }
    response = await api_client.post("/v1/stations/", json=payload)
    if response.status_code == 201:
        return response.json()["id"]
    # Already exists from a previous run; look it up by filter.
    response = await api_client.post("/v1/stations/filter", json={"code": code})
    response.raise_for_status()
    rows = response.json()
    assert rows, f"station {code} not found after non-201 response"
    return rows[0]["id"]


def _batch_payload(station_code: str, *, n_events: int) -> dict:
    started = datetime.now(UTC)
    ended = started + timedelta(seconds=5)
    return {
        "station_code": station_code,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "events": [
            {
                "aruco_id": i + 1,
                "confidence": 0.99,
                "bbox": {"x": i * 10, "y": 0, "w": 50, "h": 50},
                "inside_roi": True,
                "frame_number": i,
                "timestamp_in_video": i * 0.1,
                "detector_metadata": {"triggered_by_mog2": True},
            }
            for i in range(n_events)
        ],
    }


# ───────────────────────────────────────────────────────────────────────
# Happy path
# ───────────────────────────────────────────────────────────────────────


async def test_ingest_creates_sentinel_and_events(
    api_reachable, api_client, unique_code, cleanup_station
):
    await _ensure_station(api_client, unique_code)

    payload = _batch_payload(unique_code, n_events=3)
    response = await api_client.post("/v1/events/ingest", json=payload)
    assert response.status_code == 201, response.text

    body = response.json()
    assert body["sentinel_created"] is True
    assert body["events_created"] == 3
    sentinel_id = body["video_file_id"]

    # Verify the sentinel row exists with the expected hash.
    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT storage_uri, status, content_hash "
                "FROM video_files WHERE id = :id"
            ),
            {"id": sentinel_id},
        )
        row = result.mappings().one()
    assert row["storage_uri"] == _edge_sentinel_uri(unique_code)
    assert row["status"] == "PROCESSING"
    assert (
        bytes(row["content_hash"])
        == hashlib.sha256(_edge_sentinel_uri(unique_code).encode()).digest()
    )

    # Verify the events point at the sentinel.
    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT aruco_id, frame_number FROM events "
                "WHERE video_file_id = :id ORDER BY frame_number"
            ),
            {"id": sentinel_id},
        )
        rows = result.mappings().all()
    assert [r["aruco_id"] for r in rows] == [1, 2, 3]
    assert [r["frame_number"] for r in rows] == [0, 1, 2]


async def test_ingest_reuses_sentinel_on_second_post(
    api_reachable, api_client, unique_code, cleanup_station
):
    await _ensure_station(api_client, unique_code)

    first = await api_client.post(
        "/v1/events/ingest", json=_batch_payload(unique_code, n_events=2)
    )
    assert first.status_code == 201, first.text
    first_body = first.json()
    assert first_body["sentinel_created"] is True

    second = await api_client.post(
        "/v1/events/ingest", json=_batch_payload(unique_code, n_events=5)
    )
    assert second.status_code == 201, second.text
    second_body = second.json()
    assert second_body["sentinel_created"] is False
    assert second_body["video_file_id"] == first_body["video_file_id"]
    assert second_body["events_created"] == 5

    # All 7 events (2 + 5) live under the same sentinel.
    async with engine.connect() as conn:
        result = await conn.execute(
            text("SELECT COUNT(*) FROM events WHERE video_file_id = :id"),
            {"id": first_body["video_file_id"]},
        )
        count = result.scalar_one()
    assert count == 7


async def test_ingest_empty_batch_returns_sentinel_without_events(
    api_reachable, api_client, unique_code, cleanup_station
):
    await _ensure_station(api_client, unique_code)

    payload = _batch_payload(unique_code, n_events=0)
    response = await api_client.post("/v1/events/ingest", json=payload)
    assert response.status_code == 201, response.text

    body = response.json()
    assert body["events_created"] == 0
    assert body["sentinel_created"] is True


# ───────────────────────────────────────────────────────────────────────
# Error paths
# ───────────────────────────────────────────────────────────────────────


async def test_ingest_unknown_station_returns_404(api_reachable, api_client):
    payload = _batch_payload("STATION_DOES_NOT_EXIST_X", n_events=1)
    response = await api_client.post("/v1/events/ingest", json=payload)
    assert response.status_code == 404
    assert "STATION_DOES_NOT_EXIST_X" in response.text


async def test_ingest_rejects_empty_station_code(api_reachable, api_client):
    payload = _batch_payload("ignored", n_events=1)
    payload["station_code"] = ""
    response = await api_client.post("/v1/events/ingest", json=payload)
    assert response.status_code == 422


async def test_ingest_rejects_negative_frame_number(
    api_reachable, api_client, unique_code, cleanup_station
):
    await _ensure_station(api_client, unique_code)
    payload = _batch_payload(unique_code, n_events=1)
    payload["events"][0]["frame_number"] = -1
    response = await api_client.post("/v1/events/ingest", json=payload)
    assert response.status_code == 422
