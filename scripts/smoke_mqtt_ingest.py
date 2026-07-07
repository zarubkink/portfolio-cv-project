"""Smoke test for the MQTT edge ingestion path.

Exercises the full ``POST /v1/events/ingest`` route against the live
api container:

1. Create a smoke station via the public station endpoint.
2. POST a synthetic batch to ``/v1/events/ingest``.
3. Verify the response and a couple of DB rows.
4. Tear the station down.

The MQTT broker and the consumer process are not exercised here —
the broker piece is covered by the unit tests for
:class:`src.services.mqtt_client.MqttClient`, and the consumer
forwarding is exercised by ``tests/unit/test_mqtt_consumer.py``.
This script confirms that the api endpoint is wired up correctly
on a fresh stack so an operator can run it as part of a release
checklist.

Usage:
    uv run python scripts/smoke_mqtt_ingest.py
    API_BASE_URL=http://my-api:8000 uv run python scripts/smoke_mqtt_ingest.py
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
import uuid
from datetime import UTC, datetime, timedelta

import httpx
from loguru import logger
from sqlalchemy import text

from src.services.video_service import _edge_sentinel_uri

API_BASE_URL = "http://localhost:8000"


def _batch_payload(station_code: str) -> dict:
    started = datetime.now(UTC)
    ended = started + timedelta(seconds=5)
    return {
        "station_code": station_code,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "events": [
            {
                "aruco_id": 7,
                "confidence": 0.99,
                "bbox": {"x": 100, "y": 50, "w": 140, "h": 140},
                "inside_roi": True,
                "frame_number": 30,
                "timestamp_in_video": 1.0,
                "detector_metadata": {"triggered_by_mog2": True},
            }
        ],
    }


async def _cleanup(engine, station_code: str) -> None:
    sentinel_uri = _edge_sentinel_uri(station_code)
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
            {"code": station_code},
        )


async def main() -> int:
    # Lazy import so the script's --help and ModuleNotFoundError paths
    # are easier to debug.
    from src.dependencies import engine

    code = f"SMOKE_{uuid.uuid4().hex[:8].upper()}"
    payload = _batch_payload(code)
    expected_hash = hashlib.sha256(_edge_sentinel_uri(code).encode()).digest()

    async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=10.0) as client:
        # 1. health
        try:
            r = await client.get("/health")
            r.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            logger.error(f"API not reachable at {API_BASE_URL}: {exc}")
            return 1

        # 2. create station
        station = await client.post(
            "/v1/stations/",
            json={
                "code": code,
                "name": f"Smoke station {code}",
                "video_dir": f"./data/queue/{code}",
            },
        )
        station.raise_for_status()
        logger.info(f"created station id={station.json()['id']} code={code}")

        # 3. POST batch
        try:
            response = await client.post("/v1/events/ingest", json=payload)
            response.raise_for_status()
            body = response.json()
            logger.info(
                f"ingest ok: video_file_id={body['video_file_id']}, "
                f"events_created={body['events_created']}, "
                f"sentinel_created={body['sentinel_created']}"
            )

            # 4. verify DB row.
            async with engine.connect() as conn:
                row = (
                    (
                        await conn.execute(
                            text(
                                "SELECT storage_uri, status, content_hash "
                                "FROM video_files WHERE id = :id"
                            ),
                            {"id": body["video_file_id"]},
                        )
                    )
                    .mappings()
                    .one()
                )
            assert row["storage_uri"] == _edge_sentinel_uri(code)
            assert row["status"] == "PROCESSING"
            assert bytes(row["content_hash"]) == expected_hash
            logger.success("DB row matches: status=PROCESSING, hash ok")

            # 5. verify second post reuses the sentinel.
            response = await client.post("/v1/events/ingest", json=payload)
            response.raise_for_status()
            body2 = response.json()
            assert body2["sentinel_created"] is False, body2
            assert body2["video_file_id"] == body["video_file_id"], body2
            logger.success(f"second ingest reused sentinel id={body2['video_file_id']}")

        finally:
            await _cleanup(engine, code)
            logger.info(f"cleaned up station {code}")

    logger.success("smoke_mqtt_ingest: OK")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
