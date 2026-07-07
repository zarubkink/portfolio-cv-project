"""Unit tests for :class:`mqtt_consumer.main.MqttConsumer`.

The consumer talks to an MQTT broker and to the FastAPI ingest
endpoint over HTTP. Both collaborators are mocked here:

* :meth:`MqttClient.run_forever` is replaced by a one-shot call
  that feeds a synthetic message into the handler;
* the ``httpx.AsyncClient`` is replaced by a stub whose ``post``
  method returns a configurable ``Response``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from mqtt_consumer.main import MqttConsumer
from src.services.mqtt_client import MqttMessage


def _make_message(payload: dict | bytes) -> MqttMessage:
    if isinstance(payload, dict):
        payload = json.dumps(payload).encode("utf-8")
    return MqttMessage(topic="farm/STATION_01/detections", payload=payload)


@pytest.fixture
def consumer() -> MqttConsumer:
    c = MqttConsumer(
        api_url="http://api.test",
        ingest_path="/v1/events/ingest",
        request_timeout_sec=1.0,
        max_retries=3,
        retry_backoff_sec=0.0,
    )
    return c


# ───────────────────────────────────────────────────────────────────────
# Timestamp normalisation
# ───────────────────────────────────────────────────────────────────────


def test_normalise_timestamps_converts_float_seconds(consumer):
    payload = {"started_at": 100.0, "ended_at": 105.5, "events": []}
    out = consumer._normalise_timestamps(payload)
    # Both fields converted to ISO-8601 strings.
    assert isinstance(out["started_at"], str)
    assert isinstance(out["ended_at"], str)
    datetime.fromisoformat(out["started_at"])
    datetime.fromisoformat(out["ended_at"])


def test_normalise_timestamps_passes_iso_strings_through(consumer):
    iso = "2026-07-07T10:00:00"
    payload = {"started_at": iso, "ended_at": iso, "events": []}
    out = consumer._normalise_timestamps(payload)
    assert out["started_at"] == iso
    assert out["ended_at"] == iso


def test_normalise_timestamps_does_not_mutate_input(consumer):
    payload = {"started_at": 1.0, "ended_at": 2.0, "events": []}
    consumer._normalise_timestamps(payload)
    # input dict must remain untouched (caller may reuse it)
    assert payload["started_at"] == 1.0


# ───────────────────────────────────────────────────────────────────────
# Malformed payloads
# ───────────────────────────────────────────────────────────────────────


async def test_handle_message_drops_malformed_json(consumer):
    msg = _make_message(b"this is not json")
    consumer._http = MagicMock()
    consumer._http.post = AsyncMock()
    await consumer._handle_message(msg)
    consumer._http.post.assert_not_awaited()


async def test_handle_message_drops_payload_that_fails_schema(consumer):
    payload = {"station_code": "STATION_01"}  # missing required fields
    msg = _make_message(payload)
    consumer._http = MagicMock()
    consumer._http.post = AsyncMock()
    await consumer._handle_message(msg)
    consumer._http.post.assert_not_awaited()


# ───────────────────────────────────────────────────────────────────────
# Forwarding + retry
# ───────────────────────────────────────────────────────────────────────


def _success_response() -> httpx.Response:
    return httpx.Response(
        201,
        json={"video_file_id": 1, "events_created": 3, "sentinel_created": True},
    )


async def test_forward_posts_valid_payload(consumer):
    payload = {
        "station_code": "STATION_01",
        "started_at": datetime.now(UTC).isoformat(),
        "ended_at": datetime.now(UTC).isoformat(),
        "events": [
            {
                "aruco_id": 7,
                "confidence": 0.9,
                "inside_roi": True,
                "frame_number": 1,
                "timestamp_in_video": 0.1,
            }
        ],
    }
    msg = _make_message(payload)
    http = MagicMock()
    http.post = AsyncMock(return_value=_success_response())
    consumer._http = http

    await consumer._handle_message(msg)

    http.post.assert_awaited_once()
    url, kwargs = http.post.await_args.args[0], http.post.await_args.kwargs
    assert url == "http://api.test/v1/events/ingest"
    assert "json" in kwargs
    assert kwargs["json"]["station_code"] == "STATION_01"
    assert len(kwargs["json"]["events"]) == 1


async def test_forward_does_not_retry_on_4xx(consumer):
    payload = {
        "station_code": "STATION_01",
        "started_at": datetime.now(UTC).isoformat(),
        "ended_at": datetime.now(UTC).isoformat(),
        "events": [],
    }
    msg = _make_message(payload)
    http = MagicMock()
    http.post = AsyncMock(return_value=httpx.Response(422, json={"detail": "bad"}))
    consumer._http = http

    await consumer._handle_message(msg)

    # 4xx is permanent — single attempt only.
    http.post.assert_awaited_once()


async def test_forward_retries_on_5xx(consumer):
    payload = {
        "station_code": "STATION_01",
        "started_at": datetime.now(UTC).isoformat(),
        "ended_at": datetime.now(UTC).isoformat(),
        "events": [],
    }
    msg = _make_message(payload)
    responses = [
        httpx.Response(503, text="down"),
        httpx.Response(503, text="down"),
        _success_response(),
    ]
    http = MagicMock()
    http.post = AsyncMock(side_effect=responses)
    consumer._http = http
    consumer._max_retries = 3

    await consumer._handle_message(msg)

    assert http.post.await_count == 3


async def test_forward_gives_up_after_max_retries(consumer):
    payload = {
        "station_code": "STATION_01",
        "started_at": datetime.now(UTC).isoformat(),
        "ended_at": datetime.now(UTC).isoformat(),
        "events": [],
    }
    msg = _make_message(payload)
    http = MagicMock()
    http.post = AsyncMock(return_value=httpx.Response(503, text="down"))
    consumer._http = http
    consumer._max_retries = 2

    await consumer._handle_message(msg)

    assert http.post.await_count == 2


async def test_forward_retries_on_http_error_then_succeeds(consumer):
    payload = {
        "station_code": "STATION_01",
        "started_at": datetime.now(UTC).isoformat(),
        "ended_at": datetime.now(UTC).isoformat(),
        "events": [],
    }
    msg = _make_message(payload)
    http = MagicMock()
    http.post = AsyncMock(
        side_effect=[
            httpx.ConnectError("connection refused"),
            _success_response(),
        ]
    )
    consumer._http = http
    consumer._max_retries = 3

    await consumer._handle_message(msg)

    assert http.post.await_count == 2


async def test_forward_gives_up_on_persistent_http_error(consumer):
    payload = {
        "station_code": "STATION_01",
        "started_at": datetime.now(UTC).isoformat(),
        "ended_at": datetime.now(UTC).isoformat(),
        "events": [],
    }
    msg = _make_message(payload)
    http = MagicMock()
    http.post = AsyncMock(side_effect=httpx.ConnectError("nope"))
    consumer._http = http
    consumer._max_retries = 2

    # The handler catches the final failure so the consumer keeps running.
    await consumer._handle_message(msg)

    assert http.post.await_count == 2


# ───────────────────────────────────────────────────────────────────────
# Stop semantics
# ───────────────────────────────────────────────────────────────────────


async def test_request_stop_sets_event(consumer):
    assert not consumer._stop_event.is_set()
    consumer.request_stop()
    assert consumer._stop_event.is_set()
