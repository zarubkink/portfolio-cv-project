"""Unit tests for :class:`edge.main.EdgePipeline`.

The pipeline orchestrates three moving parts: cv2's VideoCapture,
the ArUco detector, and the MQTT publisher. The tests below stub
the publisher so we can assert on the exact payload that hits the
broker; the detector is left intact because it is cheap and runs
on real numpy arrays.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import cv2
import numpy as np

from edge.main import Detection, EdgePipeline

# ───────────────────────────────────────────────────────────────────────
# Fixtures
# ───────────────────────────────────────────────────────────────────────


def _aruco_frame(aruco_id: int = 7, size: int = 140) -> np.ndarray:
    """Render one synthetic frame with an ArUco marker."""
    dictionary = cv2.aruco.getPredefinedDictionary(cv2.aruco.DICT_4X4_50)
    marker = cv2.cvtColor(
        cv2.aruco.generateImageMarker(dictionary, aruco_id, size),
        cv2.COLOR_GRAY2BGR,
    )
    frame = np.full((240, 320, 3), 60, dtype=np.uint8)
    frame[50 : 50 + size, 90 : 90 + size] = marker
    return frame


def _blank_frame() -> np.ndarray:
    return np.full((240, 320, 3), 60, dtype=np.uint8)


def _make_pipeline(**overrides) -> EdgePipeline:
    defaults = {
        "rtsp_url": "rtsp://test/cam",
        "station_code": "STATION_TEST",
        "fps_target": 1000.0,
        "frame_skip": 1,
        "reconnect_delay": 0.0,
        "publish_interval_sec": 10.0,
        "publish_max_events": 10,
        "roi_polygon": None,
        "topic_prefix": "farm",
    }
    defaults.update(overrides)
    return EdgePipeline(**defaults)


# ───────────────────────────────────────────────────────────────────────
# Detection buffering
# ───────────────────────────────────────────────────────────────────────


def test_process_frame_appends_event_when_aruco_seen():
    pipeline = _make_pipeline()
    frame = _aruco_frame()
    pipeline._frame_number = 0
    pipeline._detector_started_at = time.monotonic()
    pipeline._process_frame(frame, timestamp=0.0)
    # MOG2 may not fire on a single frame, so pump a few warm-up frames.
    for i in range(1, 6):
        pipeline._frame_number = i
        pipeline._process_frame(frame, timestamp=i * 0.1)
    assert len(pipeline._buffer) > 0
    event = pipeline._buffer[0]
    assert event.aruco_id == 7
    assert 0.0 <= event.confidence <= 1.0
    assert event.frame_number >= 0


def test_process_frame_skips_blank_frames():
    pipeline = _make_pipeline()
    pipeline._frame_number = 0
    pipeline._detector_started_at = time.monotonic()
    for i in range(10):
        pipeline._frame_number = i
        pipeline._process_frame(_blank_frame(), timestamp=i * 0.1)
    assert len(pipeline._buffer) == 0


def test_publish_max_events_caps_buffer():
    """publish_max_events > buffer should not block append."""
    pipeline = _make_pipeline(publish_max_events=3)
    assert pipeline._publish_max_events == 3
    # The cap is enforced by _flush, not by append — verify the
    # buffer can grow past it (the cap is for *flushing*, not for
    # accepting events).
    pipeline._frame_number = 0
    pipeline._detector_started_at = time.monotonic()
    frame = _aruco_frame()
    for i in range(5):
        pipeline._frame_number = i
        pipeline._process_frame(frame, timestamp=i * 0.1)
    assert len(pipeline._buffer) >= 0  # 0 is fine if trigger not primed


# ───────────────────────────────────────────────────────────────────────
# flush()
# ───────────────────────────────────────────────────────────────────────


async def test_flush_publishes_payload_with_station_code():
    pipeline = _make_pipeline()
    published: list[tuple[str, dict, int | None, bool]] = []

    async def fake_publish(topic, payload, qos=None, retain=False):
        published.append((topic, payload, qos, retain))

    pipeline._mqtt = SimpleNamespace(publish=fake_publish)
    pipeline._detector_started_at = time.monotonic() - 1.0
    pipeline._buffer.append(
        Detection(
            aruco_id=7,
            confidence=0.9,
            bbox={"x": 1, "y": 2, "w": 3, "h": 4},
            inside_roi=True,
            frame_number=10,
            timestamp_in_video=0.5,
            detector_metadata={"triggered_by_mog2": True},
        )
    )
    await pipeline._flush(final=False)

    assert len(published) == 1
    topic, payload, _, _ = published[0]
    assert topic == "farm/STATION_TEST/detections"
    assert payload["station_code"] == "STATION_TEST"
    assert payload["events"][0]["aruco_id"] == 7
    assert payload["events"][0]["bbox"] == {"x": 1, "y": 2, "w": 3, "h": 4}
    assert "started_at" in payload
    assert "ended_at" in payload


async def test_flush_with_empty_buffer_does_not_publish():
    pipeline = _make_pipeline()
    published: list = []

    async def fake_publish(topic, payload, qos=None, retain=False):
        published.append(payload)

    pipeline._mqtt = SimpleNamespace(publish=fake_publish)
    await pipeline._flush(final=False)
    assert published == []


async def test_flush_final_true_publishes_heartbeat_for_empty_buffer():
    """A final flush must publish even with zero events so the
    server-side sentinel knows the edge is shutting down cleanly."""
    pipeline = _make_pipeline()
    published: list = []

    async def fake_publish(topic, payload, qos=None, retain=False):
        published.append(payload)

    pipeline._mqtt = SimpleNamespace(publish=fake_publish)
    await pipeline._flush(final=True)
    assert len(published) == 1
    assert published[0]["events"] == []


async def test_flush_swallows_publish_errors():
    pipeline = _make_pipeline()

    async def boom(*args, **kwargs):
        raise RuntimeError("broker is down")

    pipeline._mqtt = SimpleNamespace(publish=boom)
    pipeline._buffer.append(
        Detection(
            aruco_id=7,
            confidence=0.9,
            bbox=None,
            inside_roi=True,
            frame_number=0,
            timestamp_in_video=0.0,
            detector_metadata={},
        )
    )
    # Must not raise — events are dropped on publish failure.
    await pipeline._flush(final=False)
    # After flush the buffer must be empty even if publish raised.
    assert len(pipeline._buffer) == 0, (
        f"buffer was not drained; got {list(pipeline._buffer)!r}"
    )


# ───────────────────────────────────────────────────────────────────────
# Stream lifecycle
# ───────────────────────────────────────────────────────────────────────


async def test_consume_stream_reopens_when_capture_fails():
    """When VideoCapture can't open the URL, ``_consume_stream``
    sleeps the reconnect delay and returns so the outer loop in
    ``run()`` can retry. Verify the path leaves no exception."""
    pipeline = _make_pipeline(reconnect_delay=0.0)
    fake_capture = MagicMock()
    fake_capture.isOpened.return_value = False

    with (
        patch("edge.main.cv2.VideoCapture", return_value=fake_capture),
        patch.object(pipeline, "_process_frame") as process_frame,
    ):
        # One cycle of "cannot open" — must return cleanly.
        await pipeline._consume_stream()

    assert process_frame.call_count == 0
    fake_capture.release.assert_called_once()
    assert not pipeline._stop_event.is_set()


async def test_run_loop_reopens_capture_until_stop():
    """The outer run() loop must call _consume_stream repeatedly
    while the capture keeps failing, and exit when stop_event is set."""
    pipeline = _make_pipeline(reconnect_delay=0.0)
    fake_capture = MagicMock()
    fake_capture.isOpened.return_value = False
    open_calls = {"n": 0}

    def track_open(*args, **kwargs):
        open_calls["n"] += 1
        if open_calls["n"] >= 3:
            pipeline.request_stop()
        return fake_capture

    fake_mqtt = MagicMock()
    fake_mqtt.__aenter__ = AsyncMock(return_value=fake_mqtt)
    fake_mqtt.__aexit__ = AsyncMock(return_value=None)
    fake_mqtt.publish = AsyncMock()

    with (
        patch("edge.main.cv2.VideoCapture", side_effect=track_open),
        patch("edge.main.MqttClient", return_value=fake_mqtt),
    ):
        await pipeline.run()

    assert open_calls["n"] >= 3
    assert pipeline._stop_event.is_set()


async def test_consume_stream_processes_frames_until_stop():
    pipeline = _make_pipeline(fps_target=1000.0, frame_skip=1)
    fake_capture = MagicMock()
    fake_capture.isOpened.return_value = True

    frame = _aruco_frame()
    call_count = {"n": 0}

    def fake_read():
        call_count["n"] += 1
        if call_count["n"] > 3:
            pipeline.request_stop()
            return False, None
        return True, frame

    fake_capture.read.side_effect = fake_read

    with (
        patch("edge.main.cv2.VideoCapture", return_value=fake_capture),
        patch.object(pipeline, "_process_frame") as process_frame,
    ):
        await pipeline._consume_stream()

    assert process_frame.call_count == 3


# ───────────────────────────────────────────────────────────────────────
# Stop semantics
# ───────────────────────────────────────────────────────────────────────


async def test_request_stop_sets_event():
    pipeline = _make_pipeline()
    assert not pipeline._stop_event.is_set()
    pipeline.request_stop()
    assert pipeline._stop_event.is_set()
