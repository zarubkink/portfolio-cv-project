"""Edge process — RTSP → ArUco → MQTT.

Runs on the farm (Raspberry Pi or similar). Opens the camera RTSP
stream, runs the cheap MOG2 trigger and ArUco detector on each
frame, accumulates detections into a batch and publishes the
batch to MQTT when:

* ``EDGE_PUBLISH_INTERVAL_SEC`` has elapsed since the previous
  flush (default: 5s), or
* ``EDGE_PUBLISH_MAX_EVENTS`` events have piled up (default: 500).

The MQTT topic layout is ``farm/<station_code>/detections`` so the
server-side consumer can subscribe with ``farm/+/detections`` and
route by station. The batch payload matches
:class:`src.schemas.edge_ingest.EdgeBatchIn` 1-to-1; the consumer
forwards it to ``POST /v1/events/ingest``.

The detector runs synchronously in the main loop — cv2's Python
bindings release the GIL inside ``cv2.aruco``, so this is already
non-blocking from the asyncio perspective. The MQTT publish is
dispatched as ``asyncio.create_task`` so the detector loop is not
stalled by broker round-trips.

RTSP failures are not fatal: the loop sleeps
``EDGE_RECONNECT_DELAY`` seconds and re-opens the stream. SIGINT
and SIGTERM close the stream and disconnect cleanly.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import time
from collections import deque
from collections.abc import Iterable
from dataclasses import asdict, dataclass

import cv2
from loguru import logger

from edge.config import edge_settings
from src.config.logging import logging_settings
from src.logging_setup import configure_logging
from src.services.detector import ArucoDetector, RoiChecker, TriggerDetector
from src.services.mqtt_client import MqttClient


@dataclass(slots=True)
class Detection:
    """One ArUco detection, with frame context for the consumer."""

    aruco_id: int
    confidence: float
    bbox: dict
    inside_roi: bool
    frame_number: int
    timestamp_in_video: float
    detector_metadata: dict


class EdgePipeline:
    """Single-camera RTSP → batched MQTT pipeline."""

    def __init__(
        self,
        *,
        rtsp_url: str,
        station_code: str,
        fps_target: float,
        frame_skip: int,
        reconnect_delay: float,
        publish_interval_sec: float,
        publish_max_events: int,
        roi_polygon: list[list[int]] | None,
        topic_prefix: str,
    ) -> None:
        self._rtsp_url = rtsp_url
        self._station_code = station_code
        self._fps_target = fps_target
        self._frame_skip = max(1, frame_skip)
        self._reconnect_delay = reconnect_delay
        self._publish_interval_sec = publish_interval_sec
        self._publish_max_events = publish_max_events
        self._roi_polygon = roi_polygon
        self._topic = f"{topic_prefix}/{station_code}/detections"
        self._buffer: deque[Detection] = deque()
        self._buffer_lock = asyncio.Lock()
        self._last_publish_ts = time.monotonic()
        self._frame_number = 0
        self._detector_started_at: float | None = None
        self._stop_event = asyncio.Event()
        # Detector instances live for the lifetime of the process; the
        # MOG2 background model learns from the stream so re-creating
        # it per frame would defeat the purpose.
        self._trigger = TriggerDetector()
        self._aruco = ArucoDetector()
        self._roi = RoiChecker(roi_polygon)

    # ─────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────

    def request_stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        """Main loop. Never raises — exits cleanly on stop_event."""
        logger.info(
            f"Edge pipeline starting: station={self._station_code}, "
            f"rtsp={self._rtsp_url}, topic={self._topic}"
        )
        async with MqttClient() as mqtt:
            self._mqtt = mqtt
            timer_task = asyncio.create_task(self._flush_timer())
            try:
                while not self._stop_event.is_set():
                    try:
                        await self._consume_stream()
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.opt(exception=exc).error(
                            "stream consumer crashed; reconnecting"
                        )
                        await self._maybe_sleep(self._reconnect_delay)
            finally:
                timer_task.cancel()
                try:
                    await timer_task
                except asyncio.CancelledError:
                    pass
                await self._flush(final=True)
        logger.info("Edge pipeline stopped")

    # ─────────────────────────────────────────────────────────────────
    # RTSP consumer
    # ─────────────────────────────────────────────────────────────────

    async def _consume_stream(self) -> None:
        """Block on cv2.VideoCapture.read() in a thread so the loop
        stays responsive to ``stop_event``."""
        cap = cv2.VideoCapture(self._rtsp_url)
        if not cap.isOpened():
            logger.warning(
                f"cannot open RTSP stream: {self._rtsp_url}; "
                f"retrying in {self._reconnect_delay}s"
            )
            cap.release()
            await self._maybe_sleep(self._reconnect_delay)
            return

        logger.info(f"RTSP stream opened: {self._rtsp_url}")
        self._frame_number = 0
        self._detector_started_at = time.monotonic()
        min_frame_dt = 1.0 / max(self._fps_target, 0.1)
        try:
            while not self._stop_event.is_set():
                loop_start = time.monotonic()
                ok, frame = await asyncio.to_thread(cap.read)
                if not ok or frame is None:
                    logger.warning("RTSP stream ended; reopening")
                    break
                if self._frame_number % self._frame_skip == 0:
                    timestamp = loop_start - self._detector_started_at
                    self._process_frame(frame, timestamp)
                self._frame_number += 1
                # Pace the loop to fps_target so we don't burn CPU
                # on a 60 fps camera when we only need 10.
                elapsed = time.monotonic() - loop_start
                remaining = min_frame_dt - elapsed
                if remaining > 0:
                    await self._maybe_sleep(remaining)
        finally:
            cap.release()

    def _process_frame(self, frame, timestamp: float) -> None:
        """Run the trigger + aruco detectors and append events."""
        trigger_result = self._trigger.detect(frame)
        if not trigger_result.triggered:
            return
        for det in self._aruco.detect(frame):
            bbox_dict: dict | None = None
            if det.bbox is not None:
                x, y, w, h = det.bbox
                bbox_dict = {"x": x, "y": y, "w": w, "h": h}
            self._buffer.append(
                Detection(
                    aruco_id=det.aruco_id,
                    confidence=det.confidence,
                    bbox=bbox_dict,
                    inside_roi=self._roi.is_inside(det.bbox),
                    frame_number=self._frame_number,
                    timestamp_in_video=timestamp,
                    detector_metadata={"triggered_by_mog2": True},
                )
            )

    # ─────────────────────────────────────────────────────────────────
    # Batching / publish
    # ─────────────────────────────────────────────────────────────────

    async def _flush_timer(self) -> None:
        """Wake up every publish_interval_sec and flush."""
        while not self._stop_event.is_set():
            await self._maybe_sleep(self._publish_interval_sec)
            await self._flush(final=False)

    async def _flush(self, *, final: bool) -> None:
        """Drain the buffer and publish one batch."""
        async with self._buffer_lock:
            if not self._buffer and not final:
                return
            batch = list(self._buffer)
            self._buffer.clear()
        if not batch and not final:
            return

        now = time.monotonic()
        started_at = (
            self._detector_started_at if self._detector_started_at is not None else now
        )
        payload = {
            "station_code": self._station_code,
            "started_at": started_at,
            "ended_at": now,
            "events": [asdict(d) for d in batch],
        }
        if not batch:
            # Heartbeat: zero events keeps the sentinel alive on the
            # server side and confirms the edge is reachable.
            payload["events"] = []
        try:
            await self._mqtt.publish(self._topic, payload)
            logger.info(
                f"published {len(batch)} events to {self._topic} "
                f"({'final' if final else 'interval'})"
            )
            self._last_publish_ts = now
        except Exception as exc:  # pragma: no cover - defensive
            logger.opt(exception=exc).error(
                f"publish failed; dropping {len(batch)} events"
            )

    async def _maybe_sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except TimeoutError:
            pass


def _settings_from_args(args: argparse.Namespace) -> dict:
    """Apply CLI overrides on top of ``edge_settings``."""
    overrides: dict = {}
    for name in (
        "rtsp_url",
        "station_code",
        "fps_target",
        "frame_skip",
        "reconnect_delay",
        "publish_interval_sec",
        "publish_max_events",
    ):
        value = getattr(args, name, None)
        if value is not None:
            overrides[name] = value
    return overrides


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RTSP -> ArUco -> MQTT edge pipeline")
    parser.add_argument("--rtsp-url", default=None)
    parser.add_argument("--station-code", default=None)
    parser.add_argument("--fps-target", type=float, default=None)
    parser.add_argument("--frame-skip", type=int, default=None)
    parser.add_argument("--reconnect-delay", type=float, default=None)
    parser.add_argument("--publish-interval-sec", type=float, default=None)
    parser.add_argument("--publish-max-events", type=int, default=None)
    return parser.parse_args(argv)


async def main(argv: Iterable[str] | None = None) -> None:
    configure_logging(logging_settings, filename="edge.log")
    args = parse_args(argv)
    overrides = _settings_from_args(args)
    pipeline = EdgePipeline(
        rtsp_url=overrides.get("rtsp_url", edge_settings.rtsp_url),
        station_code=overrides.get("station_code", edge_settings.station_code),
        fps_target=overrides.get("fps_target", edge_settings.fps_target),
        frame_skip=overrides.get("frame_skip", edge_settings.frame_skip),
        reconnect_delay=overrides.get("reconnect_delay", edge_settings.reconnect_delay),
        publish_interval_sec=overrides.get(
            "publish_interval_sec", edge_settings.publish_interval_sec
        ),
        publish_max_events=overrides.get(
            "publish_max_events", edge_settings.publish_max_events
        ),
        roi_polygon=edge_settings.roi_polygon,
        topic_prefix=edge_settings.topic_prefix,
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, pipeline.request_stop)
        except NotImplementedError:  # pragma: no cover - Windows
            pass

    await pipeline.run()


if __name__ == "__main__":
    asyncio.run(main())
