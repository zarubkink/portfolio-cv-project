"""Video processing pipeline.

The pipeline is intentionally synchronous and side-effect-free on the
database — it takes a video path and a few factories, runs the
trigger → ArUco → ROI → parked decision loop, and returns the
resulting :class:`DetectionEvent` list together with counters. Stage 6
wraps this function with :class:`ProcessPoolExecutor` and writes the
events to PostgreSQL.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass

import cv2
import numpy as np

from src.config.video import video_settings
from src.services.detector import (
    ArucoDetector,
    ParkedDetector,
    RoiChecker,
    TriggerDetector,
)


def extract_frames(
    video_path: str,
) -> Iterator[tuple[int, float, np.ndarray]]:
    """Yield ``(frame_index, timestamp_seconds, frame_bgr)``.

    Frames wider than ``video_settings.target_width`` are downscaled to
    keep the trigger cheap.
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {video_path}")
    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)) or 0
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)) or 0
    target_w = video_settings.target_width
    scale = (target_w / width) if width > target_w else 1.0

    frame_index = 0
    try:
        while True:
            ok, frame = cap.read()
            if not ok:
                break
            timestamp = frame_index / fps
            if scale != 1.0:
                frame = cv2.resize(frame, (target_w, int(height * scale)))
            yield frame_index, timestamp, frame
            frame_index += 1
    finally:
        cap.release()


@dataclass
class DetectionEvent:
    """One observation of a marker, enriched with ROI / parked decision."""

    frame_number: int
    timestamp_in_video: float
    aruco_id: int | None
    confidence: float
    bbox: tuple[int, int, int, int] | None
    inside_roi: bool
    detector_metadata: dict


@dataclass
class ProcessingStats:
    frames_processed: int = 0
    triggers_fired: int = 0
    events_emitted: int = 0


def process_video(
    video_path: str,
    trigger_factory: Callable[[], TriggerDetector] = TriggerDetector,
    aruco_factory: Callable[[], ArucoDetector] = ArucoDetector,
    roi_polygon: list[list[int]] | None = None,
) -> tuple[list[DetectionEvent], int, int]:
    """Run the full pipeline against ``video_path``.

    Returns ``(events, frames_processed, triggers_fired)``.
    """
    trigger = trigger_factory()
    aruco = aruco_factory()
    roi = RoiChecker(roi_polygon)
    parked = ParkedDetector(roi)

    events: list[DetectionEvent] = []
    frames_processed = 0
    triggers_fired = 0

    for frame_number, timestamp, frame in extract_frames(video_path):
        frames_processed += 1
        trigger_result = trigger.detect(frame)
        if not trigger_result.triggered:
            continue
        triggers_fired += 1
        for detection in aruco.detect(frame):
            decision = parked.decide(detection, trigger_result)
            events.append(
                DetectionEvent(
                    frame_number=frame_number,
                    timestamp_in_video=timestamp,
                    aruco_id=detection.aruco_id,
                    confidence=detection.confidence,
                    bbox=detection.bbox,
                    inside_roi=decision.inside_roi,
                    detector_metadata=decision.metadata,
                )
            )

    return events, frames_processed, triggers_fired


__all__ = [
    "DetectionEvent",
    "ProcessingStats",
    "extract_frames",
    "process_video",
]
