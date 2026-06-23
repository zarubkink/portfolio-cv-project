"""Internal dataclasses used by the video processing pipeline.

These types live in :mod:`src.schemas.detector` rather than
:mod:`src.services.detector` because they are the *data contract* between
the trigger (MOG2), the marker decoder (ArUco), the ROI filter, and the
parked-or-transit decision. They are deliberately tiny and immutable
(NamedTuple) so they can flow through :class:`ProcessPoolExecutor` queues
without copy issues.
"""

from __future__ import annotations

from typing import NamedTuple

from src.schemas.event import DetectorMethod


class TriggerResult(NamedTuple):
    """Output of :class:`src.services.detector.TriggerDetector` per frame."""

    triggered: bool
    contour_area: float
    bbox: tuple[int, int, int, int] | None


class ArucoDetection(NamedTuple):
    """A single ArUco marker observed in a frame.

    ``tractor_id`` is ``None`` at decode time and is filled in by a
    post-processing lookup against the ``tractors`` table.
    """

    aruco_id: int | None
    tractor_id: int | None
    confidence: float
    bbox: tuple[int, int, int, int] | None
    detector_method: DetectorMethod = DetectorMethod.ARUCO


class ParkedDecision(NamedTuple):
    """Composite signal for "tractor entered the ROI and stopped"."""

    is_parked: bool
    inside_roi: bool
    velocity_px_per_frame: float
    mog2_mass_in_roi: float
    metadata: dict
