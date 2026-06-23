"""Frame-level detectors used by the video processor pipeline.

Four small composable classes:

* :class:`TriggerDetector` — MOG2 background subtraction; reports "is there
  *any* motion in this frame?" together with the area of the largest blob.
* :class:`ArucoDetector` — OpenCV ``cv2.aruco`` wrapper; decodes marker
  IDs, drops detections that are too small to trust.
* :class:`RoiChecker` — point-in-polygon test using
  ``cv2.pointPolygonTest`` so a station's polygon gate is enforced on
  every bbox.
* :class:`ParkedDetector` — composite "parked vs passing-by" decision
  combining ROI containment, frame-to-frame velocity, and MOG2 mass
  inside the ROI.

All detectors are intentionally *stateless across instances* (apart from
:class:`TriggerDetector`'s background model and :class:`ParkedDetector`'s
last centre), so they can be created fresh inside a worker process.
"""

from __future__ import annotations

import math

import cv2
import numpy as np

from src.config.detector import detector_settings
from src.schemas.detector import (
    ArucoDetection,
    ParkedDecision,
    TriggerResult,
)
from src.schemas.event import DetectorMethod


class TriggerDetector:
    """MOG2-based motion trigger.

    Returns the largest foreground contour area and its bounding box.
    A frame is considered "triggered" when the area exceeds
    ``detector_settings.min_contour_area``.
    """

    def __init__(self) -> None:
        self.bg = cv2.createBackgroundSubtractorMOG2(
            history=detector_settings.mog2_history,
            varThreshold=detector_settings.mog2_var_threshold,
            detectShadows=True,
        )
        self.min_area = detector_settings.min_contour_area

    def detect(self, frame: np.ndarray) -> TriggerResult:
        mask = self.bg.apply(frame)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        max_area = 0.0
        max_bbox: tuple[int, int, int, int] | None = None
        for c in contours:
            area = cv2.contourArea(c)
            if area > max_area:
                max_area = area
                max_bbox = cv2.boundingRect(c)
        triggered = max_area > self.min_area
        return TriggerResult(
            triggered=triggered,
            contour_area=float(max_area),
            bbox=max_bbox if triggered else None,
        )


class ArucoDetector:
    """OpenCV ArUco marker decoder.

    Filters out markers whose side is smaller than
    ``detector_settings.min_aruco_side_pixels`` to avoid false positives
    on background noise.
    """

    def __init__(self) -> None:
        dictionary = cv2.aruco.getPredefinedDictionary(
            getattr(cv2.aruco, detector_settings.aruco_dict)
        )
        params = cv2.aruco.DetectorParameters()
        params.errorCorrectionRate = 0.6
        self.detector = cv2.aruco.ArucoDetector(dictionary, params)

    def detect(self, frame: np.ndarray) -> list[ArucoDetection]:
        corners, ids, _rejected = self.detector.detectMarkers(frame)
        out: list[ArucoDetection] = []
        if ids is None:
            return out
        h, w = frame.shape[:2]
        frame_area = h * w
        for corner, aruco_id in zip(corners, ids.flatten().tolist(), strict=False):
            x, y, cw, ch = cv2.boundingRect(corner)
            if cw < detector_settings.min_aruco_side_pixels:
                continue
            confidence = min((cw * ch) / frame_area * 10, 1.0)
            out.append(
                ArucoDetection(
                    aruco_id=int(aruco_id),
                    tractor_id=None,
                    confidence=float(confidence),
                    bbox=(x, y, cw, ch),
                    detector_method=DetectorMethod.ARUCO,
                )
            )
        return out


class RoiChecker:
    """Point-in-polygon gate for a station's region-of-interest.

    ``roi_polygon`` is a list of ``[x, y]`` pairs in frame coordinates;
    when ``None`` the entire frame is considered inside the ROI (useful
    for stations without manual ground-truth annotation).
    """

    def __init__(self, roi_polygon: list[list[int]] | None) -> None:
        self.polygon: np.ndarray | None = (
            np.array(roi_polygon, dtype=np.int32) if roi_polygon else None
        )

    def is_inside(self, bbox: tuple[int, int, int, int] | None) -> bool:
        if bbox is None:
            return False
        if self.polygon is None:
            return True
        cx = bbox[0] + bbox[2] // 2
        cy = bbox[1] + bbox[3] // 2
        return cv2.pointPolygonTest(self.polygon, (float(cx), float(cy)), False) >= 0


class ParkedDetector:
    """Composite "parked vs passing-by" filter.

    Decision rule (see PROJECT_SCAFFOLD_PROMPT §3.7):

        is_parked = inside_roi AND is_stopped AND mog2_mass_in_roi > min
    """

    def __init__(self, roi_checker: RoiChecker) -> None:
        self.roi = roi_checker
        self.prev_center: tuple[float, float] | None = None

    def decide(
        self,
        detection: ArucoDetection,
        trigger_result: TriggerResult,
    ) -> ParkedDecision:
        inside_roi = self.roi.is_inside(detection.bbox)
        velocity_px = 0.0
        if detection.bbox is not None:
            cx = detection.bbox[0] + detection.bbox[2] / 2
            cy = detection.bbox[1] + detection.bbox[3] / 2
            if self.prev_center is not None:
                velocity_px = math.dist(self.prev_center, (cx, cy))
            self.prev_center = (cx, cy)
        is_stopped = velocity_px < detector_settings.velocity_px_threshold
        mog2_mass_in_roi = trigger_result.contour_area if inside_roi else 0.0
        is_parked = (
            inside_roi
            and is_stopped
            and mog2_mass_in_roi > detector_settings.mog2_mass_min
        )
        return ParkedDecision(
            is_parked=is_parked,
            inside_roi=inside_roi,
            velocity_px_per_frame=velocity_px,
            mog2_mass_in_roi=mog2_mass_in_roi,
            metadata={
                "triggered_by_mog2": trigger_result.triggered,
            },
        )

    def reset(self) -> None:
        self.prev_center = None
