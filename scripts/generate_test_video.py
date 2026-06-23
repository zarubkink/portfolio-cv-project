"""Generate a synthetic MP4 containing one or more ArUco markers.

Useful for end-to-end testing of the video pipeline without needing a
real camera. The output is a moving background (random noise) so that
the MOG2 trigger fires, plus one or more ArUco markers rendered on top.

Examples
--------
Single marker, centred inside a default ROI::

    uv run python scripts/generate_test_video.py \\
        --aruco-id 1 --duration 5 --output /tmp/test_aruco.mp4

Multiple markers (multi-marker test)::

    uv run python scripts/generate_test_video.py \\
        --aruco-ids 1,11,12 --duration 6 --output /tmp/multi.mp4

Place the marker outside the default ROI to verify ROI filtering::

    uv run python scripts/generate_test_video.py \\
        --aruco-id 1 --duration 5 --output /tmp/out.mp4 --outside-roi
"""

from __future__ import annotations

import argparse
from pathlib import Path

import cv2
import numpy as np


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aruco-id", type=int, default=None, help="Single ArUco ID")
    parser.add_argument(
        "--aruco-ids",
        type=str,
        default=None,
        help="Comma-separated ArUco IDs (overrides --aruco-id)",
    )
    parser.add_argument("--aruco-dict", default="DICT_4X4_50")
    parser.add_argument("--duration", type=float, default=5.0, help="Seconds")
    parser.add_argument("--fps", type=int, default=10)
    parser.add_argument("--width", type=int, default=640)
    parser.add_argument("--height", type=int, default=480)
    parser.add_argument("--marker-size", type=int, default=120, help="Side in px")
    parser.add_argument(
        "--outside-roi",
        action="store_true",
        help="Place markers in the top-left corner (outside default ROI)",
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for background"
    )
    return parser.parse_args()


def resolve_ids(args: argparse.Namespace) -> list[int]:
    if args.aruco_ids:
        return [int(x) for x in args.aruco_ids.split(",") if x.strip()]
    if args.aruco_id is not None:
        return [args.aruco_id]
    return [1]


def make_marker(
    dictionary: cv2.aruco.Dictionary,
    aruco_id: int,
    size: int,
) -> np.ndarray:
    return cv2.aruco.generateImageMarker(dictionary, aruco_id, size)


def default_roi(width: int, height: int) -> tuple[int, int, int, int]:
    """ROI = central rectangle covering 50% of the frame."""
    x = width // 4
    y = height // 4
    w = width // 2
    h = height // 2
    return x, y, w, h


def compose_frame(
    bg: np.ndarray,
    markers: list[np.ndarray],
    frame_idx: int,
    fps: int,
    width: int,
    height: int,
    marker_size: int,
    inside_roi: bool,
) -> np.ndarray:
    """Add jitter to the background, then draw moving markers."""
    noise = np.random.randint(0, 40, bg.shape, dtype=np.uint8)
    frame = cv2.add(bg, noise)

    roi = default_roi(width, height)
    for i, marker in enumerate(markers):
        if inside_roi:
            slots_x = roi[0] + 20 + i * (marker_size + 20)
            slots_y = roi[1] + 20
        else:
            slots_x = 10
            slots_y = 10
        offset = int(20 * np.sin(2 * np.pi * frame_idx / fps))
        x = slots_x + offset
        y = slots_y + offset
        x = max(0, min(x, width - marker_size))
        y = max(0, min(y, height - marker_size))
        frame[y : y + marker_size, x : x + marker_size] = marker

    x0, y0, w0, h0 = roi
    cv2.rectangle(frame, (x0, y0), (x0 + w0, y0 + h0), (0, 255, 0), 2)
    return frame


def main() -> None:
    args = parse_args()
    ids = resolve_ids(args)
    rng = np.random.default_rng(args.seed)
    dictionary = cv2.aruco.getPredefinedDictionary(getattr(cv2.aruco, args.aruco_dict))
    markers = [
        cv2.cvtColor(make_marker(dictionary, i, args.marker_size), cv2.COLOR_GRAY2BGR)
        for i in ids
    ]
    bg = rng.integers(40, 90, size=(args.height, args.width, 3), dtype=np.uint8)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(
        str(args.output), fourcc, float(args.fps), (args.width, args.height)
    )
    if not writer.isOpened():
        raise RuntimeError(f"Cannot open VideoWriter for {args.output}")

    total_frames = int(args.duration * args.fps)
    for idx in range(total_frames):
        frame = compose_frame(
            bg=bg,
            markers=markers,
            frame_idx=idx,
            fps=args.fps,
            width=args.width,
            height=args.height,
            marker_size=args.marker_size,
            inside_roi=not args.outside_roi,
        )
        writer.write(frame)
    writer.release()
    print(
        f"Wrote {args.output} ({total_frames} frames @ {args.fps}fps, "
        f"ids={ids}, roi={'inside' if not args.outside_roi else 'outside'})"
    )


if __name__ == "__main__":
    main()
