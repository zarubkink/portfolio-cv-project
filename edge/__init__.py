"""Edge device package — RTSP reader + ArUco + MQTT publisher.

This package is a standalone process intended to run on a
Raspberry Pi or similar farm-side computer. It reads the local
camera's RTSP stream, runs the cheap MOG2/ArUco pipeline and
publishes detection batches to the central MQTT broker.
"""

from __future__ import annotations

__all__: list[str] = []
