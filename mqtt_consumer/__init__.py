"""Server-side MQTT consumer package.

Subscribes to ``farm/+/detections`` on the broker and forwards each
batch to the FastAPI ``/v1/events/ingest`` endpoint.
"""

from __future__ import annotations

__all__: list[str] = []
