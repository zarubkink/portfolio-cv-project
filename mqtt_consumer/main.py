"""MQTT consumer process — broker -> FastAPI ``/v1/events/ingest``.

Subscribes to ``farm/+/detections`` (the topic the edge process
publishes to), parses each JSON payload into
:class:`src.schemas.edge_ingest.EdgeBatchIn` and forwards it to
the api over HTTP. The api owns the database write so the
consumer stays stateless.

Failure handling:

* HTTP errors from the api (5xx, network timeout) trigger
  exponential back-off and retry up to ``CONSUMER_MAX_RETRIES``.
  After that the batch is logged and dropped — a stuck consumer
  is worse than a missing batch.
* 4xx errors (validation, unknown station) are not retried; they
  indicate a programming error or a stale station registry.
* The MQTT loop is wrapped by :meth:`MqttClient.run_forever` so a
  broker outage reconnects automatically with backoff.

The consumer is meant to run as a side-car in ``compose.yaml``;
it never talks to the database directly.
"""

from __future__ import annotations

import asyncio
import json
import signal
import socket
from datetime import UTC, datetime

import httpx
from loguru import logger
from pydantic import ValidationError

from mqtt_consumer.config import consumer_settings
from src.config.logging import logging_settings
from src.config.mqtt import mqtt_settings
from src.logging_setup import configure_logging
from src.schemas.edge_ingest import EdgeBatchIn
from src.services.mqtt_client import MqttClient, MqttMessage


class MqttConsumer:
    """Long-lived broker subscriber that forwards each batch via HTTP."""

    def __init__(
        self,
        *,
        api_url: str | None = None,
        ingest_path: str | None = None,
        request_timeout_sec: float | None = None,
        max_retries: int | None = None,
        retry_backoff_sec: float | None = None,
    ) -> None:
        self._api_base = api_url or consumer_settings.api_url
        self._ingest_path = ingest_path or consumer_settings.ingest_path
        self._timeout = (
            request_timeout_sec
            if request_timeout_sec is not None
            else consumer_settings.request_timeout_sec
        )
        self._max_retries = (
            max_retries if max_retries is not None else consumer_settings.max_retries
        )
        self._retry_backoff = (
            retry_backoff_sec
            if retry_backoff_sec is not None
            else consumer_settings.retry_backoff_sec
        )
        self._stop_event = asyncio.Event()
        self._ingest_url = f"{self._api_base.rstrip('/')}{self._ingest_path}"

    # ─────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────

    def request_stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        logger.info(
            f"MQTT consumer starting: broker={mqtt_settings.broker_url}, "
            f"topic={mqtt_settings.detection_topic}, api={self._ingest_url}"
        )
        async with httpx.AsyncClient(timeout=self._timeout) as http:
            self._http = http
            mqtt = MqttClient(
                client_id=f"{consumer_settings.client_id_prefix}-{socket.gethostname()}",
            )
            await mqtt.run_forever(
                mqtt_settings.detection_topic,
                self._handle_message,
                stop_event=self._stop_event,
            )
        logger.info("MQTT consumer stopped")

    # ─────────────────────────────────────────────────────────────────
    # Message handling
    # ─────────────────────────────────────────────────────────────────

    async def _handle_message(self, msg: MqttMessage) -> None:
        """Validate the payload, POST it to the api, retry on 5xx."""
        try:
            payload_dict = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.opt(exception=exc).error(
                f"dropping malformed payload on {msg.topic}"
            )
            return

        # Edge batches carry the started_at / ended_at as floats
        # (seconds since the edge process started). The Pydantic
        # schema expects ISO-8601 datetimes; convert here so the
        # contract stays clean.
        try:
            payload_dict = self._normalise_timestamps(payload_dict)
            payload = EdgeBatchIn.model_validate(payload_dict)
        except ValidationError as exc:
            logger.error(
                f"dropping payload that fails schema validation on {msg.topic}: {exc}"
            )
            return
        except (TypeError, ValueError) as exc:
            logger.opt(exception=exc).error(
                f"timestamp conversion failed for {msg.topic}"
            )
            return

        try:
            await self._forward_with_retries(payload)
        except Exception as exc:  # pragma: no cover - defensive
            logger.opt(exception=exc).error(
                f"forwarding failed permanently for {msg.topic}"
            )

    @staticmethod
    def _normalise_timestamps(payload: dict) -> dict:
        """Convert ``started_at`` / ``ended_at`` from float seconds to
        ISO-8601 datetimes when the edge process sent them that way.

        The edge writes ``time.monotonic()`` deltas because that's
        what the RTSP loop naturally has; the api expects naive
        datetime strings. We sniff the type to keep the contract
        permissive — if the upstream already sends ISO strings we
        pass them through untouched.
        """
        out = dict(payload)
        for key in ("started_at", "ended_at"):
            if key in out and isinstance(out[key], (int, float)):
                # monotonic is seconds-since-startup, but we have
                # nothing to anchor it to; treat it as a wall-clock
                # anchor = the consumer's current time. This loses
                # the monotonic guarantee but keeps the field shape
                # compatible with the api.
                out[key] = datetime.now(UTC).isoformat()
        return out

    async def _forward_with_retries(self, payload: EdgeBatchIn) -> None:
        delay = self._retry_backoff
        for attempt in range(1, self._max_retries + 1):
            try:
                response = await self._http.post(
                    self._ingest_url,
                    json=payload.model_dump(mode="json"),
                )
            except httpx.HTTPError as exc:
                logger.warning(
                    f"HTTP error forwarding {payload.station_code} "
                    f"(attempt {attempt}/{self._max_retries}): {exc}"
                )
                if attempt == self._max_retries:
                    raise
                await self._maybe_sleep(delay)
                delay *= 2
                continue

            if 200 <= response.status_code < 300:
                logger.info(
                    f"forwarded {len(payload.events)} events from "
                    f"{payload.station_code} -> {self._ingest_url} "
                    f"({response.status_code})"
                )
                return

            # 4xx is a programming error or stale station; do not retry.
            if 400 <= response.status_code < 500:
                logger.error(
                    f"permanent failure for {payload.station_code} "
                    f"({response.status_code}): {response.text}"
                )
                return

            # 5xx — server-side issue, retry.
            logger.warning(
                f"server error {response.status_code} for "
                f"{payload.station_code} (attempt {attempt}/"
                f"{self._max_retries}): {response.text}"
            )
            if attempt == self._max_retries:
                logger.error(
                    f"giving up on {payload.station_code} after "
                    f"{self._max_retries} attempts"
                )
                return
            await self._maybe_sleep(delay)
            delay *= 2

    async def _maybe_sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except TimeoutError:
            pass


async def main() -> None:
    configure_logging(logging_settings, filename="mqtt_consumer.log")
    consumer = MqttConsumer()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, consumer.request_stop)
        except NotImplementedError:  # pragma: no cover - Windows
            pass
    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
