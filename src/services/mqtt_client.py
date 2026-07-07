"""Asyncio-native MQTT client wrapper.

Why :mod:`aiomqtt` and not raw ``paho-mqtt``? ``paho-mqtt`` exposes a
callback-driven API; wrapping it in asyncio requires running the loop
in a background thread. ``aiomqtt`` is a thin layer over paho that
exposes ``async with Client(...) as client:`` and ``async for message
in client.messages:`` — the asyncio integration we need for a FastAPI
side-car and for the long-lived edge process.

The wrapper below handles three jobs:

* **Lifecycle.** :class:`MqttClient` is an async context manager so
  callers never have to think about ``connect()`` / ``disconnect()``
  pairs or the underlying socket.
* **Topic parsing.** ``aiomqtt``'s wildcard subscription returns
  :class:`aiomqtt.Message` objects whose ``topic`` attribute carries
  the concrete station code (e.g. ``farm/STATION_01/detections``).
  :meth:`MqttMessage.station_code` extracts it for the consumer.
* **Reconnect with backoff.** :meth:`MqttClient.run_forever` swallows
  transient :class:`aiomqtt.MqttError` exceptions and reconnects with
  exponential backoff so a flaky link on the farm doesn't kill the
  consumer process. The bounded back-off never sleeps longer than
  :attr:`MqttSettings.reconnect_max_delay`.

The wrapper is deliberately broker-agnostic — no project-specific
payload schema is enforced here. Both the edge process and the
consumer deal with JSON payloads and validate against Pydantic
schemas closer to the boundary.
"""

from __future__ import annotations

import asyncio
import json
import ssl
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import aiomqtt
from loguru import logger

from src.config.mqtt import mqtt_settings

logger = logger.bind(component="mqtt")


def _parse_url(url: str) -> tuple[str, int, bool]:
    """Return ``(hostname, port, tls)`` from a ``mqtt://`` URL.

    Accepts ``mqtt://``, ``tcp://`` and ``mqtts://`` schemes. Anything
    else falls back to ``localhost:1883`` and logs a warning so a
    typo in ``MQTT_BROKER_URL`` is visible at startup rather than as
    a stack trace from the broker.
    """
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme not in {"mqtt", "tcp", "mqtts"}:
        logger.warning(
            f"unrecognised MQTT scheme {scheme!r}; falling back to localhost:1883"
        )
        return "localhost", 1883, False
    tls = scheme == "mqtts"
    port = parsed.port or (8883 if tls else 1883)
    hostname = parsed.hostname or "localhost"
    return hostname, port, tls


@dataclass(frozen=True, slots=True)
class MqttMessage:
    """A single delivery from the broker.

    ``topic`` is the concrete topic the message arrived on (e.g.
    ``farm/STATION_01/detections``). ``payload`` is the raw bytes
    payload — the caller is responsible for JSON decoding. The
    helper :meth:`station_code` parses the station code out of the
    topic when the topic follows the ``farm/<code>/detections``
    layout.
    """

    topic: str
    payload: bytes

    def station_code(self) -> str | None:
        parts = self.topic.split("/")
        if len(parts) >= 3 and parts[0] == "farm" and parts[-1] == "detections":
            return parts[1]
        return None

    def json(self) -> Any:
        return json.loads(self.payload.decode("utf-8"))


MessageHandler = Callable[[MqttMessage], Awaitable[None]]


def _build_client(
    *,
    broker_url: str,
    client_id: str,
    username: str | None,
    password: str | None,
    keepalive: int,
    tls_insecure: bool,
) -> aiomqtt.Client:
    hostname, port, tls = _parse_url(broker_url)
    tls_params = None
    if tls:
        ctx = ssl.create_default_context()
        if tls_insecure:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        tls_params = aiomqtt.TLSParameters(
            ca_certs=None,
            certfile=None,
            keyfile=None,
            cert_reqs=ctx.verify_mode,
            tls_version=ctx.minimum_version,
            ciphers=None,
            keyfile_password=None,
        )
    return aiomqtt.Client(
        hostname=hostname,
        port=port,
        identifier=client_id,
        username=username,
        password=password,
        keepalive=keepalive,
        tls_params=tls_params,
    )


class MqttClient:
    """Async-context-manager wrapper around :class:`aiomqtt.Client`.

    The class is intentionally small: callers either
    :meth:`publish` directly or call :meth:`run_forever` with a
    subscription callback. Higher-level abstractions (the edge
    publisher, the consumer subscriber) live next to the side they
    serve.
    """

    def __init__(
        self,
        *,
        broker_url: str | None = None,
        client_id: str | None = None,
        username: str | None = None,
        password: str | None = None,
        keepalive: int | None = None,
        tls_insecure: bool | None = None,
        reconnect_initial_delay: float | None = None,
        reconnect_max_delay: float | None = None,
    ) -> None:
        self._settings = mqtt_settings
        self._broker_url = broker_url or self._settings.broker_url
        self._client_id = client_id or self._settings.client_id
        self._username = username if username is not None else self._settings.username
        self._password = password if password is not None else self._settings.password
        self._keepalive = (
            keepalive if keepalive is not None else self._settings.keepalive
        )
        self._tls_insecure = (
            tls_insecure if tls_insecure is not None else self._settings.tls_insecure
        )
        self._initial_delay = (
            reconnect_initial_delay
            if reconnect_initial_delay is not None
            else self._settings.reconnect_initial_delay
        )
        self._max_delay = (
            reconnect_max_delay
            if reconnect_max_delay is not None
            else self._settings.reconnect_max_delay
        )
        self._client: aiomqtt.Client | None = None

    # ─────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────

    async def __aenter__(self) -> MqttClient:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        if self._client is not None:
            return
        client = _build_client(
            broker_url=self._broker_url,
            client_id=self._client_id,
            username=self._username,
            password=self._password,
            keepalive=self._keepalive,
            tls_insecure=self._tls_insecure,
        )
        await client.__aenter__()
        self._client = client
        logger.info(f"connected to MQTT broker at {self._broker_url}")

    async def disconnect(self) -> None:
        if self._client is None:
            return
        try:
            await self._client.__aexit__(None, None, None)
        finally:
            self._client = None

    @property
    def raw(self) -> aiomqtt.Client:
        if self._client is None:
            raise RuntimeError("MqttClient is not connected; use 'async with'")
        return self._client

    # ─────────────────────────────────────────────────────────────────
    # Publish
    # ─────────────────────────────────────────────────────────────────

    async def publish(
        self,
        topic: str,
        payload: bytes | str | dict,
        *,
        qos: int | None = None,
        retain: bool = False,
    ) -> None:
        """Publish ``payload`` to ``topic``.

        ``payload`` may be ``bytes``, ``str`` or a ``dict`` (JSON-
        encoded). ``qos`` defaults to :attr:`MqttSettings.qos`.
        """
        if self._client is None:
            raise RuntimeError("MqttClient is not connected; use 'async with'")
        if isinstance(payload, dict):
            payload = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        elif isinstance(payload, str):
            payload = payload.encode("utf-8")
        await self._client.publish(
            topic,
            payload,
            qos=qos if qos is not None else self._settings.qos,
            retain=retain,
        )

    # ─────────────────────────────────────────────────────────────────
    # Subscribe with reconnect
    # ─────────────────────────────────────────────────────────────────

    async def run_forever(
        self,
        topic: str,
        handler: MessageHandler,
        *,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        """Subscribe to ``topic`` and dispatch every message to
        ``handler`` until ``stop_event`` is set.

        Reconnects with exponential back-off (capped at
        :attr:`MqttSettings.reconnect_max_delay`) on any
        :class:`aiomqtt.MqttError`. The loop never raises — handlers
        that want to abort the loop should set ``stop_event``.
        """
        stop_event = stop_event or asyncio.Event()
        delay = self._initial_delay
        while not stop_event.is_set():
            try:
                await self.connect()
                assert self._client is not None
                await self._client.subscribe(topic, qos=self._settings.qos)
                logger.info(f"subscribed to {topic}")
                delay = self._initial_delay
                async for raw in self._client.messages:
                    if stop_event.is_set():
                        break
                    msg = MqttMessage(
                        topic=str(raw.topic),
                        payload=(
                            raw.payload
                            if isinstance(raw.payload, (bytes, bytearray))
                            else bytes(raw.payload)
                        ),
                    )
                    try:
                        await handler(msg)
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.opt(exception=exc).error(
                            f"handler raised on topic {msg.topic}"
                        )
                await self.disconnect()
            except asyncio.CancelledError:
                raise
            except aiomqtt.MqttError as exc:
                logger.warning(
                    f"MQTT connection lost ({exc}); reconnecting in {delay:.1f}s"
                )
                await self.disconnect()
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                    return
                except TimeoutError:
                    pass
                delay = min(delay * 2, self._max_delay)
            except Exception as exc:  # pragma: no cover - defensive
                logger.opt(exception=exc).error(
                    "unexpected error in MQTT subscribe loop"
                )
                await self.disconnect()
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                    return
                except TimeoutError:
                    pass
                delay = min(delay * 2, self._max_delay)


__all__ = [
    "MqttClient",
    "MqttMessage",
    "MessageHandler",
]
