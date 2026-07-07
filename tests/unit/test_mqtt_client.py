"""Unit tests for :mod:`src.services.mqtt_client`.

The wrapper talks to a real broker in production; in unit tests we
stub :class:`aiomqtt.Client` with a fake that records calls and lets
us push synthetic messages into the consumer's stream. The goal is
to lock in:

* ``MqttMessage.station_code()`` topic parsing for the wildcard
  subscription;
* the JSON payload helper;
* the lifecycle (``async with`` -> connect/disconnect);
* ``publish`` encoding (dict/str/bytes) and the default QoS override;
* the ``run_forever`` subscribe loop dispatches messages to the
  handler, survives an :class:`aiomqtt.MqttError`, and exits
  cleanly when ``stop_event`` is set.

The broker URL parsing helper is exercised by every test that builds
a real :class:`MqttClient`, so it gets free coverage.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import patch

import aiomqtt
import pytest

from src.services.mqtt_client import (
    MqttClient,
    MqttMessage,
    _parse_url,
)

# ───────────────────────────────────────────────────────────────────────
# Fake aiomqtt.Client
# ───────────────────────────────────────────────────────────────────────


class _FakeMessages:
    """Async iterator that yields pre-seeded messages."""

    def __init__(self, messages: list[MqttMessage]) -> None:
        self._messages = list(messages)
        self._iter: asyncio.Future | None = None

    def __aiter__(self) -> _FakeMessages:
        return self

    async def __anext__(self) -> MqttMessage:
        if not self._messages:
            raise StopAsyncIteration
        await asyncio.sleep(0)
        return self._messages.pop(0)


class _FakeRawMessage:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


class _FakeClient:
    """In-memory stand-in for :class:`aiomqtt.Client`.

    Captures ``publish``/``subscribe`` calls and lets the test drive
    the ``messages`` iterator. ``raise_on`` can name a method
    (e.g. ``"publish"``) that should raise :class:`aiomqtt.MqttError`
    instead of running normally — used to exercise the reconnect
    path.
    """

    def __init__(
        self,
        *,
        messages: list[MqttMessage] | None = None,
        raise_on: str | None = None,
        publish_delay: float = 0.0,
    ) -> None:
        self.messages_iter = _FakeMessages(messages or [])
        self.published: list[tuple[str, bytes, int, bool]] = []
        self.subscribed: list[tuple[str, int]] = []
        self.connected = False
        self.disconnected = False
        self._raise_on = raise_on
        self._publish_delay = publish_delay

    async def __aenter__(self) -> _FakeClient:
        self.connected = True
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.disconnected = True

    @property
    def messages(self) -> _FakeMessages:
        return self.messages_iter

    async def subscribe(self, topic: str, qos: int = 0) -> None:
        if self._raise_on == "subscribe":
            raise aiomqtt.MqttError("simulated subscribe failure")
        self.subscribed.append((topic, qos))

    async def publish(
        self,
        topic: str,
        payload: bytes | None = None,
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        if self._publish_delay:
            await asyncio.sleep(self._publish_delay)
        if self._raise_on == "publish":
            raise aiomqtt.MqttError("simulated publish failure")
        self.published.append((topic, payload or b"", qos, retain))


def _stub_client(
    fake: _FakeClient,
) -> Any:
    """Return a context manager that yields ``fake`` from
    :class:`aiomqtt.Client` calls."""

    @asynccontextmanager
    async def _factory(*args: Any, **kwargs: Any):
        yield fake

    return _factory


# ───────────────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────────────


def _client(**overrides: Any) -> MqttClient:
    """Build an MqttClient with overridden settings so tests don't
    have to monkey-patch ``mqtt_settings``."""
    return MqttClient(
        broker_url=overrides.pop("broker_url", "mqtt://localhost:1883"),
        client_id=overrides.pop("client_id", "test-client"),
        username=overrides.pop("username", None),
        password=overrides.pop("password", None),
        keepalive=overrides.pop("keepalive", 30),
        tls_insecure=overrides.pop("tls_insecure", False),
        reconnect_initial_delay=overrides.pop("reconnect_initial_delay", 0.05),
        reconnect_max_delay=overrides.pop("reconnect_max_delay", 0.2),
        **overrides,
    )


# ───────────────────────────────────────────────────────────────────────
# URL parsing
# ───────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "url,expected",
    [
        ("mqtt://broker:1883", ("broker", 1883, False)),
        ("tcp://broker:1884", ("broker", 1884, False)),
        ("mqtts://broker:8883", ("broker", 8883, True)),
        ("mqtt://broker", ("broker", 1883, False)),
        ("mqtts://broker", ("broker", 8883, True)),
    ],
)
def test_parse_url_known_schemes(url: str, expected: tuple[str, int, bool]):
    assert _parse_url(url) == expected


def test_parse_url_unknown_scheme_warns_and_falls_back():
    """A typo in MQTT_BROKER_URL should not raise — it should warn
    and fall back to localhost:1883 so the rest of the test runs."""
    assert _parse_url("htttp://broker:1883") == ("localhost", 1883, False)


# ───────────────────────────────────────────────────────────────────────
# MqttMessage
# ───────────────────────────────────────────────────────────────────────


def test_mqtt_message_station_code_extracts_from_topic():
    msg = MqttMessage(topic="farm/STATION_01/detections", payload=b"{}")
    assert msg.station_code() == "STATION_01"


def test_mqtt_message_station_code_returns_none_for_unrelated_topic():
    msg = MqttMessage(topic="other/topic", payload=b"{}")
    assert msg.station_code() is None


def test_mqtt_message_json_decodes_payload():
    msg = MqttMessage(topic="t", payload=b'{"a":1}')
    assert msg.json() == {"a": 1}


# ───────────────────────────────────────────────────────────────────────
# Lifecycle
# ───────────────────────────────────────────────────────────────────────


async def test_aenter_connects_and_aexit_disconnects():
    fake = _FakeClient()
    with patch("src.services.mqtt_client._build_client", return_value=fake):
        async with _client() as mqtt:
            assert mqtt.raw is fake
            assert fake.connected
        assert fake.disconnected


async def test_double_connect_is_noop():
    fake = _FakeClient()
    with patch("src.services.mqtt_client._build_client", return_value=fake):
        c = _client()
        await c.connect()
        await c.connect()  # second call must not rebuild
        assert c.raw is fake
        await c.disconnect()


async def test_publish_without_connect_raises():
    c = _client()
    with pytest.raises(RuntimeError, match="not connected"):
        await c.publish("t", {"a": 1})


# ───────────────────────────────────────────────────────────────────────
# publish()
# ───────────────────────────────────────────────────────────────────────


async def test_publish_dict_encodes_to_json():
    fake = _FakeClient()
    with patch("src.services.mqtt_client._build_client", return_value=fake):
        async with _client() as mqtt:
            await mqtt.publish("farm/S01/detections", {"a": 1, "b": [2, 3]})
    assert fake.published == [("farm/S01/detections", b'{"a":1,"b":[2,3]}', 1, False)]


async def test_publish_str_and_bytes_passthrough():
    fake = _FakeClient()
    with patch("src.services.mqtt_client._build_client", return_value=fake):
        async with _client() as mqtt:
            await mqtt.publish("t", b"raw", qos=0)
            await mqtt.publish("t", "text", qos=0)
    assert fake.published[0] == ("t", b"raw", 0, False)
    assert fake.published[1] == ("t", b"text", 0, False)


async def test_publish_qos_override():
    fake = _FakeClient()
    with patch("src.services.mqtt_client._build_client", return_value=fake):
        async with _client() as mqtt:
            await mqtt.publish("t", {"x": 1}, qos=2, retain=True)
    assert fake.published == [("t", b'{"x":1}', 2, True)]


# ───────────────────────────────────────────────────────────────────────
# run_forever()
# ───────────────────────────────────────────────────────────────────────


async def test_run_forever_dispatches_messages_to_handler():
    msgs = [
        MqttMessage(topic="farm/S01/detections", payload=b'{"i":1}'),
        MqttMessage(topic="farm/S02/detections", payload=b'{"i":2}'),
    ]
    fake = _FakeClient(messages=msgs)
    seen: list[MqttMessage] = []

    async def handler(msg: MqttMessage) -> None:
        seen.append(msg)
        if len(seen) == len(msgs):
            stop.set()

    stop = asyncio.Event()
    with patch("src.services.mqtt_client._build_client", return_value=fake):
        c = _client()
        await c.run_forever("farm/+/detections", handler, stop_event=stop)
    assert [m.payload for m in seen] == [b'{"i":1}', b'{"i":2}']
    assert fake.subscribed == [("farm/+/detections", 1)]


async def test_run_forever_exits_when_stop_event_pre_set():
    fake = _FakeClient(messages=[])
    stop = asyncio.Event()
    stop.set()  # already stopped
    with patch("src.services.mqtt_client._build_client", return_value=fake):
        await _client().run_forever("t", lambda m: None, stop_event=stop)
    assert fake.subscribed == []  # never connected


async def test_run_forever_swallows_handler_exceptions():
    msgs = [
        MqttMessage(topic="t", payload=b"1"),
        MqttMessage(topic="t", payload=b"2"),
    ]
    fake = _FakeClient(messages=msgs)
    seen: list[int] = []

    async def bad_handler(msg: MqttMessage) -> None:
        seen.append(int(msg.payload))
        if len(seen) == 2:
            stop.set()
        raise ValueError("boom")

    stop = asyncio.Event()
    with patch("src.services.mqtt_client._build_client", return_value=fake):
        await _client().run_forever("t", bad_handler, stop_event=stop)
    assert seen == [1, 2]


async def test_run_forever_reconnects_after_mqtt_error():
    """After the first subscribe call fails with MqttError the
    wrapper must reconnect and keep going."""
    fake1 = _FakeClient(raise_on="subscribe")
    fake2 = _FakeClient(messages=[MqttMessage(topic="t", payload=b"ok")])
    factory_calls: list[_FakeClient] = []

    def factory(*args: Any, **kwargs: Any) -> _FakeClient:
        factory_calls.append(fake1 if len(factory_calls) == 0 else fake2)
        return factory_calls[-1]

    seen: list[MqttMessage] = []

    async def handler(msg: MqttMessage) -> None:
        seen.append(msg)
        stop.set()

    stop = asyncio.Event()
    with patch("src.services.mqtt_client._build_client", side_effect=factory):
        await _client(
            reconnect_initial_delay=0.01, reconnect_max_delay=0.05
        ).run_forever("t", handler, stop_event=stop)
    # First fake fails on subscribe; wrapper reconnects and second
    # fake delivers the message.
    assert factory_calls == [fake1, fake2]
    assert seen == [MqttMessage(topic="t", payload=b"ok")]
