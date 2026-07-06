"""Minimal pub/sub SSE broker.

Adapted from ``sbr/src/sse/broker.py``. Each connected client gets an
``asyncio.Queue``; ``publish`` fan-outs the same payload to every
queue. ``stream`` is an async generator that yields
:class:`sse_starlette.sse.ServerSentEvent` events.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any

from sse_starlette.sse import ServerSentEvent


class SSEBroker:
    def __init__(self) -> None:
        self._subscribers: list[asyncio.Queue[ServerSentEvent]] = []

    async def connect(self) -> asyncio.Queue[ServerSentEvent]:
        queue: asyncio.Queue[ServerSentEvent] = asyncio.Queue()
        self._subscribers.append(queue)
        return queue

    async def disconnect(self, queue: asyncio.Queue[ServerSentEvent]) -> None:
        if queue in self._subscribers:
            self._subscribers.remove(queue)

    async def publish(
        self,
        payload: dict[str, Any],
        *,
        event: str = "message",
    ) -> None:
        """Fan-out a payload to every connected subscriber.

        Subscribers that have closed queues are silently dropped so a
        dead client never blocks the producer.
        """
        message = ServerSentEvent(event=event, data=_jsonify(payload))
        dead: list[asyncio.Queue[ServerSentEvent]] = []
        for queue in list(self._subscribers):
            try:
                await queue.put(message)
            except Exception:
                dead.append(queue)
        for q in dead:
            await self.disconnect(q)

    async def stream(self) -> AsyncGenerator[ServerSentEvent, None]:
        queue = await self.connect()
        try:
            yield ServerSentEvent(event="ready", data=_jsonify({"status": "listening"}))
            while True:
                yield await queue.get()
        except asyncio.CancelledError:
            pass
        finally:
            await self.disconnect(queue)


def _jsonify(payload: dict[str, Any]) -> str:
    import json

    return json.dumps(payload, default=str, ensure_ascii=False)
