"""Named SSE channels used across the service.

Importing this module instantiates each broker once. Other modules
subscribe or publish via these singletons.
"""

from src.sse.broker import SSEBroker

tractor_status_channel = SSEBroker()

__all__ = ["tractor_status_channel"]
