"""Message bus abstraction and an in-memory broker substitute.

The gateway and the collector only need publish/subscribe with MQTT topic
semantics. ``MqttBus`` (:mod:`usine40.mqtt_bus`) talks to a real broker;
``InMemoryBroker`` stands in for it in tests and in the Docker-free demo.

The substitute models the behaviours the pipeline depends on and nothing more:
topic filters with ``+`` and ``#``, retained messages, and what happens to a
publisher during a broker outage (QoS 0 messages are dropped, QoS 1 messages
are queued by the client and sent in order when the broker returns, the last
unacknowledged ones possibly twice). Every number reported for a real broker
restart comes from Mosquitto in the compose stack, not from this class.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from typing import Protocol

MessageCallback = Callable[[str, bytes], None]

QOS_AT_MOST_ONCE = 0
QOS_AT_LEAST_ONCE = 1

REDELIVERY_WINDOW = 32
"""How many recent QoS 1 publishes a substitute client keeps for redelivery."""


class Bus(Protocol):
    """What a pipeline stage needs from a message broker connection."""

    def publish(self, topic: str, payload: bytes, qos: int, retain: bool = False) -> None: ...

    def subscribe(self, topic_filter: str, qos: int, callback: MessageCallback) -> None: ...

    def close(self) -> None: ...


def topic_matches(topic_filter: str, topic: str) -> bool:
    """MQTT 3.1.1 topic filter matching (``+`` one level, trailing ``#`` any levels)."""
    filter_levels, topic_levels = topic_filter.split("/"), topic.split("/")
    for index, level in enumerate(filter_levels):
        if level == "#":
            return index == len(filter_levels) - 1
        if index >= len(topic_levels):
            return False
        if level not in ("+", topic_levels[index]):
            return False
    return len(filter_levels) == len(topic_levels)


class InMemoryBroker:
    """Synchronous in-process broker substitute (see module docstring)."""

    def __init__(self) -> None:
        self.online = True
        self.dropped = 0
        self._subscriptions: list[tuple[str, MessageCallback]] = []
        self._retained: dict[str, bytes] = {}
        self._clients: list[InMemoryBus] = []

    def client(self) -> InMemoryBus:
        connection = InMemoryBus(self)
        self._clients.append(connection)
        return connection

    def stop(self) -> None:
        """The broker goes away; subscriptions survive (persistent sessions)."""
        self.online = False

    def start(self, redeliver: int = 0) -> None:
        """The broker returns and clients flush what they queued.

        ``redeliver`` models at-least-once delivery: each client sends again
        its last ``redeliver`` QoS 1 messages published before the outage, as
        a real client does for publishes whose acknowledgement was lost.
        """
        self.online = True
        for connection in self._clients:
            connection.flush(redeliver)

    def route(self, topic: str, payload: bytes, retain: bool) -> None:
        if retain:
            self._retained[topic] = payload
        for topic_filter, callback in list(self._subscriptions):
            if topic_matches(topic_filter, topic):
                callback(topic, payload)

    def add_subscription(self, topic_filter: str, callback: MessageCallback) -> None:
        self._subscriptions.append((topic_filter, callback))
        for topic, payload in list(self._retained.items()):
            if topic_matches(topic_filter, topic):
                callback(topic, payload)


class InMemoryBus:
    """Client connection to an :class:`InMemoryBroker`."""

    def __init__(self, broker: InMemoryBroker) -> None:
        self._broker = broker
        self._queued: list[tuple[str, bytes, bool]] = []
        self._last_sent: deque[tuple[str, bytes, bool]] = deque(maxlen=REDELIVERY_WINDOW)

    def publish(self, topic: str, payload: bytes, qos: int, retain: bool = False) -> None:
        message = (topic, payload, retain)
        if not self._broker.online:
            if qos == QOS_AT_MOST_ONCE:
                self._broker.dropped += 1
            else:
                self._queued.append(message)
            return
        if qos != QOS_AT_MOST_ONCE:
            self._last_sent.append(message)
        self._broker.route(topic, payload, retain)

    def subscribe(self, topic_filter: str, qos: int, callback: MessageCallback) -> None:
        self._broker.add_subscription(topic_filter, callback)

    def flush(self, redeliver: int) -> None:
        resend = list(self._last_sent)[-redeliver:] if redeliver > 0 else []
        queued, self._queued = self._queued, []
        self._last_sent.clear()
        for topic, payload, retain in resend + queued:
            self._broker.route(topic, payload, retain)

    def close(self) -> None:
        self._queued.clear()
