"""MQTT implementation of :class:`usine40.bus.Bus` on top of paho-mqtt.

The client reconnects by itself. While the broker is unreachable, paho keeps
QoS 1 publishes in memory and sends them again on reconnection (flagged as
duplicates when they were in flight), and it refuses QoS 0 publishes, which
are therefore lost. Subscriptions are re-issued on every connection so that a
broker that lost its session state still delivers.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

import paho.mqtt.client as mqtt

from usine40.bus import MessageCallback

KEEPALIVE_S = 5
RECONNECT_MIN_DELAY_S = 1
RECONNECT_MAX_DELAY_S = 2
MAX_INFLIGHT_MESSAGES = 200

_log = logging.getLogger(__name__)


class MqttBus:
    """Connection to an MQTT 3.1.1 broker with automatic reconnection."""

    def __init__(
        self,
        host: str,
        port: int,
        client_id: str,
        *,
        clean_session: bool = True,
        will: tuple[str, bytes] | None = None,
        on_online: Callable[[MqttBus], None] | None = None,
    ) -> None:
        self._host, self._port = host, port
        self._on_online = on_online
        self._subscriptions: list[tuple[str, int]] = []
        self._lock = threading.Lock()
        self._connected = threading.Event()
        self.refused = 0
        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=clean_session,
            protocol=mqtt.MQTTv311,
        )
        self._client.max_inflight_messages_set(MAX_INFLIGHT_MESSAGES)
        self._client.max_queued_messages_set(0)
        self._client.reconnect_delay_set(RECONNECT_MIN_DELAY_S, RECONNECT_MAX_DELAY_S)
        if will is not None:
            self._client.will_set(will[0], will[1], qos=1, retain=True)
        self._client.on_connect = self._handle_connect
        self._client.on_disconnect = self._handle_disconnect

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def start(self) -> None:
        """Start the network thread; the first connection is retried until it succeeds."""
        self._client.connect_async(self._host, self._port, keepalive=KEEPALIVE_S)
        self._client.loop_start()

    def wait_connected(self, timeout_s: float) -> bool:
        return self._connected.wait(timeout_s)

    def publish(self, topic: str, payload: bytes, qos: int, retain: bool = False) -> None:
        info = self._client.publish(topic, payload, qos=qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS and qos == 0:
            self.refused += 1

    def subscribe(self, topic_filter: str, qos: int, callback: MessageCallback) -> None:
        def deliver(_client: mqtt.Client, _userdata: object, message: mqtt.MQTTMessage) -> None:
            callback(message.topic, message.payload)

        self._client.message_callback_add(topic_filter, deliver)
        with self._lock:
            self._subscriptions.append((topic_filter, qos))
        if self.connected:
            self._client.subscribe(topic_filter, qos)

    def close(self) -> None:
        self._client.disconnect()
        self._client.loop_stop()

    def _handle_connect(
        self,
        client: mqtt.Client,
        _userdata: object,
        _flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ) -> None:
        if reason_code.is_failure:
            _log.warning("MQTT connection refused: %s", reason_code)
            return
        with self._lock:
            subscriptions = list(self._subscriptions)
        for topic_filter, qos in subscriptions:
            client.subscribe(topic_filter, qos)
        self._connected.set()
        _log.info("MQTT connected to %s:%s", self._host, self._port)
        if self._on_online is not None:
            self._on_online(self)

    def _handle_disconnect(
        self,
        _client: mqtt.Client,
        _userdata: object,
        _flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ) -> None:
        self._connected.clear()
        _log.warning("MQTT disconnected: %s", reason_code)
