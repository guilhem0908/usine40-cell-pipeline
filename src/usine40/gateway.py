"""Edge gateway: OPC UA subscription in, MQTT telemetry out.

The gateway discovers the cell by browsing the server (stations are the child
objects, signals their variables), subscribes to every variable and republishes
each data change with the PLC source timestamp, a session identifier and a
sequence number. It holds no state worth keeping: after a restart it re-reads
the last ``backfill_s`` seconds from the server's OPC UA history and publishes
them flagged as replays, which the idempotent store merges with what it
already has. Telemetry is published retained, so a consumer that subscribes
late still receives the current value of every signal, including the ones
that never change such as the ideal cycle time. A sample without a source
timestamp or with a non-numeric value is counted and skipped rather than
forwarded with a made-up time.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import math
import uuid
from collections.abc import Callable

from asyncua import Client, ua

from usine40.bus import Bus
from usine40.config import Settings, configure_logging, install_stop_signals
from usine40.health import Heartbeat
from usine40.model import CELL_STATION, SENSORS_STATION
from usine40.opcua_server import NAMESPACE_URI, SIGNAL_LOAD_RATE, SIGNAL_NAMES
from usine40.payload import Telemetry
from usine40.timebase import datetime_to_us, now_us, seconds_to_us, us_to_datetime
from usine40.topics import CellPath, data_topic, status_topic

MONITORED_ITEM_QUEUE_SIZE = 1000
"""Data changes the server may queue per variable between two publish cycles."""

WATCHDOG_PERIOD_S = 1.0
RECONNECT_DELAY_S = 1.0
STATUS_ONLINE = "online"
STATUS_OFFLINE = "offline"

_log = logging.getLogger(__name__)


class Gateway:
    """One OPC UA client session republished on a message bus."""

    def __init__(
        self,
        opcua_url: str,
        bus: Bus,
        path: CellPath,
        *,
        qos: int = 1,
        backfill_s: float = 120.0,
        publishing_interval_ms: float = 100.0,
        clock: Callable[[], int] = now_us,
        session: str | None = None,
    ) -> None:
        self._url, self._bus, self._path = opcua_url, bus, path
        self._qos = qos
        self._backfill_us = seconds_to_us(backfill_s)
        self._publishing_interval_ms = publishing_interval_ms
        self._clock = clock
        self._client: Client | None = None
        self._signals: dict[ua.NodeId, tuple[str, str]] = {}
        self._primed: set[ua.NodeId] = set()
        self._seq = 0
        self.session = session or new_session_id()
        self.forwarded = 0
        self.replayed = 0
        self.skipped = 0

    async def connect(self) -> None:
        """Open the OPC UA session, subscribe, then replay the recent history.

        Subscribing first guarantees that the replay and the live stream
        overlap instead of leaving a gap; the overlap is removed downstream.
        """
        self._client = Client(self._url)
        await self._client.connect()
        self._signals = await self._discover(self._client)
        self._primed = set()
        subscription = await self._client.create_subscription(self._publishing_interval_ms, self)
        nodes = [self._client.get_node(node_id) for node_id in self._signals]
        await subscription.subscribe_data_change(
            nodes, queuesize=MONITORED_ITEM_QUEUE_SIZE, sampling_interval=0
        )
        _log.info("Subscribed to %d variables on %s", len(nodes), self._url)
        if self._backfill_us > 0:
            await self._replay_history(self._client)

    async def disconnect(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            with contextlib.suppress(Exception):
                await client.disconnect()

    async def run(self, stop: asyncio.Event, heartbeat: Heartbeat | None = None) -> None:
        """Keep a session open until ``stop`` is set, reconnecting after failures."""
        while not stop.is_set():
            try:
                await self.connect()
                while not stop.is_set():
                    await self._client.check_connection()
                    if heartbeat is not None:
                        heartbeat.beat()
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), WATCHDOG_PERIOD_S)
            except (OSError, TimeoutError, ua.UaError, ConnectionError) as error:
                reason = error or type(error).__name__
                _log.warning("OPC UA session lost (%s); reconnecting", reason)
            finally:
                await self.disconnect()
            if not stop.is_set():
                await asyncio.sleep(RECONNECT_DELAY_S)

    def datachange_notification(self, node, _value, data) -> None:
        """Subscription callback: forward one data change.

        The first notification of a variable is its current value, sent by the
        server when the subscription starts. It can be arbitrarily old, so it
        is flagged as a replay like the history, not as a live change.
        """
        station, signal = self._signals[node.nodeid]
        initial = node.nodeid not in self._primed
        self._primed.add(node.nodeid)
        if self._publish(station, signal, data.monitored_item.Value, replay=initial):
            if initial:
                self.replayed += 1
            else:
                self.forwarded += 1

    async def _discover(self, client: Client) -> dict[ua.NodeId, tuple[str, str]]:
        namespace = await client.get_namespace_index(NAMESPACE_URI)
        signals: dict[ua.NodeId, tuple[str, str]] = {}
        for cell in await client.nodes.objects.get_children_descriptions():
            if cell.NodeId.NamespaceIndex != namespace:
                continue
            for child in await client.get_node(cell.NodeId).get_children_descriptions():
                if child.NodeClass == ua.NodeClass.Variable:
                    signals[child.NodeId] = (CELL_STATION, _signal_name(child))
                elif child.NodeClass == ua.NodeClass.Object:
                    station = child.BrowseName.Name
                    variables = await client.get_node(child.NodeId).get_children_descriptions()
                    for variable in variables:
                        if variable.NodeClass == ua.NodeClass.Variable:
                            signals[variable.NodeId] = (station, _signal_name(variable))
        if not signals:
            raise ua.UaError(f"No variable found in namespace {NAMESPACE_URI}.")
        return signals

    async def _replay_history(self, client: Client) -> None:
        end_us = self._clock()
        start, end = us_to_datetime(end_us - self._backfill_us), us_to_datetime(end_us)
        before = self.replayed
        for node_id, (station, signal) in self._signals.items():
            if station == SENSORS_STATION or signal == SIGNAL_LOAD_RATE:
                continue
            for data_value in await client.get_node(node_id).read_raw_history(start, end):
                if self._publish(station, signal, data_value, replay=True):
                    self.replayed += 1
        _log.info("Replayed %d samples from the OPC UA history", self.replayed - before)

    def _publish(self, station: str, signal: str, data_value: ua.DataValue, replay: bool) -> bool:
        value = data_value.Value.Value if data_value.Value is not None else None
        moment = data_value.SourceTimestamp
        numeric = isinstance(value, bool | int | float) and math.isfinite(value)
        if moment is None or not numeric:
            self.skipped += 1
            return False
        self._seq += 1
        message = Telemetry(
            session=self.session,
            seq=self._seq,
            source_us=datetime_to_us(moment),
            gateway_us=self._clock(),
            value=float(value),
            replay=replay,
        )
        topic = data_topic(self._path, station, signal)
        self._bus.publish(topic, message.to_json(), self._qos, retain=True)
        return True


def new_session_id() -> str:
    """Random identifier of one run of the gateway process."""
    return uuid.uuid4().hex[:12]


def _signal_name(description: ua.ReferenceDescription) -> str:
    name = description.BrowseName.Name
    return SIGNAL_NAMES.get(name, name)


def status_payload(status: str, session: str) -> bytes:
    document = {"status": status, "session": session, "ts_us": now_us()}
    return json.dumps(document, separators=(",", ":")).encode()


def main() -> None:
    from usine40.mqtt_bus import MqttBus

    parser = argparse.ArgumentParser(description="Republish a cell's OPC UA data on MQTT.")
    parser.parse_args()
    settings = Settings.from_env()
    configure_logging()

    async def serve() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        install_stop_signals(lambda: loop.call_soon_threadsafe(stop.set))
        topic = status_topic(settings.path, "gateway")
        session = new_session_id()
        bus = MqttBus(
            settings.mqtt_host,
            settings.mqtt_port,
            f"usine40-gateway-{session}",
            will=(topic, status_payload(STATUS_OFFLINE, session)),
            on_online=lambda online: online.publish(
                topic, status_payload(STATUS_ONLINE, session), qos=1, retain=True
            ),
        )
        gateway = Gateway(
            settings.opcua_url,
            bus,
            settings.path,
            qos=settings.mqtt_qos,
            backfill_s=settings.backfill_s,
            publishing_interval_ms=settings.publishing_interval_ms,
            session=session,
        )
        bus.start()
        _log.info("Gateway session %s, QoS %d", session, settings.mqtt_qos)
        try:
            await gateway.run(stop, Heartbeat(settings.health_file))
        finally:
            bus.publish(topic, status_payload(STATUS_OFFLINE, session), qos=1, retain=True)
            bus.close()

    asyncio.run(serve())


if __name__ == "__main__":
    main()
