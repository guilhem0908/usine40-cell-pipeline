"""The whole pipeline in one process and in simulated time.

``Rig`` wires the real components (simulated PLC with its OPC UA server over
loopback TCP, gateway, collector) to the in-memory broker substitute and an
in-memory SQLite store, and advances the PLC scan by scan as fast as the
machine allows. Source timestamps start from a fixed virtual epoch, so a run
depends only on the cell configuration and the seed. It is what the tests, the
Docker-free demo and the multi-seed validation use; the compose stack measures
the same chain against Mosquitto and PostgreSQL in real time.
"""

from __future__ import annotations

import asyncio
import socket
import time
from datetime import UTC, datetime

from usine40.bus import InMemoryBroker, InMemoryBus, MessageCallback
from usine40.collector import Collector
from usine40.gateway import Gateway
from usine40.ground_truth import truth_windows
from usine40.model import CELL_STATION, SIGNAL_HEARTBEAT, Event, expected_samples
from usine40.oee import WindowTotals
from usine40.payload import Telemetry
from usine40.plc import Plc
from usine40.sim import CellConfig
from usine40.store import SqlStore, open_sqlite
from usine40.timebase import datetime_to_us, seconds_to_us
from usine40.topics import CellPath, parse_data_topic
from usine40.validation import Comparison, compare_windows, missing_samples

VIRTUAL_EPOCH_US = datetime_to_us(datetime(2026, 1, 1, tzinfo=UTC))
PUBLISHING_INTERVAL_MS = 10.0
SETTLE_EVERY_S = 30.0
SETTLE_TIMEOUT_S = 20.0
CELL_NAME = "cell1"


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class _PublishTap:
    """Bus wrapper that records which samples the gateway has published."""

    def __init__(self, inner: InMemoryBus, published: set[tuple[str, str, int]]) -> None:
        self._inner, self._published = inner, published

    def publish(self, topic: str, payload: bytes, qos: int, retain: bool = False) -> None:
        _, station, signal = parse_data_topic(topic)
        self._published.add((station, signal, Telemetry.from_json(payload).source_us))
        self._inner.publish(topic, payload, qos, retain)

    def subscribe(self, topic_filter: str, qos: int, callback: MessageCallback) -> None:
        self._inner.subscribe(topic_filter, qos, callback)

    def close(self) -> None:
        self._inner.close()


class Rig:
    """In-process pipeline. Use as ``async with Rig(config) as rig``."""

    def __init__(
        self,
        config: CellConfig,
        *,
        qos: int = 1,
        backfill_s: float = 120.0,
        window_s: float = 30.0,
    ) -> None:
        self.config = config
        self._qos, self._backfill_s = qos, backfill_s
        self._window_us = seconds_to_us(window_s)
        self._url = f"opc.tcp://127.0.0.1:{free_port()}/usine40"
        self._path = CellPath("site1", "assembly", CELL_NAME)
        self.plc = Plc(config, self._url, CELL_NAME, keep_events=True)
        self.broker = InMemoryBroker()
        self.store: SqlStore = open_sqlite()
        self.collector = Collector(
            self.broker.client(),
            self.store,
            self._path,
            qos=qos,
            window_s=window_s,
            clock=self._now_us,
        )
        self.gateway: Gateway | None = None
        self._published: set[tuple[str, str, int]] = set()

    @property
    def events(self) -> list[Event]:
        return self.plc.events

    async def __aenter__(self) -> Rig:
        await self.plc.start(VIRTUAL_EPOCH_US)
        self.collector.start()
        await self.start_gateway()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop_gateway()
        await self.plc.stop()
        self.store.close()

    async def start_gateway(self, backfill_s: float | None = None) -> None:
        """Start a new gateway process stand-in (new session, optional history replay)."""
        self._published.clear()
        self.gateway = Gateway(
            self._url,
            _PublishTap(self.broker.client(), self._published),
            self._path,
            qos=self._qos,
            backfill_s=self._backfill_s if backfill_s is None else backfill_s,
            publishing_interval_ms=PUBLISHING_INTERVAL_MS,
            clock=self._now_us,
        )
        await self.gateway.connect()
        await self.settle()

    async def stop_gateway(self) -> None:
        """Kill the gateway after letting it forward what it already received."""
        if self.gateway is not None:
            await self.settle()
            await self.gateway.disconnect()
            self.gateway = None

    async def advance(self, seconds: float) -> None:
        """Run the PLC for ``seconds`` of simulated time."""
        scans = round(seconds / self.config.scan_period_s)
        settle_scans = round(SETTLE_EVERY_S / self.config.scan_period_s)
        for index in range(scans):
            await self.plc.scan()
            if (index + 1) % settle_scans == 0:
                await self.settle()
        await self.settle()

    async def settle(self) -> None:
        """Wait until the gateway has published the newest sample written by the PLC.

        The substitute broker delivers synchronously, so once the gateway has
        published a sample a flush puts it in the store (or, during a broker
        outage, the sample is already dropped or queued). Without a gateway
        there is nothing to wait for.
        """
        if self.gateway is not None:
            newest = expected_samples(self.events[-1:])[-1].key
            deadline = time.monotonic() + SETTLE_TIMEOUT_S
            while newest not in self._published:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"Sample {newest} was never published by the gateway.")
                await asyncio.sleep(PUBLISHING_INTERVAL_MS / 1000.0)
        self.collector.flush()

    def watermark_us(self) -> int:
        return self.store.latest_timestamp(CELL_STATION, SIGNAL_HEARTBEAT)

    def truth(self) -> list[WindowTotals]:
        """Ground-truth windows that end at or before the pipeline watermark."""
        watermark = self.watermark_us()
        windows = truth_windows(self.events, self._window_us, watermark)
        return [w for station in windows.values() for w in station if w.end_us <= watermark]

    def pipeline(self) -> list[WindowTotals]:
        """Complete OEE windows as the collector computed and stored them."""
        self.collector.flush()
        self.collector.aggregate(since_us=VIRTUAL_EPOCH_US)
        return self.store.oee_windows(complete_only=True)

    def compare(self) -> Comparison:
        pipeline = self.pipeline()
        return compare_windows(self.truth(), pipeline)

    def missing(self) -> int:
        """Samples produced by the simulator before the watermark that are not stored."""
        watermark = self.watermark_us()
        events = [event for event in self.events if event.ts_us <= watermark]
        return len(missing_samples(events, self.store.sample_keys()))

    def _now_us(self) -> int:
        return self.plc.sim.time_us if self.plc.sim is not None else VIRTUAL_EPOCH_US
