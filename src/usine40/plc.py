"""Simulated PLC: the cell simulator scanned behind an OPC UA server.

One scan does what a controller cycle does: advance the process model, update
the outputs (here the OPC UA variables) and log. In the container the scans
are paced by the wall clock; tests and the validation runs call :meth:`Plc.scan`
as fast as they like, because the timestamps come from the scan counter and not
from the clock.

Besides the production signals the PLC can generate synthetic load: a set of
analog tags whose value is their own update count, so that a consumer can tell
how many updates it missed without any side channel.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
from pathlib import Path
from typing import TextIO

from usine40.config import Settings, configure_logging, install_stop_signals
from usine40.health import Heartbeat
from usine40.model import (
    CELL_STATION,
    SENSORS_STATION,
    SIGNAL_GOOD,
    SIGNAL_TOTAL,
    Event,
    Sample,
    SampleEncoder,
)
from usine40.opcua_server import SIGNAL_LOAD_RATE, CellServer, analog_signal
from usine40.sim import (
    HEARTBEAT_PERIOD_S,
    CellConfig,
    CellSimulator,
    FaultWindow,
    PlannedStop,
    default_cell,
)
from usine40.timebase import US_PER_S, now_us, seconds_to_us

ANALOG_TAGS = 100
"""Synthetic tags in the container; bounds the load at tags / scan period per second."""

_log = logging.getLogger(__name__)


class LoadGenerator:
    """Round-robin updates of analog tags at a bounded rate."""

    def __init__(self, tags: int, scan_period_s: float) -> None:
        self._tags = tags
        self._scan_period_s = scan_period_s
        self._counts = [0] * tags
        self._next = 0
        self._budget = 0.0
        self.rate = 0.0

    @property
    def max_rate(self) -> float:
        """One update per tag and per scan at most, so sample keys stay unique."""
        return self._tags / self._scan_period_s

    def set_rate(self, requested: float) -> float:
        """Clamp the requested changes per second to ``[0, max_rate]``."""
        self.rate = 0.0 if math.isnan(requested) else min(max(requested, 0.0), self.max_rate)
        return self.rate

    def samples(self, ts_us: int) -> list[Sample]:
        self._budget = min(self._budget + self.rate * self._scan_period_s, float(self._tags))
        count = int(self._budget)
        self._budget -= count
        samples = []
        for _ in range(count):
            tag = self._next
            self._next = (tag + 1) % self._tags
            self._counts[tag] += 1
            value = float(self._counts[tag])
            samples.append(Sample(SENSORS_STATION, analog_signal(tag), ts_us, value))
        return samples


class Plc:
    """Simulator, OPC UA server and event log advanced together, scan by scan."""

    def __init__(
        self,
        config: CellConfig,
        endpoint: str,
        cell_name: str,
        *,
        analog_tags: int = 0,
        event_log: TextIO | None = None,
        keep_events: bool = False,
    ) -> None:
        self.config = config
        self._endpoint, self._cell_name = endpoint, cell_name
        self._event_log = event_log
        self._encoder = SampleEncoder()
        self._load = LoadGenerator(analog_tags, config.scan_period_s)
        self._analog_tags = analog_tags
        self.events: list[Event] | None = [] if keep_events else None
        self.sim: CellSimulator | None = None
        self._server: CellServer | None = None

    async def start(self, epoch_us: int) -> None:
        """Run scan 0, publish its values, then let clients in."""
        self.sim = CellSimulator(self.config, epoch_us)
        self._server = CellServer(
            self._endpoint,
            self._cell_name,
            self.config.station_names,
            inject_fault=self.sim.inject_fault,
            analog_tags=self._analog_tags,
        )
        await self._server.build()
        counters_at_zero = [
            Sample(station, signal, epoch_us, 0.0)
            for station in self.config.station_names
            for signal in (SIGNAL_TOTAL, SIGNAL_GOOD)
        ]
        await self._server.write(counters_at_zero)
        await self.scan()
        await self._server.open()

    async def scan(self) -> list[Event]:
        """Execute one scan: step the model, write the changed variables, log."""
        ts_us = self.sim.time_us
        events = self.sim.step()
        samples = [sample for event in events for sample in self._encoder.encode(event)]
        if self._load.rate > 0:
            samples.extend(self._load.samples(ts_us))
        await self._server.write(samples)
        if self.events is not None:
            self.events.extend(events)
        if self._event_log is not None:
            for event in events:
                self._event_log.write(event.to_json() + "\n")
        return events

    async def stop(self) -> None:
        if self._server is not None:
            await self._server.stop()

    async def run_realtime(self, stop: asyncio.Event, heartbeat: Heartbeat | None = None) -> None:
        """Pace the scans on the wall clock until ``stop`` is set.

        Scan ``k`` is due at ``epoch + k * period``. A late scan is not skipped:
        it runs immediately with its nominal timestamp, so the model never
        drifts from the clock by more than the delay itself.
        """
        housekeeping_scans = max(1, round(HEARTBEAT_PERIOD_S / self.config.scan_period_s))
        while not stop.is_set():
            delay_s = (self.sim.time_us - now_us()) / US_PER_S
            await asyncio.sleep(max(delay_s, 0.0))
            await self.scan()
            if self.sim.scan_index % housekeeping_scans == 0:
                await self._apply_load_setpoint()
                if self._event_log is not None:
                    self._event_log.flush()
                if heartbeat is not None:
                    heartbeat.beat()

    async def _apply_load_setpoint(self) -> None:
        requested = await self._server.read_load_rate()
        accepted = self._load.set_rate(requested)
        if accepted != requested:
            setpoint = Sample(CELL_STATION, SIGNAL_LOAD_RATE, self.sim.time_us, accepted)
            await self._server.write([setpoint])


def cell_from_settings(settings: Settings) -> CellConfig:
    """Default cell with the scripted faults and planned stops given as JSON lists."""
    faults = tuple(FaultWindow(**item) for item in json.loads(settings.fault_schedule))
    stops = tuple(PlannedStop(**item) for item in json.loads(settings.planned_stops))
    return default_cell(settings.seed, faults, stops)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the simulated PLC and its OPC UA server.")
    parser.parse_args()
    settings = Settings.from_env()
    configure_logging()
    config = cell_from_settings(settings)

    async def serve() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        install_stop_signals(lambda: loop.call_soon_threadsafe(stop.set))
        period_us = seconds_to_us(config.scan_period_s)
        with Path(settings.event_log).open("w", encoding="utf-8") as event_log:
            plc = Plc(
                config,
                settings.opcua_bind,
                settings.cell,
                analog_tags=ANALOG_TAGS,
                event_log=event_log,
            )
            epoch_us = now_us()
            await plc.start(epoch_us - epoch_us % period_us)
            _log.info("PLC serving %s, seed %d", settings.opcua_bind, settings.seed)
            try:
                await plc.run_realtime(stop, Heartbeat(settings.health_file))
            finally:
                await plc.stop()

    asyncio.run(serve())


if __name__ == "__main__":
    main()
