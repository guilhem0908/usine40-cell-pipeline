"""Deterministic simulator of a serial production cell with finite buffers.

The cell is a line of stations separated by buffers::

    raw parts -> [station 0] -> buffer 0 -> [station 1] -> buffer 1 -> [station 2] -> out

It is advanced like a PLC program, one fixed scan at a time. Scan ``k`` covers
``[k * T, (k + 1) * T)`` and every duration is an integer number of scans, so a
run is reproducible bit for bit from the seed and the event timestamps fall on
an exact microsecond grid.

Station states during a scan:

* IDLE: planned stop (the whole cell); not counted as planned production time
* FAULT: breakdown, random (exponential time between failures measured in run
  time, exponential repair time) or injected (scripted schedule, live command)
* BLOCKED: holds a finished part and the downstream buffer is full
* STARVED: empty and the upstream buffer is empty
* RUNNING: working on a part

Cycle times are ``ideal * (1 + X)`` with ``X ~ Exp(mean = slowdown)``: a station
never beats its ideal cycle, so the performance ratio stays below one except
for the part that straddles a window boundary.

This is a Python model of a controller, not IEC 61131-3 code, and the five
states are a simplification that does not implement the PackML state machine.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field

from usine40.model import CELL_STATION, Event, EventKind, State
from usine40.timebase import seconds_to_us

MAX_INJECTED_FAULT_S = 120.0
"""Upper bound applied to live fault-injection commands."""

HEARTBEAT_PERIOD_S = 1.0


@dataclass(frozen=True, slots=True)
class StationConfig:
    name: str
    ideal_cycle_s: float
    slowdown: float = 0.0
    scrap_probability: float = 0.0
    mtbf_s: float = math.inf
    mttr_s: float = 0.0


@dataclass(frozen=True, slots=True)
class FaultWindow:
    """A scripted breakdown of one station, relative to the start of the run."""

    station: str
    start_s: float
    duration_s: float


@dataclass(frozen=True, slots=True)
class PlannedStop:
    """A scheduled stop of the whole cell (break, changeover)."""

    start_s: float
    duration_s: float


@dataclass(frozen=True, slots=True)
class CellConfig:
    stations: tuple[StationConfig, ...]
    buffer_capacity: tuple[int, ...]
    scan_period_s: float = 0.05
    scripted_faults: tuple[FaultWindow, ...] = ()
    planned_stops: tuple[PlannedStop, ...] = ()
    seed: int = 0

    def __post_init__(self) -> None:
        if len(self.stations) < 1:
            raise ValueError("A cell needs at least one station.")
        if len(self.buffer_capacity) != len(self.stations) - 1:
            raise ValueError("Expected one buffer between each pair of stations.")
        if any(capacity < 1 for capacity in self.buffer_capacity):
            raise ValueError("Buffer capacities must be at least 1.")
        if self.scan_period_s <= 0:
            raise ValueError("scan_period_s must be positive.")
        names = [station.name for station in self.stations]
        if len(set(names)) != len(names):
            raise ValueError("Station names must be unique.")
        for fault in self.scripted_faults:
            if fault.station not in names:
                raise ValueError(f"Scripted fault on unknown station {fault.station!r}.")

    @property
    def station_names(self) -> tuple[str, ...]:
        return tuple(station.name for station in self.stations)


def default_cell(
    seed: int = 0,
    scripted_faults: tuple[FaultWindow, ...] = (),
    planned_stops: tuple[PlannedStop, ...] = (),
) -> CellConfig:
    """Three-station cell used by the demo, the tests and the compose stack.

    Machining is the bottleneck (2.0 s ideal cycle); the infeed is faster and
    gets blocked, the inspection is faster and gets starved.
    """
    return CellConfig(
        stations=(
            StationConfig("infeed", 1.6, slowdown=0.05, mtbf_s=600.0, mttr_s=8.0),
            StationConfig(
                "machining", 2.0, slowdown=0.08, scrap_probability=0.03, mtbf_s=300.0, mttr_s=12.0
            ),
            StationConfig(
                "inspection", 1.4, slowdown=0.05, scrap_probability=0.04, mtbf_s=900.0, mttr_s=6.0
            ),
        ),
        buffer_capacity=(4, 4),
        scripted_faults=scripted_faults,
        planned_stops=planned_stops,
        seed=seed,
    )


@dataclass(slots=True)
class _Station:
    config: StationConfig
    rng: random.Random
    ideal_scans: int
    state: State | None = None
    work_left: int = 0
    holding_finished: bool = False
    fault_left: int = 0
    run_until_failure: int = 0
    taken: int = 0
    total: int = 0
    good: int = 0
    scans_in_state: list[int] = field(default_factory=lambda: [0] * len(State))


class CellSimulator:
    """Scan-by-scan simulator. ``step`` returns the events of one scan."""

    def __init__(self, config: CellConfig, epoch_us: int = 0) -> None:
        self.config = config
        self.epoch_us = epoch_us
        self._period_us = seconds_to_us(config.scan_period_s)
        self._scan = 0
        self._heartbeat_scans = max(1, self._to_scans(HEARTBEAT_PERIOD_S))
        self._buffers = [0] * len(config.buffer_capacity)
        self._stations = [self._make_station(station) for station in config.stations]
        self._index = {station.name: i for i, station in enumerate(config.stations)}
        self._scripted: dict[int, list[tuple[int, int]]] = {}
        for fault in config.scripted_faults:
            entry = (self._index[fault.station], self._to_scans(fault.duration_s))
            self._scripted.setdefault(self._to_scans(fault.start_s), []).append(entry)
        self._planned = [
            (self._to_scans(stop.start_s), self._to_scans(stop.start_s + stop.duration_s))
            for stop in config.planned_stops
        ]
        self._pending_faults: dict[int, int] = {}

    @property
    def scan_index(self) -> int:
        return self._scan

    @property
    def time_us(self) -> int:
        """Timestamp of the start of the next scan to be executed."""
        return self.epoch_us + self._scan * self._period_us

    @property
    def buffers(self) -> tuple[int, ...]:
        return tuple(self._buffers)

    def counters(self, station: str) -> dict[str, int]:
        """Part counts of a station plus what it currently holds (for invariants)."""
        entry = self._stations[self._index[station]]
        return {
            "taken": entry.taken,
            "total": entry.total,
            "good": entry.good,
            "in_station": int(entry.work_left > 0 or entry.holding_finished),
        }

    def scans_in_state(self, station: str) -> tuple[int, ...]:
        """Number of scans spent in each state, indexed by ``State``."""
        return tuple(self._stations[self._index[station]].scans_in_state)

    def inject_fault(self, station: str, duration_s: float) -> float:
        """Request a breakdown starting at the next scan; returns the accepted duration.

        The duration is clamped to ``[one scan, MAX_INJECTED_FAULT_S]`` so that a
        wrong command cannot stop the cell for an unbounded time.
        """
        if station not in self._index:
            raise ValueError(f"Unknown station {station!r}.")
        if not math.isfinite(duration_s):
            raise ValueError("duration_s must be finite.")
        accepted = min(max(duration_s, self.config.scan_period_s), MAX_INJECTED_FAULT_S)
        index = self._index[station]
        scans = self._to_scans(accepted)
        self._pending_faults[index] = max(self._pending_faults.get(index, 0), scans)
        return scans * self.config.scan_period_s

    def step(self) -> list[Event]:
        """Execute one scan and return its events, all stamped at the scan start."""
        scan, ts_us = self._scan, self.time_us
        events: list[Event] = []
        if scan == 0:
            events.extend(
                Event(ts_us, s.config.name, EventKind.IDEAL, s.config.ideal_cycle_s)
                for s in self._stations
            )
        for index, scans in self._scripted.get(scan, ()):
            self._pending_faults[index] = max(self._pending_faults.get(index, 0), scans)
        planned_stop = any(start <= scan < end for start, end in self._planned)
        # Downstream first: a slot freed by station i+1 is usable by station i in the same scan.
        for index in reversed(range(len(self._stations))):
            station = self._stations[index]
            state = self._advance(index, planned_stop, ts_us, events)
            station.scans_in_state[state] += 1
            if state is not station.state:
                station.state = state
                events.append(Event(ts_us, station.config.name, EventKind.STATE, float(state)))
        if scan % self._heartbeat_scans == 0:
            beat = float(scan // self._heartbeat_scans)
            events.append(Event(ts_us, CELL_STATION, EventKind.HEARTBEAT, beat))
        self._scan += 1
        return events

    def _advance(self, index: int, planned_stop: bool, ts_us: int, events: list[Event]) -> State:
        station = self._stations[index]
        if planned_stop:
            return State.IDLE
        injected = self._pending_faults.pop(index, 0)
        station.fault_left = max(station.fault_left, injected)
        if station.fault_left > 0:
            station.fault_left -= 1
            return State.FAULT
        if station.holding_finished:
            if not self._can_release(index):
                return State.BLOCKED
            self._release(index, ts_us, events)
        if station.work_left == 0:
            if not self._can_take(index):
                return State.STARVED
            self._take(index)
        station.run_until_failure -= 1
        if station.run_until_failure <= 0:
            station.run_until_failure = self._draw_run_until_failure(station)
            station.fault_left = self._draw_repair(station) - 1
            return State.FAULT
        station.work_left -= 1
        if station.work_left == 0:
            station.holding_finished = True
        return State.RUNNING

    def _can_take(self, index: int) -> bool:
        return index == 0 or self._buffers[index - 1] > 0

    def _take(self, index: int) -> None:
        station = self._stations[index]
        if index > 0:
            self._buffers[index - 1] -= 1
        station.taken += 1
        mean_slowdown = station.config.slowdown
        slowdown = station.rng.expovariate(1.0 / mean_slowdown) if mean_slowdown > 0 else 0.0
        cycle_scans = self._to_scans(station.config.ideal_cycle_s * (1.0 + slowdown))
        station.work_left = max(station.ideal_scans, cycle_scans)

    def _can_release(self, index: int) -> bool:
        last = index == len(self._stations) - 1
        return last or self._buffers[index] < self.config.buffer_capacity[index]

    def _release(self, index: int, ts_us: int, events: list[Event]) -> None:
        station = self._stations[index]
        station.holding_finished = False
        good = station.rng.random() >= station.config.scrap_probability
        station.total += 1
        if good:
            station.good += 1
            if index < len(self._buffers):
                self._buffers[index] += 1
        events.append(Event(ts_us, station.config.name, EventKind.PART, 1.0 if good else 0.0))

    def _make_station(self, config: StationConfig) -> _Station:
        rng = random.Random(f"{self.config.seed}/{config.name}")
        station = _Station(config, rng, ideal_scans=max(1, self._to_scans(config.ideal_cycle_s)))
        station.run_until_failure = self._draw_run_until_failure(station)
        return station

    def _draw_run_until_failure(self, station: _Station) -> int:
        if not math.isfinite(station.config.mtbf_s):
            return 2**62
        return max(1, self._to_scans(station.rng.expovariate(1.0 / station.config.mtbf_s)))

    def _draw_repair(self, station: _Station) -> int:
        if station.config.mttr_s <= 0:
            return 1
        return max(1, self._to_scans(station.rng.expovariate(1.0 / station.config.mttr_s)))

    def _to_scans(self, seconds: float) -> int:
        return round(seconds / self.config.scan_period_s)
