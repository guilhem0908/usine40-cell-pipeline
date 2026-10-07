"""Shared vocabulary: station states, simulator events and published samples.

Two representations of the same production run exist on purpose:

* ``Event`` is what the simulator knows: a state change, one finished part
  (good or scrap), a heartbeat. The ground truth is computed from events.
* ``Sample`` is what a PLC exposes: the value of a signal at a source
  timestamp, with parts reported as cumulative counters. The pipeline only
  ever sees samples.

``SampleEncoder`` is the one-way bridge between the two.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from enum import IntEnum, StrEnum
from pathlib import Path


class State(IntEnum):
    """Station states. IDLE is a planned stop and is excluded from planned time."""

    IDLE = 0
    RUNNING = 1
    BLOCKED = 2
    STARVED = 3
    FAULT = 4


STATE_VALUES = frozenset(float(state) for state in State)
"""The only values a ``state`` sample may carry."""


class EventKind(StrEnum):
    STATE = "state"
    PART = "part"
    IDEAL = "ideal"
    HEARTBEAT = "heartbeat"


SIGNAL_STATE = "state"
SIGNAL_TOTAL = "total_count"
SIGNAL_GOOD = "good_count"
SIGNAL_IDEAL = "ideal_cycle_s"
SIGNAL_HEARTBEAT = "heartbeat"

CELL_STATION = "cell"
"""Pseudo-station that carries cell-level signals (the heartbeat)."""

SENSORS_STATION = "sensors"
"""Pseudo-station that carries the synthetic analog tags used for load tests."""

STATION_SIGNALS = (SIGNAL_STATE, SIGNAL_TOTAL, SIGNAL_GOOD, SIGNAL_IDEAL)


@dataclass(frozen=True, slots=True)
class Event:
    """One thing that happened in the simulator at ``ts_us``.

    ``value`` holds the new state (STATE), 1.0 for a good part and 0.0 for a
    scrapped one (PART), the ideal cycle time in seconds (IDEAL) or the beat
    number (HEARTBEAT).
    """

    ts_us: int
    station: str
    kind: EventKind
    value: float

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @classmethod
    def from_json(cls, line: str) -> Event:
        raw = json.loads(line)
        return cls(int(raw["ts_us"]), raw["station"], EventKind(raw["kind"]), float(raw["value"]))


@dataclass(frozen=True, slots=True)
class Sample:
    """The value of one signal of one station at a PLC source timestamp."""

    station: str
    signal: str
    ts_us: int
    value: float

    @property
    def key(self) -> tuple[str, str, int]:
        """Identity of the sample; also the primary key in the database."""
        return (self.station, self.signal, self.ts_us)


class SampleEncoder:
    """Turns simulator events into the samples a PLC would expose.

    Finished parts become cumulative counters, as on a real controller: a lost
    message never loses a part, because the next sample carries the running
    total, but the part is then counted in the window of that later sample.
    """

    def __init__(self) -> None:
        self._total: dict[str, int] = {}
        self._good: dict[str, int] = {}

    def encode(self, event: Event) -> list[Sample]:
        if event.kind is EventKind.STATE:
            return [Sample(event.station, SIGNAL_STATE, event.ts_us, event.value)]
        if event.kind is EventKind.IDEAL:
            return [Sample(event.station, SIGNAL_IDEAL, event.ts_us, event.value)]
        if event.kind is EventKind.HEARTBEAT:
            return [Sample(CELL_STATION, SIGNAL_HEARTBEAT, event.ts_us, event.value)]
        total = self._total.get(event.station, 0) + 1
        self._total[event.station] = total
        samples = [Sample(event.station, SIGNAL_TOTAL, event.ts_us, float(total))]
        if event.value > 0:
            good = self._good.get(event.station, 0) + 1
            self._good[event.station] = good
            samples.append(Sample(event.station, SIGNAL_GOOD, event.ts_us, float(good)))
        return samples


def expected_samples(events: Iterable[Event]) -> list[Sample]:
    """Every sample a lossless pipeline must end up storing for ``events``."""
    encoder = SampleEncoder()
    return [sample for event in events for sample in encoder.encode(event)]


def write_events(path: Path, events: Iterable[Event]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(event.to_json() + "\n")


def read_events(path: Path) -> Iterator[Event]:
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield Event.from_json(line)
