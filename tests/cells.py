"""Cell configurations and small helpers shared by the tests."""

from __future__ import annotations

from usine40.model import Event, EventKind, State
from usine40.sim import CellConfig, CellSimulator, FaultWindow, PlannedStop, StationConfig

SCAN_S = 0.05


def steady_cell(
    scripted_faults: tuple[FaultWindow, ...] = (),
    planned_stops: tuple[PlannedStop, ...] = (),
) -> CellConfig:
    """Three stations with no randomness at all: no slowdown, scrap or breakdown."""
    return CellConfig(
        stations=(
            StationConfig("infeed", 1.0),
            StationConfig("machining", 2.0),
            StationConfig("inspection", 1.0),
        ),
        buffer_capacity=(2, 2),
        scan_period_s=SCAN_S,
        scripted_faults=scripted_faults,
        planned_stops=planned_stops,
    )


def run_scans(
    config: CellConfig, seconds: float, epoch_us: int = 0
) -> tuple[CellSimulator, list[Event]]:
    simulator = CellSimulator(config, epoch_us)
    events: list[Event] = []
    for _ in range(round(seconds / config.scan_period_s)):
        events.extend(simulator.step())
    return simulator, events


def state_changes(events: list[Event], station: str) -> list[tuple[float, State]]:
    """(seconds, state) for each state change of one station."""
    return [
        (event.ts_us / 1e6, State(int(event.value)))
        for event in events
        if event.kind is EventKind.STATE and event.station == station
    ]
