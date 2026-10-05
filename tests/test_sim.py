"""Behaviour of the cell simulator: determinism, conservation and fault handling."""

from __future__ import annotations

import math

import pytest

from usine40.model import EventKind, State
from usine40.sim import (
    MAX_INJECTED_FAULT_S,
    CellConfig,
    CellSimulator,
    FaultWindow,
    PlannedStop,
    StationConfig,
    default_cell,
)

from cells import SCAN_S, run_scans, state_changes, steady_cell


def test_same_seed_reproduces_the_event_log_exactly():
    _, first = run_scans(default_cell(seed=7), 600)
    _, second = run_scans(default_cell(seed=7), 600)
    assert first == second


def test_different_seeds_give_different_runs():
    _, first = run_scans(default_cell(seed=1), 600)
    _, second = run_scans(default_cell(seed=2), 600)
    assert first != second


@pytest.mark.parametrize("seed", [0, 1, 2, 3])
def test_parts_are_conserved_along_the_line(seed):
    simulator, _ = run_scans(default_cell(seed=seed), 1200)
    names = simulator.config.station_names
    for index, name in enumerate(names):
        counters = simulator.counters(name)
        assert counters["taken"] == counters["total"] + counters["in_station"]
        assert counters["good"] <= counters["total"]
        if index + 1 < len(names):
            downstream = simulator.counters(names[index + 1])
            assert counters["good"] == downstream["taken"] + simulator.buffers[index]


def test_buffers_stay_within_their_capacity_on_every_scan():
    config = default_cell(seed=5)
    simulator = CellSimulator(config)
    for _ in range(20_000):
        simulator.step()
        for level, capacity in zip(simulator.buffers, config.buffer_capacity, strict=True):
            assert 0 <= level <= capacity


def test_every_scan_is_spent_in_exactly_one_state():
    simulator, _ = run_scans(default_cell(seed=3), 900)
    for name in simulator.config.station_names:
        assert sum(simulator.scans_in_state(name)) == simulator.scan_index


def test_event_timestamps_fall_on_the_scan_grid():
    epoch_us = 1_700_000_000_000_000
    _, events = run_scans(default_cell(seed=1), 120, epoch_us)
    period_us = round(SCAN_S * 1e6)
    assert all((event.ts_us - epoch_us) % period_us == 0 for event in events)
    assert [event.ts_us for event in events] == sorted(event.ts_us for event in events)


def test_steady_line_produces_at_the_bottleneck_rate():
    simulator, _ = run_scans(steady_cell(), 600)
    bottleneck_cycle_s = 2.0
    produced = simulator.counters("inspection")["total"]
    assert produced == pytest.approx(600 / bottleneck_cycle_s, abs=2)


def test_fast_station_upstream_of_the_bottleneck_gets_blocked_and_downstream_starved():
    simulator, _ = run_scans(steady_cell(), 600)
    assert simulator.scans_in_state("infeed")[State.BLOCKED] > 0
    assert simulator.scans_in_state("inspection")[State.STARVED] > 0
    assert simulator.scans_in_state("machining")[State.BLOCKED] == 0


def test_scripted_fault_lasts_exactly_its_duration():
    config = steady_cell(scripted_faults=(FaultWindow("machining", 30.0, 12.5),))
    _, events = run_scans(config, 120)
    changes = state_changes(events, "machining")
    fault_index = next(i for i, (_, state) in enumerate(changes) if state is State.FAULT)
    assert changes[fault_index][0] == pytest.approx(30.0)
    assert changes[fault_index + 1][0] == pytest.approx(42.5)
    assert sum(state is State.FAULT for _, state in changes) == 1


def test_breakdown_of_the_bottleneck_blocks_upstream_and_starves_downstream():
    config = steady_cell(scripted_faults=(FaultWindow("machining", 30.0, 30.0),))
    _, events = run_scans(config, 60)
    assert state_changes(events, "infeed")[-1][1] is State.BLOCKED
    assert state_changes(events, "inspection")[-1][1] is State.STARVED
    assert state_changes(events, "machining")[-1] == (pytest.approx(30.0), State.FAULT)


def test_planned_stop_idles_every_station_and_freezes_production():
    config = steady_cell(planned_stops=(PlannedStop(20.0, 10.0),))
    simulator, events = run_scans(config, 60)
    idle_scans = round(10.0 / SCAN_S)
    for name in config.station_names:
        assert simulator.scans_in_state(name)[State.IDLE] == idle_scans
    part_times = [event.ts_us / 1e6 for event in events if event.kind is EventKind.PART]
    assert not [moment for moment in part_times if 20.0 < moment < 30.0]


def test_injected_fault_is_clamped_to_the_safety_limit():
    simulator = CellSimulator(steady_cell())
    assert simulator.inject_fault("machining", 1e9) == pytest.approx(MAX_INJECTED_FAULT_S)
    assert simulator.inject_fault("machining", -5.0) == pytest.approx(SCAN_S)
    with pytest.raises(ValueError, match="Unknown station"):
        simulator.inject_fault("lathe", 5.0)
    with pytest.raises(ValueError, match="finite"):
        simulator.inject_fault("machining", math.nan)


def test_injected_fault_starts_on_the_next_scan():
    simulator = CellSimulator(steady_cell())
    for _ in range(100):
        simulator.step()
    accepted = simulator.inject_fault("infeed", 3.0)
    events = [event for _ in range(200) for event in simulator.step()]
    changes = state_changes(events, "infeed")
    assert changes[0] == (pytest.approx(100 * SCAN_S), State.FAULT)
    assert changes[1][0] == pytest.approx(100 * SCAN_S + accepted)


def test_station_never_beats_its_ideal_cycle():
    simulator, _ = run_scans(default_cell(seed=11), 3600)
    for station in simulator.config.stations:
        run_s = simulator.scans_in_state(station.name)[State.RUNNING] * SCAN_S
        finished = simulator.counters(station.name)["total"]
        assert finished * station.ideal_cycle_s <= run_s + 1e-9


def test_random_breakdowns_follow_the_configured_mtbf_and_mttr():
    station = StationConfig("solo", 1.0, mtbf_s=100.0, mttr_s=10.0)
    config = CellConfig(stations=(station,), buffer_capacity=(), seed=4)
    simulator, events = run_scans(config, 200_000)
    faults = sum(state is State.FAULT for _, state in state_changes(events, "solo"))
    run_s = simulator.scans_in_state("solo")[State.RUNNING] * SCAN_S
    fault_s = simulator.scans_in_state("solo")[State.FAULT] * SCAN_S
    assert run_s / faults == pytest.approx(100.0, rel=0.1)
    assert fault_s / faults == pytest.approx(10.0, rel=0.1)


def test_invalid_configurations_are_rejected():
    station = StationConfig("a", 1.0)
    with pytest.raises(ValueError, match="one buffer"):
        CellConfig(stations=(station, StationConfig("b", 1.0)), buffer_capacity=())
    with pytest.raises(ValueError, match="unique"):
        CellConfig(stations=(station, station), buffer_capacity=(1,))
    with pytest.raises(ValueError, match="unknown station"):
        CellConfig((station,), (), scripted_faults=(FaultWindow("zzz", 0.0, 1.0),))
