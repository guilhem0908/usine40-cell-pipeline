"""OEE arithmetic on stored samples, and its agreement with the ground truth."""

from __future__ import annotations

from itertools import pairwise

import pytest

from usine40.ground_truth import truth_windows
from usine40.model import (
    SIGNAL_GOOD,
    SIGNAL_IDEAL,
    SIGNAL_STATE,
    SIGNAL_TOTAL,
    Event,
    EventKind,
    State,
    expected_samples,
)
from usine40.oee import (
    Series,
    WindowTotals,
    combine,
    ratio,
    totals_from_series,
    window_bounds,
)
from usine40.sim import PlannedStop, default_cell
from usine40.validation import compare_windows

from cells import SCAN_S, run_scans, steady_cell

S = 1_000_000
WINDOW_US = 30 * S


def _series_by_signal(events: list[Event], station: str) -> dict[str, Series]:
    samples = [sample for sample in expected_samples(events) if sample.station == station]
    signals = (SIGNAL_STATE, SIGNAL_TOTAL, SIGNAL_GOOD, SIGNAL_IDEAL)
    return {
        signal: Series((s.ts_us, s.value) for s in samples if s.signal == signal)
        for signal in signals
    }


def _pipeline_windows(events: list[Event], station: str, end_us: int) -> list[WindowTotals]:
    """What the collector computes, minus the transport: stored-sample arithmetic."""
    series = _series_by_signal(events, station)
    counters = {
        # Counters start at zero when the controller starts, as the PLC publishes them.
        signal: Series([(0, 0.0), *zip(series[signal].ts, series[signal].values, strict=True)])
        for signal in (SIGNAL_TOTAL, SIGNAL_GOOD)
    }
    return [
        totals_from_series(
            station,
            series[SIGNAL_STATE],
            counters[SIGNAL_TOTAL],
            counters[SIGNAL_GOOD],
            series[SIGNAL_IDEAL].last_value(),
            start,
            min(end, end_us),
        )
        for start, end in window_bounds(0, end_us, WINDOW_US)
    ]


def _counter(parts: int, every_s: int) -> Series:
    """Cumulative counter starting at zero, +1 every ``every_s`` seconds."""
    return Series([(0, 0.0), *((n * every_s * S, float(n)) for n in range(1, parts + 1))])


def test_hand_computed_window():
    states = Series([(0, State.RUNNING), (40 * S, State.FAULT), (50 * S, State.RUNNING)])
    total = _counter(parts=22, every_s=2)
    good = _counter(parts=20, every_s=2)
    window = totals_from_series("machining", states, total, good, 2.0, 0, 60 * S)

    assert window.total == 22
    assert window.good == 20
    assert window.availability == pytest.approx(50 / 60)
    assert window.performance == pytest.approx(2.0 * 22 / 50)
    assert window.quality == pytest.approx(20 / 22)
    assert window.oee == pytest.approx(2.0 * 20 / 60)


def test_idle_time_is_excluded_from_planned_time():
    states = Series([(0, State.RUNNING), (20 * S, State.IDLE), (30 * S, State.RUNNING)])
    window = totals_from_series("s", states, Series([]), Series([]), 1.0, 0, 60 * S)
    assert window.covered_us == 60 * S
    assert window.planned_us == 50 * S
    assert window.availability == pytest.approx(1.0)


def test_blocked_starved_and_fault_all_count_against_availability():
    sequence = (State.RUNNING, State.BLOCKED, State.STARVED, State.FAULT)
    states = Series(zip((0, 30 * S, 40 * S, 50 * S), sequence, strict=True))
    window = totals_from_series("s", states, Series([]), Series([]), 1.0, 0, 60 * S)
    assert window.state_us == (0, 30 * S, 10 * S, 10 * S, 10 * S)
    assert window.availability == pytest.approx(0.5)


def test_time_before_the_first_state_sample_is_not_covered():
    states = Series([(45 * S, State.RUNNING)])
    window = totals_from_series("s", states, Series([]), Series([]), 1.0, 30 * S, 60 * S)
    assert window.covered_us == 15 * S


def test_time_at_a_value_that_is_not_a_state_is_not_attributed_to_any_state():
    points = [(0, 1.0), (10 * S, 9.0), (20 * S, -1.0), (30 * S, 2.5), (40 * S, 1.0)]
    assert Series(points).durations_by_state(0, 50 * S) == (0, 20 * S, 0, 0, 0)


def test_state_before_the_window_carries_into_it():
    states = Series([(5 * S, State.FAULT), (70 * S, State.RUNNING)])
    window = totals_from_series("s", states, Series([]), Series([]), 1.0, 30 * S, 60 * S)
    assert window.state_us[State.FAULT] == 30 * S


def test_counter_increase_only_counts_samples_stamped_inside_the_window():
    counter = Series([(0, 0.0), (10 * S, 1.0), (30 * S, 2.0), (59 * S, 3.0), (60 * S, 4.0)])
    assert counter.counter_increase(30 * S, 60 * S) == 2
    assert counter.counter_increase(0, 30 * S) == 1
    assert counter.counter_increase(60 * S, 90 * S) == 1


def test_counter_reset_counts_from_zero():
    counter = Series([(0, 0.0), (10 * S, 41.0), (20 * S, 42.0), (30 * S, 2.0), (40 * S, 3.0)])
    assert counter.counter_increase(0, 60 * S) == 42 + 2 + 1


def test_first_counter_sample_without_baseline_contributes_nothing():
    counter = Series([(10 * S, 500.0), (20 * S, 501.0)])
    assert counter.counter_increase(0, 60 * S) == 1


def test_lost_counter_samples_do_not_lose_parts():
    complete = Series([(0, 0.0)] + [(n * S, float(n)) for n in range(1, 21)])
    thinned = Series([(0, 0.0), (1 * S, 1.0), (20 * S, 20.0)])
    assert thinned.counter_increase(0, 60 * S) == complete.counter_increase(0, 60 * S) == 20


def test_ratios_are_undefined_rather_than_zero_without_data():
    empty = WindowTotals("s", 0, 60 * S, (60 * S, 0, 0, 0, 0), 0, 0, 1.0)
    assert ratio(1.0, 0.0) is None
    assert empty.availability is None
    assert empty.performance is None
    assert empty.quality is None
    assert empty.oee is None


def test_window_bounds_are_aligned_and_cover_the_range():
    bounds = list(window_bounds(95 * S, 200 * S, WINDOW_US))
    assert bounds[0] == (90 * S, 120 * S)
    assert bounds[-1] == (180 * S, 210 * S)
    assert all(end - start == WINDOW_US for start, end in bounds)
    assert all(first[1] == second[0] for first, second in pairwise(bounds))
    with pytest.raises(ValueError, match="positive"):
        list(window_bounds(0, 10, 0))


def test_combining_windows_sums_durations_and_counts_instead_of_averaging_ratios():
    busy = WindowTotals("s", 0, 30 * S, (0, 30 * S, 0, 0, 0), 30, 30, 1.0)
    down = WindowTotals("s", 30 * S, 60 * S, (0, 3 * S, 0, 0, 27 * S), 1, 0, 1.0)
    whole = combine([busy, down])
    assert whole.availability == pytest.approx(33 / 60)
    assert whole.quality == pytest.approx(30 / 31)
    mean_of_ratios = (busy.quality + down.quality) / 2
    assert whole.quality != pytest.approx(mean_of_ratios)
    with pytest.raises(ValueError, match="one station"):
        combine([busy, WindowTotals("other", 0, 30 * S, (0, 0, 0, 0, 0), 0, 0, 1.0)])
    with pytest.raises(ValueError, match="zero windows"):
        combine([])


def test_a_window_is_wrong_when_its_oee_differs_or_is_undefined_on_one_side_only():
    def window(index: int, running_s: int, parts: int) -> WindowTotals:
        state_us = (0, running_s * S, 0, 0, 0)
        return WindowTotals("s", index * 30 * S, (index + 1) * 30 * S, state_us, parts, parts, 2.0)

    truth = [window(0, 30, 15), window(1, 30, 15), window(2, 30, 15), window(3, 0, 0)]
    pipeline = [window(0, 30, 15), window(1, 15, 15), window(2, 0, 0), window(3, 0, 0)]
    comparison = compare_windows(truth, pipeline)
    assert comparison.matched == 4
    assert comparison.wrong_windows == 2
    assert comparison.errors["oee"].windows == 3
    assert comparison.errors["oee"].max_pp == pytest.approx(100.0)


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_oee_equals_ideal_cycle_times_good_parts_over_planned_time(seed):
    _, events = run_scans(default_cell(seed=seed), 900)
    for station in ("infeed", "machining", "inspection"):
        for window in _pipeline_windows(events, station, 900 * S):
            if window.oee is None:
                continue
            identity = window.ideal_cycle_s * window.good / (window.planned_us / S)
            assert window.oee == pytest.approx(identity, rel=1e-12)


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_stored_sample_arithmetic_agrees_with_the_event_log_ground_truth(seed):
    stops = (PlannedStop(200.0, 45.0),)
    _, events = run_scans(default_cell(seed=seed, planned_stops=stops), 1000)
    end_us = 1000 * S - 7 * S
    truth = truth_windows(events, WINDOW_US, end_us)
    for station in ("infeed", "machining", "inspection"):
        measured = _pipeline_windows(events, station, end_us)
        assert [(w.state_us, w.total, w.good) for w in measured] == [
            (w.state_us, w.total, w.good) for w in truth[station]
        ]


def test_ground_truth_agrees_with_the_simulator_scan_accounting():
    simulator, events = run_scans(default_cell(seed=9), 1234)
    end_us = simulator.time_us
    truth = truth_windows(events, WINDOW_US, end_us)
    scan_us = round(SCAN_S * S)
    for station in simulator.config.station_names:
        whole = combine(truth[station])
        expected = tuple(scans * scan_us for scans in simulator.scans_in_state(station))
        assert whole.state_us == expected
        assert whole.total == simulator.counters(station)["total"]
        assert whole.good == simulator.counters(station)["good"]


def test_ground_truth_spreads_an_interval_over_the_windows_it_crosses():
    events = [
        Event(0, "s", EventKind.IDEAL, 1.0),
        Event(10 * S, "s", EventKind.STATE, float(State.RUNNING)),
        Event(50 * S, "s", EventKind.STATE, float(State.FAULT)),
        Event(29 * S, "s", EventKind.PART, 1.0),
        Event(30 * S, "s", EventKind.PART, 0.0),
    ]
    first, second, third = truth_windows(events, WINDOW_US, 75 * S)["s"]
    assert first.state_us[State.RUNNING] == 20 * S
    assert (first.total, first.good) == (1, 1)
    assert second.state_us[State.RUNNING] == 20 * S
    assert second.state_us[State.FAULT] == 10 * S
    assert (second.total, second.good) == (1, 0)
    assert third.state_us[State.FAULT] == 15 * S
    assert third.covered_us == 15 * S


def test_steady_cell_bottleneck_runs_at_full_oee_once_it_has_been_fed():
    simulator, events = run_scans(steady_cell(), 600)
    after_start_up = truth_windows(events, WINDOW_US, simulator.time_us)["machining"][1:]
    machining = combine(after_start_up)
    assert machining.availability == pytest.approx(1.0)
    assert machining.performance == pytest.approx(1.0, abs=0.01)
    assert machining.quality == pytest.approx(1.0)
