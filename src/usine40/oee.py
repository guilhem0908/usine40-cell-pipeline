"""OEE per station and per time window, computed from what the pipeline stored.

Definitions (Nakajima's availability x performance x quality decomposition;
ISO 22400-2 names the same three ratios availability, effectiveness and
quality ratio). For one station over one window:

* planned time   ``T_p = covered time - time in IDLE``
* run time       ``T_r = time in RUNNING``
* availability   ``A = T_r / T_p``
* performance    ``P = C_ideal * N / T_r``   (N parts finished, C_ideal ideal cycle)
* quality        ``Q = N_good / N``
* OEE            ``A * P * Q = C_ideal * N_good / T_p``

BLOCKED, STARVED and FAULT time all count against availability. Over several
windows the ratios are recomputed from summed durations and counts
(:func:`combine`); averaging per-window ratios would be wrong.

The inputs are the stored samples: state changes (the state holds until the
next change) and cumulative part counters (the count in a window is the sum of
the increments whose timestamps fall inside it).
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass

from usine40.model import State
from usine40.timebase import US_PER_S

PERCENT = 100.0


def ratio(numerator: float, denominator: float) -> float | None:
    """``numerator / denominator``, or None when the ratio is undefined."""
    return numerator / denominator if denominator > 0 else None


@dataclass(frozen=True, slots=True)
class WindowTotals:
    """Durations and counts of one station over ``[start_us, end_us)``.

    ``state_us`` is indexed by :class:`State`. Its sum can be shorter than the
    window when the window is only partly covered by data.
    """

    station: str
    start_us: int
    end_us: int
    state_us: tuple[int, ...]
    total: int
    good: int
    ideal_cycle_s: float

    @property
    def covered_us(self) -> int:
        return sum(self.state_us)

    @property
    def planned_us(self) -> int:
        return self.covered_us - self.state_us[State.IDLE]

    @property
    def run_us(self) -> int:
        return self.state_us[State.RUNNING]

    @property
    def availability(self) -> float | None:
        return ratio(self.run_us, self.planned_us)

    @property
    def performance(self) -> float | None:
        return ratio(self.ideal_cycle_s * self.total, self.run_us / US_PER_S)

    @property
    def quality(self) -> float | None:
        return ratio(self.good, self.total)

    @property
    def oee(self) -> float | None:
        factors = (self.availability, self.performance, self.quality)
        if any(factor is None for factor in factors):
            return None
        return factors[0] * factors[1] * factors[2]


def combine(windows: Sequence[WindowTotals]) -> WindowTotals:
    """Sum the durations and counts of several windows of the same station."""
    if not windows:
        raise ValueError("Cannot combine zero windows.")
    stations = {window.station for window in windows}
    ideals = {window.ideal_cycle_s for window in windows}
    if len(stations) != 1 or len(ideals) != 1:
        raise ValueError("Windows must share one station and one ideal cycle time.")
    return WindowTotals(
        station=windows[0].station,
        start_us=min(window.start_us for window in windows),
        end_us=max(window.end_us for window in windows),
        state_us=tuple(sum(window.state_us[state] for window in windows) for state in State),
        total=sum(window.total for window in windows),
        good=sum(window.good for window in windows),
        ideal_cycle_s=windows[0].ideal_cycle_s,
    )


def window_bounds(start_us: int, end_us: int, window_us: int) -> Iterator[tuple[int, int]]:
    """Windows aligned on multiples of ``window_us`` that overlap ``[start_us, end_us)``."""
    if window_us <= 0:
        raise ValueError("window_us must be positive.")
    window_start = start_us - start_us % window_us
    while window_start < end_us:
        yield window_start, window_start + window_us
        window_start += window_us


class Series:
    """Time-sorted ``(ts_us, value)`` points of one signal with bisect lookups."""

    def __init__(self, points: Iterable[tuple[int, float]]) -> None:
        ordered = sorted(points)
        self.ts = [ts for ts, _ in ordered]
        self.values = [value for _, value in ordered]

    def __len__(self) -> int:
        return len(self.ts)

    def last_value(self) -> float | None:
        return self.values[-1] if self.values else None

    def durations_by_state(self, start_us: int, end_us: int) -> tuple[int, ...]:
        """Time spent in each state inside ``[start_us, end_us)``.

        The state at ``t`` is the value of the last point at or before ``t``;
        time before the first point is not attributed to any state.
        """
        durations = [0] * len(State)
        index = max(bisect_right(self.ts, start_us) - 1, 0)
        while index < len(self.ts) and self.ts[index] < end_us:
            begin = max(self.ts[index], start_us)
            finish = self.ts[index + 1] if index + 1 < len(self.ts) else end_us
            durations[int(self.values[index])] += max(min(finish, end_us) - begin, 0)
            index += 1
        return tuple(durations)

    def counter_increase(self, start_us: int, end_us: int) -> int:
        """Increase of a cumulative counter over points stamped in ``[start_us, end_us)``.

        A value lower than its predecessor is a counter reset (controller
        restart) and counts from zero. A point without a predecessor has no
        baseline and contributes nothing.
        """
        low, high = bisect_left(self.ts, start_us), bisect_left(self.ts, end_us)
        increase = 0.0
        for index in range(max(low, 1), high):
            current, previous = self.values[index], self.values[index - 1]
            increase += current - previous if current >= previous else current
        return round(increase)


def totals_from_series(
    station: str,
    states: Series,
    total_count: Series,
    good_count: Series,
    ideal_cycle_s: float,
    start_us: int,
    end_us: int,
) -> WindowTotals:
    """Durations and counts of ``station`` over ``[start_us, end_us)`` from stored samples."""
    return WindowTotals(
        station=station,
        start_us=start_us,
        end_us=end_us,
        state_us=states.durations_by_state(start_us, end_us),
        total=total_count.counter_increase(start_us, end_us),
        good=good_count.counter_increase(start_us, end_us),
        ideal_cycle_s=ideal_cycle_s,
    )
