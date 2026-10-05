"""Reference OEE totals computed straight from the simulator's event log.

This is deliberately a second implementation, working on a different
representation than :mod:`usine40.oee`: it walks the simulator's own events
(state changes and individual finished parts) and spreads each state interval
over the windows it crosses, whereas the pipeline slices stored samples per
window and differentiates cumulative counters. Agreement between the two is
the end-to-end check of the pipeline.
"""

from __future__ import annotations

from collections.abc import Iterable

from usine40.model import CELL_STATION, Event, EventKind, State
from usine40.oee import WindowTotals


class _Accumulator:
    def __init__(self) -> None:
        self.state_us = [0] * len(State)
        self.total = 0
        self.good = 0


def truth_windows(
    events: Iterable[Event], window_us: int, end_us: int
) -> dict[str, list[WindowTotals]]:
    """Per-station window totals from simulator events, up to ``end_us`` (exclusive).

    Windows are aligned on multiples of ``window_us`` like the pipeline's. The
    last state of each station is extended to ``end_us``.
    """
    if window_us <= 0:
        raise ValueError("window_us must be positive.")
    changes: dict[str, list[tuple[int, int]]] = {}
    parts: dict[str, list[tuple[int, bool]]] = {}
    ideal: dict[str, float] = {}
    for event in events:
        if event.station == CELL_STATION or event.ts_us >= end_us:
            continue
        if event.kind is EventKind.STATE:
            changes.setdefault(event.station, []).append((event.ts_us, int(event.value)))
        elif event.kind is EventKind.PART:
            parts.setdefault(event.station, []).append((event.ts_us, event.value > 0))
        elif event.kind is EventKind.IDEAL:
            ideal[event.station] = event.value

    result: dict[str, list[WindowTotals]] = {}
    for station, station_changes in changes.items():
        windows: dict[int, _Accumulator] = {}
        station_changes.sort()
        boundaries = [ts for ts, _ in station_changes[1:]] + [end_us]
        for (begin, state), finish in zip(station_changes, boundaries, strict=True):
            window_start = begin - begin % window_us
            while window_start < finish:
                overlap = min(finish, window_start + window_us) - max(begin, window_start)
                windows.setdefault(window_start, _Accumulator()).state_us[state] += overlap
                window_start += window_us
        for ts_us, good in parts.get(station, ()):
            accumulator = windows.setdefault(ts_us - ts_us % window_us, _Accumulator())
            accumulator.total += 1
            accumulator.good += int(good)
        result[station] = [
            WindowTotals(
                station=station,
                start_us=window_start,
                end_us=window_start + window_us,
                state_us=tuple(accumulator.state_us),
                total=accumulator.total,
                good=accumulator.good,
                ideal_cycle_s=ideal[station],
            )
            for window_start, accumulator in sorted(windows.items())
        ]
    return result
