"""Comparison of what the pipeline stored with the simulator's ground truth."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from usine40.model import Event, Sample, expected_samples
from usine40.oee import PERCENT, WindowTotals, combine
from usine40.timebase import US_PER_S

METRICS = ("availability", "performance", "quality", "oee")


@dataclass(frozen=True, slots=True)
class MetricError:
    """Absolute error of one ratio over the compared windows, in percentage points."""

    windows: int
    max_pp: float
    mean_pp: float


@dataclass(frozen=True, slots=True)
class Comparison:
    matched: int
    truth_only: int
    pipeline_only: int
    count_mismatches: int
    errors: dict[str, MetricError]


def compare_windows(truth: Iterable[WindowTotals], pipeline: Iterable[WindowTotals]) -> Comparison:
    """Match windows on (station, start) and measure the error of each ratio.

    A ratio enters the statistics for a window only when it is defined on both
    sides; a window where exactly one side is undefined counts as a full
    100-point error, so that missing data cannot hide behind a skipped window.
    """
    truth_by_key = {(w.station, w.start_us): w for w in truth}
    pipeline_by_key = {(w.station, w.start_us): w for w in pipeline}
    shared = sorted(truth_by_key.keys() & pipeline_by_key.keys())
    deltas: dict[str, list[float]] = {metric: [] for metric in METRICS}
    count_mismatches = 0
    for key in shared:
        reference, measured = truth_by_key[key], pipeline_by_key[key]
        if (reference.total, reference.good) != (measured.total, measured.good):
            count_mismatches += 1
        for metric in METRICS:
            expected, actual = getattr(reference, metric), getattr(measured, metric)
            if expected is None and actual is None:
                continue
            if expected is None or actual is None:
                deltas[metric].append(PERCENT)
            else:
                deltas[metric].append(abs(expected - actual) * PERCENT)
    errors = {
        metric: MetricError(
            windows=len(values),
            max_pp=max(values, default=0.0),
            mean_pp=sum(values) / len(values) if values else 0.0,
        )
        for metric, values in deltas.items()
    }
    return Comparison(
        matched=len(shared),
        truth_only=len(truth_by_key.keys() - pipeline_by_key.keys()),
        pipeline_only=len(pipeline_by_key.keys() - truth_by_key.keys()),
        count_mismatches=count_mismatches,
        errors=errors,
    )


def missing_samples(events: Iterable[Event], stored: set[tuple[str, str, int]]) -> list[Sample]:
    """Samples the simulator produced that the store does not contain."""
    return [sample for sample in expected_samples(events) if sample.key not in stored]


def summarize(windows: Sequence[WindowTotals]) -> dict[str, float | int | None]:
    """Totals and ratios of one station over a set of windows, ready for JSON."""
    total = combine(windows)
    return {
        "windows": len(windows),
        "planned_s": total.planned_us / US_PER_S,
        "run_s": total.run_us / US_PER_S,
        "parts": total.total,
        "good_parts": total.good,
        "availability": total.availability,
        "performance": total.performance,
        "quality": total.quality,
        "oee": total.oee,
    }
