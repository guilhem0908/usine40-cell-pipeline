"""Deterministic experiments on the in-process pipeline (no Docker).

Two questions are answered with the real OPC UA server, gateway and collector
wired to the broker substitute and SQLite, in simulated time:

1. Does the OEE the pipeline stores equal the ground truth computed from the
   simulator's own event log, window by window, over many seeds?
2. What is lost, duplicated or miscounted when the gateway restarts or the
   broker goes away, with and without the protections (history replay, QoS 1)?

Everything here depends only on the seeds, so the output files are identical
on every machine. Broker timings and real Mosquitto behaviour are measured
separately by ``compose_scenario.py``.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from usine40.model import expected_samples
from usine40.rig import Rig
from usine40.sim import PlannedStop, default_cell
from usine40.validation import METRICS, summarize

OEE_SEEDS = tuple(range(1, 11))
OEE_MINUTES = 60
PLANNED_STOP = PlannedStop(start_s=1200.0, duration_s=300.0)

FAULT_SEEDS = tuple(range(1, 6))
WARM_UP_S = 240.0
OUTAGE_S = 60.0
RECOVERY_S = 180.0
REDELIVERED = 8


@dataclass(frozen=True, slots=True)
class FaultScenario:
    key: str
    label: str
    qos: int
    backfill_s: float
    disturb: Callable[[Rig], Awaitable[None]]


async def _gateway_restart(rig: Rig) -> None:
    await rig.stop_gateway()
    await rig.advance(OUTAGE_S)
    await rig.start_gateway()


async def _broker_outage(rig: Rig, redeliver: int = 0) -> None:
    rig.broker.stop()
    await rig.advance(OUTAGE_S)
    rig.broker.start(redeliver=redeliver)


async def _broker_outage_with_redelivery(rig: Rig) -> None:
    await _broker_outage(rig, redeliver=REDELIVERED)


FAULT_SCENARIOS = (
    FaultScenario("gateway_no_replay", "Gateway down 60 s, no replay", 1, 0.0, _gateway_restart),
    FaultScenario("gateway_replay_30", "Gateway down 60 s, 30 s replay", 1, 30.0, _gateway_restart),
    FaultScenario(
        "gateway_replay_120", "Gateway down 60 s, 120 s replay", 1, 120.0, _gateway_restart
    ),
    FaultScenario("broker_qos0", "Broker down 60 s, QoS 0", 0, 0.0, _broker_outage),
    FaultScenario("broker_qos1", "Broker down 60 s, QoS 1", 1, 0.0, _broker_outage),
    FaultScenario(
        "broker_qos1_redelivery",
        f"Broker down 60 s, QoS 1, {REDELIVERED} messages redelivered",
        1,
        0.0,
        _broker_outage_with_redelivery,
    ),
)


async def _validate_seed(seed: int) -> dict:
    config = default_cell(seed, planned_stops=(PLANNED_STOP,))
    async with Rig(config) as rig:
        await rig.advance(OEE_MINUTES * 60.0)
        comparison = rig.compare()
        truth, pipeline = rig.truth(), rig.pipeline()
        stations = {}
        for station in config.station_names:
            stations[station] = {
                "truth": [w for w in truth if w.station == station],
                "pipeline": [w for w in pipeline if w.station == station],
            }
        return {
            "seed": seed,
            "comparison": comparison,
            "stations": stations,
            "stored": len(rig.store.sample_keys()),
            "missing": rig.missing(),
        }


async def _run_fault_scenario(scenario: FaultScenario, seed: int) -> dict:
    async with Rig(default_cell(seed), qos=scenario.qos, backfill_s=scenario.backfill_s) as rig:
        await rig.advance(WARM_UP_S)
        await scenario.disturb(rig)
        await rig.advance(RECOVERY_S)
        comparison = rig.compare()
        truth, pipeline = rig.truth(), rig.pipeline()
        sessions = rig.collector.session_stats()
        return {
            "scenario": scenario.key,
            "seed": seed,
            "produced": len(
                expected_samples(e for e in rig.events if e.ts_us <= rig.watermark_us())
            ),
            "missing": rig.missing(),
            "wire_missing": sum(s.missing for s in sessions),
            "wire_duplicates": sum(s.duplicates for s in sessions),
            "windows": comparison.matched,
            "wrong_windows": comparison.wrong_windows,
            "wrong_windows_right_counts": comparison.wrong_windows_right_counts,
            "wrong_part_count_windows": comparison.count_mismatches,
            "worst_oee_error_pp": comparison.errors["oee"].max_pp,
            "worst_availability_error_pp": comparison.errors["availability"].max_pp,
            "parts_truth": sum(w.total for w in truth),
            "parts_pipeline": sum(w.total for w in pipeline),
        }


def run_oee_validation(out_dir: Path) -> dict:
    """Experiment 1: pipeline OEE against ground truth, per seed and station."""
    runs = [asyncio.run(_validate_seed(seed)) for seed in OEE_SEEDS]
    rows = []
    for run in runs:
        for station, sides in run["stations"].items():
            reference, measured = summarize(sides["truth"]), summarize(sides["pipeline"])
            row = {"seed": run["seed"], "station": station, "windows": reference["windows"]}
            for name in ("parts", "good_parts", *METRICS):
                row[f"truth_{name}"] = reference[name]
                row[f"pipeline_{name}"] = measured[name]
            rows.append(row)
    _write_csv(out_dir / "inprocess_oee.csv", rows)

    stations = {}
    for station in runs[0]["stations"]:
        merged = {
            side: summarize([window for run in runs for window in run["stations"][station][side]])
            for side in ("truth", "pipeline")
        }
        stations[station] = merged
    errors = {
        metric: {
            "max_pp": max(run["comparison"].errors[metric].max_pp for run in runs),
            "mean_pp": sum(
                run["comparison"].errors[metric].mean_pp * run["comparison"].errors[metric].windows
                for run in runs
            )
            / max(sum(run["comparison"].errors[metric].windows for run in runs), 1),
        }
        for metric in METRICS
    }
    return {
        "seeds": list(OEE_SEEDS),
        "simulated_minutes_per_seed": OEE_MINUTES,
        "window_s": 30,
        "station_windows": sum(run["comparison"].matched for run in runs),
        "unmatched_windows": sum(
            run["comparison"].truth_only + run["comparison"].pipeline_only for run in runs
        ),
        "windows_with_wrong_part_count": sum(run["comparison"].count_mismatches for run in runs),
        "samples_stored": sum(run["stored"] for run in runs),
        "samples_missing": sum(run["missing"] for run in runs),
        "error_pp": errors,
        "stations": stations,
    }


def run_fault_scenarios(out_dir: Path) -> list[dict]:
    """Experiment 2: gateway restarts and broker outages on the substitute broker."""
    rows = [
        asyncio.run(_run_fault_scenario(scenario, seed))
        for scenario in FAULT_SCENARIOS
        for seed in FAULT_SEEDS
    ]
    _write_csv(out_dir / "inprocess_faults.csv", rows)
    summary = []
    for scenario in FAULT_SCENARIOS:
        runs = [row for row in rows if row["scenario"] == scenario.key]
        summary.append(
            {
                "scenario": scenario.key,
                "label": scenario.label,
                "seeds": list(FAULT_SEEDS),
                "produced": sum(run["produced"] for run in runs),
                "missing": sum(run["missing"] for run in runs),
                "wire_missing": sum(run["wire_missing"] for run in runs),
                "wire_duplicates": sum(run["wire_duplicates"] for run in runs),
                "windows": sum(run["windows"] for run in runs),
                "wrong_windows": sum(run["wrong_windows"] for run in runs),
                "wrong_windows_right_counts": sum(
                    run["wrong_windows_right_counts"] for run in runs
                ),
                "wrong_part_count_windows": sum(run["wrong_part_count_windows"] for run in runs),
                "worst_availability_error_pp": max(
                    run["worst_availability_error_pp"] for run in runs
                ),
                "worst_oee_error_pp": max(run["worst_oee_error_pp"] for run in runs),
                "parts_truth": sum(run["parts_truth"] for run in runs),
                "parts_pipeline": sum(run["parts_pipeline"] for run in runs),
            }
        )
    return summary


def run_all(out_dir: Path) -> dict:
    logging.getLogger("asyncua").setLevel(logging.ERROR)
    out_dir.mkdir(parents=True, exist_ok=True)
    results = {
        "oee_validation": run_oee_validation(out_dir),
        "fault_scenarios": run_fault_scenarios(out_dir),
    }
    text = json.dumps(results, indent=2) + "\n"
    (out_dir / "inprocess.json").write_text(text, encoding="utf-8", newline="\n")
    return results


def _write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
