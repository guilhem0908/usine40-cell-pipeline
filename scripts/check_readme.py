"""Fail when a number in the README differs from the committed results.

    python scripts/check_readme.py           # check the README against results/
    python scripts/check_readme.py --print   # print the blocks the README must contain

Every table and every figure quoted in the text is rendered here from
``results/*.json`` and has to appear in ``README.md`` verbatim. Change a
result, or edit a number by hand, and the corresponding block is no longer
found. The README is written from the output of ``--print``, so the numbers
are never typed twice.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEPARATORS = {"left": ":---", "right": "---:"}


def percent(value: float) -> str:
    return f"{value * 100:.2f} %"


def count_share(count: int, whole: int) -> str:
    return f"{count:,} ({count / whole * 100:.1f} %)" if whole else f"{count:,}"


def table(header: list[str], rows: list[list[str]], aligns: list[str]) -> str:
    """A GitHub-flavoured Markdown table."""
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(SEPARATORS[align] for align in aligns) + " |",
    ]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def oee_agreement(results: dict) -> dict[str, str]:
    validation = results["oee_validation"]
    rows = []
    for station, sides in validation["stations"].items():
        truth, pipeline = sides["truth"], sides["pipeline"]
        rows.append(
            [
                station,
                f"{truth['parts']:,}",
                f"{truth['good_parts']:,}",
                percent(truth["availability"]),
                percent(truth["performance"]),
                percent(truth["quality"]),
                percent(truth["oee"]),
                percent(pipeline["oee"]),
            ]
        )
    header = [
        "Station",
        "Parts",
        "Good parts",
        "Availability",
        "Performance",
        "Quality",
        "OEE, event log",
        "OEE, pipeline",
    ]
    errors = validation["error_pp"]
    worst = max(error["max_pp"] for error in errors.values())
    return {
        "in-process OEE table": table(header, rows, ["left"] + ["right"] * 7),
        "in-process OEE scope": (
            f"{len(validation['seeds'])} seeds of {validation['simulated_minutes_per_seed']} "
            f"simulated minutes, {validation['station_windows']:,} station-windows of "
            f"{validation['window_s']} s"
        ),
        "in-process OEE error": f"{worst:.3f} pp",
        "in-process samples": (
            f"{validation['samples_stored']:,} samples stored, "
            f"{validation['samples_missing']} missing"
        ),
    }


def outage_simulation(results: dict) -> dict[str, str]:
    scenarios = results["fault_scenarios"]
    rows = []
    for scenario in scenarios:
        rows.append(
            [
                scenario["label"],
                f"{scenario['produced']:,}",
                count_share(scenario["missing"], scenario["produced"]),
                f"{scenario['wire_duplicates']:,}",
                f"{scenario['wrong_windows']} of {scenario['windows']}",
                f"{scenario['worst_availability_error_pp']:.1f} pp",
                f"{scenario['worst_oee_error_pp']:.1f} pp",
            ]
        )
    header = [
        "Scenario (5 seeds each)",
        "Samples produced",
        "Never stored",
        "Duplicates on the wire",
        "Wrong OEE windows",
        "Worst availability error",
        "Worst OEE error",
    ]
    wrong = sum(scenario["wrong_windows"] for scenario in scenarios)
    right_counts = sum(scenario["wrong_windows_right_counts"] for scenario in scenarios)
    totals = {(scenario["parts_truth"], scenario["parts_pipeline"]) for scenario in scenarios}
    (truth, pipeline), *others = totals
    if truth == pipeline and not others:
        parts = (
            f"{truth:,} parts in the event log and {pipeline:,} in the pipeline "
            "in every scenario"
        )
    else:
        parts = "the part totals of the run differ in some scenario (see results/inprocess.json)"
    return {
        "in-process outage table": table(header, rows, ["left"] + ["right"] * 6),
        "in-process wrong OEE windows": (
            f"{wrong} windows have a wrong OEE and {right_counts} of them have the right part count"
        ),
        "in-process part totals": parts,
    }


def latency(results: dict) -> dict[str, str]:
    nominal = results["nominal"]
    levels = [
        (f"{nominal['rows_per_s']:,.1f} (nominal)", nominal["latency_ms"]),
        *(
            (
                f"{level['rows_per_s']:,.1f} (target {level['target_changes_per_s']:,})",
                level["latency_ms"],
            )
            for level in results["load"]
        ),
    ]
    rows = []
    for label, hops in levels:
        total = hops["total"]
        rows.append(
            [
                label,
                f"{total['n']:,}",
                f"{total['p50']:,.1f}",
                f"{total['p95']:,.1f}",
                f"{total['p99']:,.1f}",
                f"{total['max']:,.1f}",
                f"{hops['opcua']['p50']:,.1f}",
                f"{hops['mqtt']['p50']:,.1f}",
                f"{hops['database']['p50']:,.1f}",
            ]
        )
    header = [
        "Rows stored per s",
        "Rows",
        "p50",
        "p95",
        "p99",
        "max",
        "OPC UA p50",
        "MQTT p50",
        "Database p50",
    ]
    heaviest = results["load"][-1]
    busiest, usage = max(heaviest["containers"].items(), key=lambda item: item[1]["cpu_percent"])
    environment = results["environment"]
    images = ", ".join(f"{name} {size}" for name, size in environment["images"].items())
    return {
        "latency table": table(header, rows, ["left"] + ["right"] * 8),
        "latency nominal": (
            f"p50 {nominal['latency_ms']['total']['p50']:g} ms, "
            f"p95 {nominal['latency_ms']['total']['p95']:g} ms, "
            f"p99 {nominal['latency_ms']['total']['p99']:g} ms"
        ),
        "load busiest container": (
            f"{busiest} container at {usage['cpu_percent']:g} % of one core"
        ),
        "load gaps": (
            f"{sum(level['analog_updates_missing'] for level in results['load'])} "
            "gaps in the analog tag series"
        ),
        "environment": (
            f"{environment['host_os']}, {environment['logical_cpus']} logical CPUs, "
            f"Docker {environment['docker_server']}"
        ),
        "image sizes": images,
    }


def compose_oee(results: dict) -> dict[str, str]:
    nominal = results["nominal"]
    worst = max(error["max_pp"] for error in nominal["error_pp"].values())
    return {
        "compose OEE": (
            f"{nominal['windows']} station-windows over {nominal['duration_s']:g} s, "
            f"{nominal['parts_pipeline']} parts counted by the pipeline and "
            f"{nominal['parts_truth']} in the event log, largest OEE error {worst:.3f} pp, "
            f"{nominal['samples_missing']} of {nominal['samples_expected']:,} samples missing"
        ),
        "cold start": (
            f"all services healthy after {results['cold_start']['all_healthy_s']:g} s, "
            f"first sample stored after {results['cold_start']['first_sample_s']:g} s"
        ),
    }


def outages(results: dict) -> dict[str, str]:
    runs = [
        (
            f"Gateway killed, {run['replay_s']} s history replay"
            if run["replay_s"]
            else "Gateway killed, no history replay",
            run,
        )
        for run in results["gateway_kill"]
    ] + [(f"Broker killed, QoS {run['qos']}", run) for run in results["broker_kill"]]
    rows = []
    for label, run in runs:
        rows.append(
            [
                label,
                f"{run['outage_s']:g}",
                f"{run['samples_expected']:,}",
                count_share(run["samples_missing"], run["samples_expected"]),
                f"{run['wire_duplicates']:,}",
                f"{run['recovery_s']:g}",
            ]
        )
    header = [
        "Scenario",
        "Outage (s)",
        "Samples expected",
        "Never stored",
        "Duplicates on the wire",
        "First live row after restart (s)",
    ]
    gateway = results["gateway_kill"][0]
    qos0 = next(run for run in results["broker_kill"] if run["qos"] == 0)
    return {
        "compose outage table": table(header, rows, ["left"] + ["right"] * 5),
        "compose gateway windows": (
            f"{gateway['wrong_windows']} of {gateway['windows']} windows had a wrong OEE "
            f"(up to {_pp(gateway, 'oee')}) while availability was off by up to "
            f"{_pp(gateway, 'availability')} and performance by up to {_pp(gateway, 'performance')}"
        ),
        "compose QoS 0 windows": (
            f"{qos0['wrong_windows']} of {qos0['windows']} compared windows had a wrong OEE "
            f"(up to {_pp(qos0, 'oee')}), availability was off by up to "
            f"{_pp(qos0, 'availability')}, and {qos0['parts_pipeline']} parts were counted "
            f"against {qos0['parts_truth']} in the event log"
        ),
    }


def _pp(run: dict, metric: str) -> str:
    return f"{run['error_pp'][metric]['max_pp']:.1f} pp"




def alerts(results: dict) -> dict[str, str]:
    alert = results["alerts"]
    return {
        "alert delay": (
            f"median {alert['median_s']:g} s (min {alert['min_s']:g} s, max {alert['max_s']:g} s) "
            f"over {alert['injections']} injected breakdowns"
        )
    }


def load_json(path: Path) -> dict | None:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def blocks(root: Path) -> dict[str, str]:
    """Every text the README must contain, by name."""
    expected: dict[str, str] = {}
    inprocess = load_json(root / "results" / "inprocess.json")
    compose = load_json(root / "results" / "compose.json")
    if inprocess is not None:
        expected |= oee_agreement(inprocess) | outage_simulation(inprocess)
    if compose is not None:
        expected |= latency(compose) | compose_oee(compose) | outages(compose) | alerts(compose)
    return expected


def verify(root: Path = ROOT) -> list[str]:
    """Problems found; empty when every expected block is in the README."""
    readme = (root / "README.md").read_text(encoding="utf-8")
    expected = blocks(root)
    if not expected:
        return ["no result files found in results/"]
    return [
        f"{name!r} is missing or differs from results/ :\n{text}"
        for name, text in expected.items()
        if text not in readme
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--print", action="store_true", dest="show", help="print the blocks")
    arguments = parser.parse_args()
    if arguments.show:
        for name, text in blocks(ROOT).items():
            print(f"--- {name}\n{text}\n")
        return 0
    problems = verify()
    for problem in problems:
        print(f"README: {problem}")
    if not problems:
        print(f"README matches the results ({len(blocks(ROOT))} blocks checked).")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
