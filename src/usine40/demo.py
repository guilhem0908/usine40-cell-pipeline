"""Docker-free demonstration: run the pipeline in one process and audit it.

    usine40-demo --minutes 20 --seed 1

runs the simulated cell for 20 simulated minutes through the real OPC UA
server, gateway and collector (in-memory broker substitute, SQLite store),
then prints the OEE the pipeline stored next to the ground truth computed
from the simulator's event log.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time

from usine40.oee import PERCENT, WindowTotals, combine
from usine40.rig import Rig
from usine40.sim import default_cell

SECONDS_PER_MINUTE = 60.0


def _percent(value: float | None) -> str:
    return "    n/a" if value is None else f"{value * PERCENT:6.2f}%"


def _row(label: str, totals: WindowTotals) -> str:
    ratios = (totals.availability, totals.performance, totals.quality, totals.oee)
    cells = "  ".join(_percent(value) for value in ratios)
    return f"  {label:<9} {cells}  {totals.total:6d} {totals.good:6d}"


async def run(minutes: float, seed: int) -> int:
    """Run the demo; returns the number of missing samples plus mismatching windows."""
    started = time.perf_counter()
    async with Rig(default_cell(seed)) as rig:
        await rig.advance(minutes * SECONDS_PER_MINUTE)
        comparison = rig.compare()
        truth, pipeline = rig.truth(), rig.pipeline()
        missing = rig.missing()
        stored = len(rig.store.sample_keys())
        stations = rig.config.station_names
    elapsed = time.perf_counter() - started

    print(f"Simulated {minutes:g} min of production (seed {seed}) in {elapsed:.1f} s.")
    print(f"Samples stored: {stored}, missing against the simulator log: {missing}.\n")
    header = "availab.  perform.  quality      OEE   parts   good"
    print(f"  {'':<9} {header}")
    for station in stations:
        print(station)
        print(_row("truth", combine([w for w in truth if w.station == station])))
        print(_row("pipeline", combine([w for w in pipeline if w.station == station])))
    worst = max(error.max_pp for error in comparison.errors.values())
    print(
        f"\n{comparison.matched} station-windows compared, "
        f"largest OEE-ratio error {worst:.4f} percentage points, "
        f"{comparison.count_mismatches} windows with a different part count."
    )
    return missing + comparison.count_mismatches + comparison.truth_only


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--minutes", type=float, default=20.0, help="simulated duration")
    parser.add_argument("--seed", type=int, default=1, help="simulator seed")
    arguments = parser.parse_args()
    logging.getLogger("asyncua").setLevel(logging.ERROR)
    raise SystemExit(1 if asyncio.run(run(arguments.minutes, arguments.seed)) else 0)


if __name__ == "__main__":
    main()
