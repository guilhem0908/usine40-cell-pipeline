"""Regenerate every result and figure of the README.

    python scripts/reproduce.py             # deterministic in-process experiments (about 2 min)
    python scripts/reproduce.py --compose   # also the Docker scenario and dashboard GIF (~25 min)

The in-process part needs no Docker and gives the same files on every machine
(fixed seeds, simulated time). The compose part starts the stack under the
project name ``usine40cell``, measures latency, restarts and alert delays on
this machine, captures the dashboard and tears the stack down. Afterwards the
tables of the README are checked against the result files.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import check_readme
import inprocess_experiments

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
DOCS = ROOT / "docs"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--compose",
        action="store_true",
        help="also run the Docker scenario and capture the dashboard",
    )
    parser.add_argument(
        "--skip-inprocess",
        action="store_true",
        help="keep the committed in-process results (only useful with --compose)",
    )
    arguments = parser.parse_args()

    if not arguments.skip_inprocess:
        started = time.perf_counter()
        results = inprocess_experiments.run_all(RESULTS)
        validation = results["oee_validation"]
        print(
            f"In-process experiments done in {time.perf_counter() - started:.0f} s: "
            f"{validation['station_windows']} station-windows compared, "
            f"largest OEE error {validation['error_pp']['oee']['max_pp']:.6f} pp, "
            f"{validation['samples_missing']} samples missing."
        )
    if arguments.compose:
        import compose_scenario

        compose_scenario.run(RESULTS, DOCS)

    problems = check_readme.verify(ROOT)
    for problem in problems:
        print(f"README: {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
