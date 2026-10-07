"""Measurements on the real stack (Mosquitto, PostgreSQL, Grafana) under Docker Compose.

The scenario starts the stack from nothing and runs, in order: a nominal phase
(latency per hop, OEE against the simulator log), injected breakdowns (alert
delay), the dashboard capture, a gateway kill with and without history replay,
a broker kill at QoS 0 and QoS 1, and a load sweep. Every duration is taken
from the database clock, which all containers share, so no host/VM clock
offset enters a result. Numbers depend on the machine; the README states where
they were measured.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import platform
import random
import subprocess
import tempfile
import threading
import time
import urllib.request
from base64 import b64encode
from pathlib import Path

import numpy as np

from usine40.ctl import inject_fault, set_load
from usine40.ground_truth import truth_windows
from usine40.model import Event, State, expected_samples, read_events
from usine40.oee import WindowTotals
from usine40.store import SqlStore, open_postgres
from usine40.timebase import US_PER_S, us_to_datetime
from usine40.validation import METRICS, compare_windows, summarize

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "usine40cell"
# The published ports are bound to the IPv4 loopback. On Windows, libpq waits for the full
# connect timeout on "localhost" before it falls back from ::1 to 127.0.0.1, so use the address.
OPCUA_URL = "opc.tcp://127.0.0.1:14840/usine40"
GRAFANA_URL = "http://127.0.0.1:3300"
DB_PASSWORD = os.environ.get("USINE40_DB_PASSWORD", "usine40-local")
GRAFANA_PASSWORD = os.environ.get("USINE40_GRAFANA_PASSWORD", "usine40-local")
DSN = f"postgresql://usine40:{DB_PASSWORD}@127.0.0.1:15432/usine40"
IMAGES = ("eclipse-mosquitto", "postgres", "grafana/grafana", "usine40cell/app")

WINDOW_US = 30 * US_PER_S
NOMINAL_S = 180.0
ALERT_INJECTIONS = 6
ALERT_FAULT_S = 20.0
ALERT_POLL_S = 0.25
ALERT_TIMEOUT_S = 45.0
STEADY_S = 20.0
GATEWAY_OUTAGE_S = 20.0
BROKER_OUTAGE_S = 15.0
SETTLE_S = 40.0
REPLAY_S = 120
LOAD_LEVELS = (100, 1000)
LOAD_RAMP_S = 8.0
LOAD_S = 30.0
PERCENTILES = (50, 95, 99)
HOPS = {
    "opcua": "gateway_us - source_us",
    "mqtt": "collector_us - gateway_us",
    "database": "db_us - collector_us",
    "total": "db_us - source_us",
}


def compose(*arguments: str, env: dict[str, str] | None = None) -> str:
    """Run ``docker compose`` for this project only and return its output."""
    completed = subprocess.run(
        ["docker", "compose", "-p", PROJECT, *arguments],
        cwd=ROOT,
        env={**os.environ, **(env or {})},
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=True,
    )
    return completed.stdout


def run_async(coroutine):
    """Run a coroutine in its own thread, away from any event loop of the caller."""
    box: dict[str, object] = {}
    thread = threading.Thread(target=lambda: box.setdefault("value", asyncio.run(coroutine)))
    thread.start()
    thread.join()
    return box.get("value")


class Stack:
    """The running compose project plus a database connection to observe it."""

    def __init__(self) -> None:
        self.env: dict[str, str] = {}
        self.store: SqlStore | None = None

    def up(self, **overrides: str) -> None:
        """Apply environment overrides; compose recreates the services they change."""
        self.env.update(overrides)
        compose("up", "-d", "--wait", env=self.env)

    def connect(self) -> None:
        self.store = open_postgres(DSN)

    def now_us(self) -> int:
        """Database clock, the time base of every measurement."""
        (row,) = self.store.query(
            "SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint"
        )
        return int(row[0])

    def sleep_until(self, deadline_us: int) -> None:
        time.sleep(max(deadline_us - self.now_us(), 0) / US_PER_S)

    def events(self) -> list[Event]:
        """The simulator's event log, copied out of the PLC container."""
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "events.jsonl"
            compose("cp", "plc:/data/events.jsonl", str(target))
            return list(read_events(target))

    def first_live_row_after(self, start_us: int, timeout_s: float = 60.0) -> int:
        """Database time of the first live sample produced after ``start_us``."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            (row,) = self.store.query(
                "SELECT min(db_us) FROM sample WHERE NOT replay AND source_us > ?", (start_us,)
            )
            if row[0] is not None:
                return int(row[0])
            time.sleep(0.1)
        raise TimeoutError("No live sample was stored after the restart.")

    def latency_ms(self, start_us: int, end_us: int) -> dict[str, np.ndarray]:
        """Per-hop latencies of the live samples produced in ``[start_us, end_us)``."""
        columns = ", ".join(HOPS.values())
        rows = self.store.query(
            f"SELECT {columns} FROM sample WHERE NOT replay AND source_us >= ? AND source_us < ?"
            " ORDER BY source_us",
            (start_us, end_us),
        )
        table = np.array(rows, dtype=float).reshape(-1, len(HOPS)) / 1000.0
        return {hop: table[:, index] for index, hop in enumerate(HOPS)}

    def sessions(self, since_us: int) -> dict[str, int]:
        """Wire statistics summed over the gateway sessions updated since ``since_us``."""
        (row,) = self.store.query(
            "SELECT coalesce(sum(received), 0), coalesce(sum(duplicates), 0),"
            " coalesce(sum(missing), 0), coalesce(sum(replay_received), 0),"
            " coalesce(sum(replay_inserted), 0) FROM ingest_session WHERE updated_us >= ?",
            (since_us,),
        )
        names = (
            "received",
            "wire_duplicates",
            "wire_missing",
            "replay_received",
            "replay_inserted",
        )
        return {name: int(value) for name, value in zip(names, row, strict=True)}

    def evaluate(self, start_us: int, end_us: int) -> dict:
        """Compare stored samples and OEE windows with the simulator log on a time range."""
        events = self.events()
        truth = [
            window
            for station in truth_windows(events, WINDOW_US, end_us).values()
            for window in station
            if window.start_us >= start_us and window.end_us <= end_us
        ]
        pipeline = [
            window
            for window in self.store.oee_windows(complete_only=True)
            if window.start_us >= start_us and window.end_us <= end_us
        ]
        rows = self.store.query(
            "SELECT station, signal, source_us FROM sample WHERE source_us >= ? AND source_us < ?",
            (start_us, end_us),
        )
        stored = {(station, signal, int(ts)) for station, signal, ts in rows}
        expected = [s for s in expected_samples(events) if start_us <= s.ts_us < end_us]
        comparison = compare_windows(truth, pipeline)
        return {
            "samples_expected": len(expected),
            "samples_missing": sum(sample.key not in stored for sample in expected),
            "windows": comparison.matched,
            "windows_unmatched": comparison.truth_only + comparison.pipeline_only,
            "wrong_windows": comparison.wrong_windows,
            "error_pp": {
                metric: {
                    "max_pp": comparison.errors[metric].max_pp,
                    "mean_pp": comparison.errors[metric].mean_pp,
                }
                for metric in METRICS
            },
            "parts_truth": sum(window.total for window in truth),
            "parts_pipeline": sum(window.total for window in pipeline),
            "truth": truth,
            "pipeline": pipeline,
        }


def percentiles(values: np.ndarray) -> dict[str, float | int]:
    summary: dict[str, float | int] = {"n": int(values.size)}
    for level in PERCENTILES:
        summary[f"p{level}"] = round(float(np.percentile(values, level)), 1)
    summary["max"] = round(float(values.max()), 1)
    return summary


def public(evaluation: dict) -> dict:
    """An evaluation without its raw window lists, ready for JSON."""
    return {key: value for key, value in evaluation.items() if key not in ("truth", "pipeline")}


def grafana(path: str) -> dict:
    request = urllib.request.Request(GRAFANA_URL + path)
    token = b64encode(f"admin:{GRAFANA_PASSWORD}".encode()).decode()
    request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def fault_alert_state(station: str) -> str:
    """State of the 'Station in fault' alert instance of one station."""
    groups = grafana("/api/prometheus/grafana/api/v1/rules")["data"]["groups"]
    for group in groups:
        for rule in group["rules"]:
            for alert in rule.get("alerts", []):
                if alert["labels"].get("station") == station:
                    return alert["state"]
    return "Unknown"


def measure_cold_start(stack: Stack) -> dict:
    compose("build")
    compose("down", "-v", "--remove-orphans")
    started = time.monotonic()
    stack.up()
    healthy_s = time.monotonic() - started
    stack.connect()
    first: dict[str, float] = {}
    for table in ("sample", "oee_window"):
        while not stack.store.query(f"SELECT 1 FROM {table} LIMIT 1"):
            time.sleep(0.1)
        first[table] = time.monotonic() - started
    return {
        "all_healthy_s": round(healthy_s, 1),
        "first_sample_s": round(first["sample"], 1),
        "first_oee_window_s": round(first["oee_window"], 1),
    }


def measure_nominal(stack: Stack, out_dir: Path) -> dict:
    start_us = stack.now_us()
    start_us += WINDOW_US - start_us % WINDOW_US
    end_us = start_us + round(NOMINAL_S * US_PER_S)
    stack.sleep_until(end_us + 6 * US_PER_S)
    latency = stack.latency_ms(start_us, end_us)
    evaluation = stack.evaluate(start_us, end_us)
    _write_latency_csv(out_dir / "compose_latency_nominal.csv", latency)
    _write_window_csv(out_dir / "compose_oee_windows.csv", evaluation)
    stations = {}
    for station in sorted({window.station for window in evaluation["truth"]}):
        stations[station] = {
            side: summarize([w for w in evaluation[side] if w.station == station])
            for side in ("truth", "pipeline")
        }
    return {
        "duration_s": NOMINAL_S,
        "rows_per_s": round(latency["total"].size / NOMINAL_S, 1),
        "latency_ms": {hop: percentiles(values) for hop, values in latency.items()},
        "stations": stations,
        **public(evaluation),
    }


def measure_alert_delay(stack: Stack) -> dict:
    """Inject breakdowns at varying offsets from the 10 s evaluation tick."""
    rng = random.Random(0)
    delays = []
    for _ in range(ALERT_INJECTIONS):
        time.sleep(rng.uniform(8.0, 18.0))
        while fault_alert_state("machining") != "Normal":
            time.sleep(1.0)
        before_us = stack.now_us()
        run_async(inject_fault(OPCUA_URL, "machining", ALERT_FAULT_S))
        deadline = time.monotonic() + ALERT_TIMEOUT_S
        while fault_alert_state("machining") != "Alerting":
            if time.monotonic() > deadline:
                raise TimeoutError("The fault alert did not fire.")
            time.sleep(ALERT_POLL_S)
        seen_us = stack.now_us()
        (row,) = stack.store.query(
            "SELECT min(source_us) FROM sample WHERE station = 'machining' AND signal = 'state'"
            " AND value = ? AND source_us >= ?",
            (float(State.FAULT), before_us),
        )
        delays.append(round((seen_us - int(row[0])) / US_PER_S, 2))
        time.sleep(ALERT_FAULT_S)
    return {
        "injections": ALERT_INJECTIONS,
        "fault_s": ALERT_FAULT_S,
        "evaluation_interval_s": 10,
        "delays_s": delays,
        "min_s": min(delays),
        "median_s": round(float(np.median(delays)), 2),
        "max_s": max(delays),
    }


def measure_gateway_kill(stack: Stack, replay_s: int) -> dict:
    stack.up(USINE40_MQTT_QOS="1", USINE40_BACKFILL_S=str(replay_s))
    time.sleep(STEADY_S)
    kill_us = stack.now_us()
    compose("kill", "gateway")
    stack.sleep_until(kill_us + round(GATEWAY_OUTAGE_S * US_PER_S))
    restart_us = stack.now_us()
    compose("start", "gateway")
    recovered_us = stack.first_live_row_after(restart_us)
    time.sleep(SETTLE_S)
    end_us = stack.now_us() - 5 * US_PER_S
    evaluation = stack.evaluate(kill_us - 10 * US_PER_S, end_us)
    return {
        "replay_s": replay_s,
        "outage_s": round((restart_us - kill_us) / US_PER_S, 1),
        "recovery_s": round((recovered_us - restart_us) / US_PER_S, 2),
        **stack.sessions(restart_us),
        **public(evaluation),
    }


def measure_broker_kill(stack: Stack, qos: int) -> dict:
    stack.up(USINE40_MQTT_QOS=str(qos), USINE40_BACKFILL_S="0")
    time.sleep(STEADY_S)
    kill_us = stack.now_us()
    compose("kill", "mosquitto")
    stack.sleep_until(kill_us + round(BROKER_OUTAGE_S * US_PER_S))
    restart_us = stack.now_us()
    compose("start", "mosquitto")
    recovered_us = stack.first_live_row_after(restart_us)
    time.sleep(SETTLE_S)
    end_us = stack.now_us() - 5 * US_PER_S
    evaluation = stack.evaluate(kill_us - 10 * US_PER_S, end_us)
    return {
        "qos": qos,
        "outage_s": round((restart_us - kill_us) / US_PER_S, 1),
        "recovery_s": round((recovered_us - restart_us) / US_PER_S, 2),
        **stack.sessions(kill_us - round(STEADY_S * US_PER_S)),
        **public(evaluation),
    }


def container_stats() -> dict[str, dict[str, float]]:
    """CPU percent and memory of this project's containers (one docker stats sample)."""
    identifiers = compose("ps", "-q").split()
    output = subprocess.run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}};{{.CPUPerc}};{{.MemUsage}}",
            *identifiers,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=True,
    ).stdout
    stats = {}
    for line in output.strip().splitlines():
        name, cpu, memory = line.split(";")
        service = name.removeprefix(f"{PROJECT}-").rsplit("-", 1)[0]
        used = memory.split("/")[0].strip()
        factor = 1024.0 if used.endswith("GiB") else 1.0
        megabytes = float(used.rstrip("GMiB")) * factor
        stats[service] = {"cpu_percent": float(cpu.rstrip("%")), "memory_mb": round(megabytes, 1)}
    return stats


def measure_load(stack: Stack) -> list[dict]:
    levels = []
    for rate in LOAD_LEVELS:
        run_async(set_load(OPCUA_URL, rate))
        time.sleep(LOAD_RAMP_S)
        start_us = stack.now_us()
        stack.sleep_until(start_us + round(LOAD_S * US_PER_S))
        end_us = stack.now_us()
        stats = container_stats()
        time.sleep(3.0)
        latency = stack.latency_ms(start_us, end_us)
        (row,) = stack.store.query(
            "SELECT coalesce(sum(span - stored), 0) FROM (SELECT max(value) - min(value) + 1"
            " AS span, count(*) AS stored FROM sample WHERE station = 'sensors'"
            " AND source_us >= ? AND source_us < ? GROUP BY signal) per_tag",
            (start_us, end_us),
        )
        levels.append(
            {
                "target_changes_per_s": rate,
                "duration_s": LOAD_S,
                "rows_per_s": round(latency["total"].size / LOAD_S, 1),
                "latency_ms": {hop: percentiles(values) for hop, values in latency.items()},
                "analog_updates_missing": int(row[0]),
                "containers": stats,
            }
        )
    run_async(set_load(OPCUA_URL, 0.0))
    return levels


def environment() -> dict:
    version = subprocess.run(
        ["docker", "version", "--format", "{{.Server.Version}} {{.Server.Os}}/{{.Server.Arch}}"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=True,
    ).stdout.strip()
    listing = subprocess.run(
        ["docker", "images", "--format", "{{.Repository}}:{{.Tag}};{{.Size}}"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=True,
    ).stdout
    used = compose("config", "--images").split()
    images = dict(line.split(";") for line in listing.splitlines() if line.split(";")[0] in used)
    return {
        "host_os": f"{platform.system()} {platform.release()}",
        "logical_cpus": os.cpu_count(),
        "docker_server": version,
        "images": dict(sorted(images.items())),
    }


def run(out_dir: Path, docs_dir: Path) -> dict:
    """Run the whole scenario, write ``compose.json`` and the dashboard GIF, tear down."""
    import capture_dashboard

    stack = Stack()
    results: dict = {"environment": environment()}
    try:
        results["cold_start"] = _report("cold start", measure_cold_start(stack))
        results["nominal"] = _report("nominal", measure_nominal(stack, out_dir))
        results["alerts"] = _report("alert delay", measure_alert_delay(stack))
        results["capture"] = _report(
            "dashboard capture", capture_dashboard.capture(stack, docs_dir / "dashboard.gif")
        )
        time.sleep(STEADY_S)
        results["gateway_kill"] = [
            _report(f"gateway kill, replay {replay_s} s", measure_gateway_kill(stack, replay_s))
            for replay_s in (0, REPLAY_S)
        ]
        results["broker_kill"] = [
            _report(f"broker kill, QoS {qos}", measure_broker_kill(stack, qos)) for qos in (0, 1)
        ]
        stack.up(USINE40_MQTT_QOS="1", USINE40_BACKFILL_S=str(REPLAY_S))
        time.sleep(STEADY_S)
        results["load"] = _report("load", measure_load(stack))
    finally:
        if stack.store is not None:
            stack.store.close()
        compose("down", "-v", "--remove-orphans")
    text = json.dumps(results, indent=2) + "\n"
    (out_dir / "compose.json").write_text(text, encoding="utf-8", newline="\n")
    return results


def _report(name: str, result):
    print(f"[compose] {name}: done", flush=True)
    return result


def _write_latency_csv(path: Path, latency: dict[str, np.ndarray]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow([f"{hop}_ms" for hop in latency])
        writer.writerows(zip(*(np.round(values, 3) for values in latency.values()), strict=True))


def _write_window_csv(path: Path, evaluation: dict) -> None:
    measured = {(w.station, w.start_us): w for w in evaluation["pipeline"]}
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        header = ["station", "window_start_utc"]
        for side in ("truth", "pipeline"):
            header += [f"{side}_{name}" for name in ("run_s", "planned_s", "parts", "good", "oee")]
        writer.writerow(header)
        for window in sorted(evaluation["truth"], key=lambda w: (w.station, w.start_us)):
            row = [window.station, us_to_datetime(window.start_us).isoformat()]
            for side in (window, measured.get((window.station, window.start_us))):
                row += _window_cells(side)
            writer.writerow(row)


def _window_cells(window: WindowTotals | None) -> list:
    if window is None:
        return [""] * 5
    oee = "" if window.oee is None else round(window.oee, 6)
    seconds = (window.run_us / US_PER_S, window.planned_us / US_PER_S)
    return [*seconds, window.total, window.good, oee]
