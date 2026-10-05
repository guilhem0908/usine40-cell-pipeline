"""Environment-driven settings shared by the three services."""

from __future__ import annotations

import logging
import os
import signal
from collections.abc import Callable
from dataclasses import dataclass

from usine40.topics import CellPath

ENV_PREFIX = "USINE40_"


def _env(name: str, default: str) -> str:
    return os.environ.get(ENV_PREFIX + name, default)


@dataclass(frozen=True, slots=True)
class Settings:
    site: str
    area: str
    cell: str
    opcua_url: str
    opcua_bind: str
    mqtt_host: str
    mqtt_port: int
    mqtt_qos: int
    database_url: str
    window_s: float
    recompute_s: float
    backfill_s: float
    publishing_interval_ms: float
    seed: int
    fault_schedule: str
    planned_stops: str
    event_log: str
    health_file: str

    @property
    def path(self) -> CellPath:
        return CellPath(self.site, self.area, self.cell)

    @classmethod
    def from_env(cls) -> Settings:
        """Read ``USINE40_*`` variables; defaults suit a local, single-host run."""
        qos = int(_env("MQTT_QOS", "1"))
        if qos not in (0, 1):
            raise ValueError("USINE40_MQTT_QOS must be 0 or 1.")
        return cls(
            site=_env("SITE", "site1"),
            area=_env("AREA", "assembly"),
            cell=_env("CELL", "cell1"),
            opcua_url=_env("OPCUA_URL", "opc.tcp://localhost:4840/usine40"),
            opcua_bind=_env("OPCUA_BIND", "opc.tcp://0.0.0.0:4840/usine40"),
            mqtt_host=_env("MQTT_HOST", "localhost"),
            mqtt_port=int(_env("MQTT_PORT", "1883")),
            mqtt_qos=qos,
            database_url=_env("DATABASE_URL", "sqlite:usine40.sqlite"),
            window_s=float(_env("WINDOW_S", "30")),
            recompute_s=float(_env("RECOMPUTE_S", "300")),
            backfill_s=float(_env("BACKFILL_S", "120")),
            publishing_interval_ms=float(_env("PUBLISHING_INTERVAL_MS", "100")),
            seed=int(_env("SEED", "1")),
            fault_schedule=_env("FAULT_SCHEDULE", "[]"),
            planned_stops=_env("PLANNED_STOPS", "[]"),
            event_log=_env("EVENT_LOG", "events.jsonl"),
            health_file=_env("HEALTH_FILE", ""),
        )


def configure_logging() -> None:
    logging.basicConfig(
        level=_env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("asyncua").setLevel(logging.WARNING)


def install_stop_signals(stop: Callable[[], None]) -> None:
    """Call ``stop`` on SIGINT and SIGTERM so containers shut down cleanly."""
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop())
