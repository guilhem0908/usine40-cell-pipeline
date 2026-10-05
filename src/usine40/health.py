"""File-based liveness used by the container healthchecks.

Each service touches a file from its main loop only while it is doing its job
(the PLC scanning, the gateway connected to the OPC UA server, the collector
writing to the database). ``python -m usine40.health <file>`` exits 0 when the
file was touched recently.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

MAX_AGE_S = 10.0
MIN_BEAT_INTERVAL_S = 1.0


class Heartbeat:
    """Touches ``path`` at most once per second; does nothing if ``path`` is empty."""

    def __init__(self, path: str) -> None:
        self._path = Path(path) if path else None
        self._last = 0.0

    def beat(self) -> None:
        now = time.monotonic()
        if self._path is None or now - self._last < MIN_BEAT_INTERVAL_S:
            return
        self._last = now
        self._path.touch()


def is_alive(path: str, max_age_s: float = MAX_AGE_S) -> bool:
    try:
        return time.time() - Path(path).stat().st_mtime <= max_age_s
    except OSError:
        return False


if __name__ == "__main__":
    sys.exit(0 if len(sys.argv) == 2 and is_alive(sys.argv[1]) else 1)
