"""Telemetry payload: one JSON object per sample.

The station and the signal are in the topic; the payload carries the value and
what is needed to audit the transport::

    {"v": 1, "session": "9f2c41d07a3e", "seq": 1842,
     "source_us": 1759696245150000, "gateway_us": 1759696245231907,
     "value": 4.0, "replay": false}

* ``session`` identifies one run of the gateway process; ``seq`` increases by
  one per published message within a session, so gaps are losses and repeats
  are duplicates.
* ``source_us`` is the PLC source timestamp and ``gateway_us`` the time the
  gateway published, both in microseconds since the Unix epoch.
* ``replay`` is true for samples that are not live changes: values re-read
  from the OPC UA history after a gateway start, and the current value the
  server sends for each variable when a subscription begins.

The same contract is published as JSON Schema in ``schemas/telemetry.schema.json``.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass

SCHEMA_VERSION = 1

_FIELDS = frozenset({"v", "session", "seq", "source_us", "gateway_us", "value", "replay"})


class PayloadError(ValueError):
    """Raised when a payload does not follow the telemetry contract."""


def _integer(raw: dict[str, object], key: str) -> int:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PayloadError(f"{key!r} must be a non-negative integer.")
    return value


@dataclass(frozen=True, slots=True)
class Telemetry:
    session: str
    seq: int
    source_us: int
    gateway_us: int
    value: float
    replay: bool = False

    def to_json(self) -> bytes:
        document = {
            "v": SCHEMA_VERSION,
            "session": self.session,
            "seq": self.seq,
            "source_us": self.source_us,
            "gateway_us": self.gateway_us,
            "value": self.value,
            "replay": self.replay,
        }
        return json.dumps(document, separators=(",", ":"), allow_nan=False).encode()

    @classmethod
    def from_json(cls, payload: bytes) -> Telemetry:
        try:
            raw = json.loads(payload)
        except (ValueError, UnicodeDecodeError) as error:
            raise PayloadError("Payload is not valid JSON.") from error
        if not isinstance(raw, dict) or set(raw) != _FIELDS:
            raise PayloadError("Payload must be an object with exactly the contract fields.")
        if raw["v"] != SCHEMA_VERSION or isinstance(raw["v"], bool):
            raise PayloadError(f"Unsupported payload version {raw['v']!r}.")
        session, value, replay = raw["session"], raw["value"], raw["replay"]
        if not isinstance(session, str) or not session:
            raise PayloadError("'session' must be a non-empty string.")
        numeric = isinstance(value, int | float) and not isinstance(value, bool)
        if not numeric or not math.isfinite(value):
            raise PayloadError("'value' must be a finite number.")
        if not isinstance(replay, bool):
            raise PayloadError("'replay' must be a boolean.")
        return cls(
            session=session,
            seq=_integer(raw, "seq"),
            source_us=_integer(raw, "source_us"),
            gateway_us=_integer(raw, "gateway_us"),
            value=float(value),
            replay=replay,
        )
