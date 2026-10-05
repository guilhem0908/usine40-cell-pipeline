"""Integer microsecond timestamps shared by every stage of the pipeline.

A sample is identified end to end by its PLC source timestamp. Carrying that
timestamp as an integer number of microseconds since the Unix epoch keeps it
bit-identical in the simulator log, the OPC UA DateTime, the JSON payload and
the database key, which is what makes idempotent inserts and exact comparison
with the ground truth possible. Floats would round differently at each hop.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

US_PER_S = 1_000_000

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_US = timedelta(microseconds=1)


def now_us() -> int:
    """Wall-clock time in microseconds since the Unix epoch."""
    return time.time_ns() // 1_000


def seconds_to_us(seconds: float) -> int:
    """Round a duration in seconds to the nearest microsecond."""
    return round(seconds * US_PER_S)


def datetime_to_us(moment: datetime) -> int:
    """Convert a datetime to microseconds since the epoch with integer arithmetic.

    Naive datetimes are taken as UTC, which is what OPC UA DateTime values are.
    """
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return (moment - _EPOCH) // _ONE_US


def us_to_datetime(us: int) -> datetime:
    """Inverse of :func:`datetime_to_us` (timezone-aware UTC)."""
    return _EPOCH + timedelta(microseconds=us)
