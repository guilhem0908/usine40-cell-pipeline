"""Time-series store: PostgreSQL in the compose stack, SQLite in tests and the demo.

Three tables, created at start-up if they do not exist:

* ``sample``       one row per (station, signal, source timestamp); the primary
                   key makes inserts idempotent, so MQTT redeliveries and history
                   replays cannot create duplicates
* ``oee_window``   durations per state and part counts per station and window
* ``ingest_session`` per gateway session: messages received, duplicates, gaps

Timestamps are BIGINT microseconds since the Unix epoch (see
:mod:`usine40.timebase`). Both engines run the same statements; only the
parameter marker and the expression for the database clock differ.
"""

from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from usine40.model import SIGNAL_STATE, State
from usine40.oee import WindowTotals

_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS sample (
        station      TEXT             NOT NULL,
        signal       TEXT             NOT NULL,
        source_us    BIGINT           NOT NULL,
        value        DOUBLE PRECISION NOT NULL,
        session      TEXT             NOT NULL,
        seq          BIGINT           NOT NULL,
        gateway_us   BIGINT           NOT NULL,
        collector_us BIGINT           NOT NULL,
        db_us        BIGINT           NOT NULL,
        replay       BOOLEAN          NOT NULL,
        PRIMARY KEY (station, signal, source_us)
    )
    """,
    "CREATE INDEX IF NOT EXISTS sample_source_us ON sample (source_us)",
    """
    CREATE TABLE IF NOT EXISTS oee_window (
        station       TEXT             NOT NULL,
        start_us      BIGINT           NOT NULL,
        end_us        BIGINT           NOT NULL,
        idle_us       BIGINT           NOT NULL,
        running_us    BIGINT           NOT NULL,
        blocked_us    BIGINT           NOT NULL,
        starved_us    BIGINT           NOT NULL,
        fault_us      BIGINT           NOT NULL,
        total         INTEGER          NOT NULL,
        good          INTEGER          NOT NULL,
        ideal_cycle_s DOUBLE PRECISION NOT NULL,
        complete      BOOLEAN          NOT NULL,
        PRIMARY KEY (station, start_us)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingest_session (
        session         TEXT   PRIMARY KEY,
        updated_us      BIGINT NOT NULL,
        received        BIGINT NOT NULL,
        duplicates      BIGINT NOT NULL,
        missing         BIGINT NOT NULL,
        highest_seq     BIGINT NOT NULL,
        live_inserted   BIGINT NOT NULL,
        replay_received BIGINT NOT NULL,
        replay_inserted BIGINT NOT NULL
    )
    """,
)

_STATE_COLUMNS = {
    State.IDLE: "idle_us",
    State.RUNNING: "running_us",
    State.BLOCKED: "blocked_us",
    State.STARVED: "starved_us",
    State.FAULT: "fault_us",
}


@dataclass(frozen=True, slots=True)
class SampleRow:
    """One decoded telemetry message, ready to be stored."""

    station: str
    signal: str
    source_us: int
    value: float
    session: str
    seq: int
    gateway_us: int
    collector_us: int
    replay: bool


@dataclass(frozen=True, slots=True)
class SessionStats:
    session: str
    updated_us: int
    received: int
    duplicates: int
    missing: int
    highest_seq: int
    live_inserted: int
    replay_received: int
    replay_inserted: int


@dataclass(frozen=True, slots=True)
class _Dialect:
    marker: str
    database_clock_us: str


_SQLITE = _Dialect("?", "CAST(ROUND((julianday('now') - 2440587.5) * 86400000000.0) AS INTEGER)")
_POSTGRES = _Dialect("%s", "(extract(epoch FROM clock_timestamp()) * 1000000)::bigint")


class SqlStore:
    """DB-API store shared by SQLite and PostgreSQL."""

    def __init__(self, connect: Callable[[], Any], dialect: _Dialect) -> None:
        self._connect = connect
        self._connection = connect()
        self._dialect = dialect

    def close(self) -> None:
        self._connection.close()

    def reset(self) -> None:
        """Drop the connection and open a new one (after a database outage)."""
        with contextlib.suppress(Exception):
            self._connection.close()
        self._connection = self._connect()

    def migrate(self) -> None:
        cursor = self._connection.cursor()
        for statement in _SCHEMA:
            cursor.execute(statement)
        self._connection.commit()

    def insert_samples(self, rows: Sequence[SampleRow]) -> int:
        """Insert rows, ignoring those whose key already exists; returns the number added."""
        if not rows:
            return 0
        statement = self._sql(
            "INSERT INTO sample (station, signal, source_us, value, session, seq, gateway_us,"
            " collector_us, replay, db_us) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, {clock})"
            " ON CONFLICT (station, signal, source_us) DO NOTHING"
        )
        parameters = [
            (r.station, r.signal, r.source_us, r.value, r.session, r.seq, r.gateway_us,
             r.collector_us, r.replay)
            for r in rows
        ]  # fmt: skip
        cursor = self._connection.cursor()
        cursor.executemany(statement, parameters)
        inserted = cursor.rowcount
        self._connection.commit()
        return inserted

    def series(
        self, station: str, signal: str, start_us: int, end_us: int
    ) -> list[tuple[int, float]]:
        """Points of one signal in ``[start_us, end_us)``, preceded by the last one before."""
        cursor = self._connection.cursor()
        cursor.execute(
            self._sql(
                "SELECT source_us, value FROM sample WHERE station = ? AND signal = ?"
                " AND source_us < ? ORDER BY source_us DESC LIMIT 1"
            ),
            (station, signal, start_us),
        )
        points = [(int(ts), float(value)) for ts, value in cursor.fetchall()]
        cursor.execute(
            self._sql(
                "SELECT source_us, value FROM sample WHERE station = ? AND signal = ?"
                " AND source_us >= ? AND source_us < ? ORDER BY source_us"
            ),
            (station, signal, start_us, end_us),
        )
        points.extend((int(ts), float(value)) for ts, value in cursor.fetchall())
        self._connection.commit()
        return points

    def latest_timestamp(self, station: str, signal: str) -> int | None:
        """Source timestamp of the newest stored point of a signal."""
        rows = self.query(
            "SELECT max(source_us) FROM sample WHERE station = ? AND signal = ?",
            (station, signal),
        )
        return None if rows[0][0] is None else int(rows[0][0])

    def production_stations(self) -> list[str]:
        """Stations that report a state, in name order."""
        rows = self.query(
            "SELECT DISTINCT station FROM sample WHERE signal = ? ORDER BY station",
            (SIGNAL_STATE,),
        )
        return [row[0] for row in rows]

    def sample_keys(self) -> set[tuple[str, str, int]]:
        rows = self.query("SELECT station, signal, source_us FROM sample")
        return {(station, signal, int(ts)) for station, signal, ts in rows}

    def upsert_oee(self, windows: Sequence[tuple[WindowTotals, bool]]) -> None:
        """Write ``(totals, complete)`` pairs, replacing earlier versions of a window."""
        if not windows:
            return
        columns = [_STATE_COLUMNS[state] for state in State]
        updates = ", ".join(
            f"{column} = excluded.{column}"
            for column in ["end_us", *columns, "total", "good", "ideal_cycle_s", "complete"]
        )
        statement = self._sql(
            f"INSERT INTO oee_window (station, start_us, end_us, {', '.join(columns)},"
            " total, good, ideal_cycle_s, complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            f" ON CONFLICT (station, start_us) DO UPDATE SET {updates}"
        )
        parameters = [
            (w.station, w.start_us, w.end_us, *w.state_us, w.total, w.good, w.ideal_cycle_s, done)
            for w, done in windows
        ]
        cursor = self._connection.cursor()
        cursor.executemany(statement, parameters)
        self._connection.commit()

    def oee_windows(self, complete_only: bool = True) -> list[WindowTotals]:
        columns = ", ".join(_STATE_COLUMNS[state] for state in State)
        rows = self.query(
            f"SELECT station, start_us, end_us, {columns}, total, good, ideal_cycle_s, complete"
            " FROM oee_window ORDER BY station, start_us"
        )
        return [
            WindowTotals(
                station=row[0],
                start_us=int(row[1]),
                end_us=int(row[2]),
                state_us=tuple(int(value) for value in row[3:8]),
                total=int(row[8]),
                good=int(row[9]),
                ideal_cycle_s=float(row[10]),
            )
            for row in rows
            if row[11] or not complete_only
        ]

    def upsert_session(self, stats: SessionStats) -> None:
        columns = [
            "updated_us", "received", "duplicates", "missing", "highest_seq",
            "live_inserted", "replay_received", "replay_inserted",
        ]  # fmt: skip
        updates = ", ".join(f"{column} = excluded.{column}" for column in columns)
        self._connection.cursor().execute(
            self._sql(
                f"INSERT INTO ingest_session (session, {', '.join(columns)})"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                f" ON CONFLICT (session) DO UPDATE SET {updates}"
            ),
            (
                stats.session, stats.updated_us, stats.received, stats.duplicates, stats.missing,
                stats.highest_seq, stats.live_inserted, stats.replay_received,
                stats.replay_inserted,
            ),
        )  # fmt: skip
        self._connection.commit()

    def query(self, statement: str, parameters: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        """Run a read-only statement written with ``?`` markers."""
        cursor = self._connection.cursor()
        cursor.execute(self._sql(statement), tuple(parameters))
        rows = cursor.fetchall()
        self._connection.commit()
        return rows

    def _sql(self, statement: str) -> str:
        statement = statement.replace("{clock}", self._dialect.database_clock_us)
        return statement.replace("?", self._dialect.marker)


def open_sqlite(path: str = ":memory:") -> SqlStore:
    """Embedded store; ``check_same_thread`` is off because the collector owns it."""
    store = SqlStore(lambda: sqlite3.connect(path, check_same_thread=False), _SQLITE)
    store.migrate()
    return store


def open_postgres(dsn: str) -> SqlStore:
    import psycopg

    store = SqlStore(lambda: psycopg.connect(dsn), _POSTGRES)
    store.migrate()
    return store
