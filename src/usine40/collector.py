"""Collector: MQTT telemetry in, idempotent rows and OEE windows out.

Messages are queued by the network thread and written in small batches by the
main loop. Three rules keep the stored data trustworthy:

* a malformed topic or payload, or a ``state`` value that is not one of the
  five station states, is counted and dropped, never stored;
* a sample is identified by (station, signal, source timestamp), so the
  duplicates produced by QoS 1 redelivery and by history replays are ignored
  by the database;
* OEE is only computed up to the watermark, the source timestamp of the last
  PLC heartbeat stored. When the data stops, the figures stop too instead of
  stretching the last known state up to the present.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import logging
import queue
import threading
import time
from collections.abc import Callable

from usine40.bus import Bus
from usine40.config import Settings, configure_logging, install_stop_signals
from usine40.health import Heartbeat
from usine40.model import (
    CELL_STATION,
    SIGNAL_GOOD,
    SIGNAL_HEARTBEAT,
    SIGNAL_IDEAL,
    SIGNAL_STATE,
    SIGNAL_TOTAL,
    STATE_VALUES,
)
from usine40.oee import Series, WindowTotals, totals_from_series, window_bounds
from usine40.payload import PayloadError, Telemetry
from usine40.sequence import SequenceTracker
from usine40.store import SampleRow, SessionStats, SqlStore
from usine40.timebase import now_us, seconds_to_us
from usine40.topics import CellPath, TopicError, data_filter, parse_data_topic

FLUSH_INTERVAL_S = 0.05
AGGREGATE_INTERVAL_S = 2.0
STORE_RETRY_DELAY_S = 1.0

_log = logging.getLogger(__name__)


@dataclasses.dataclass(slots=True)
class _SessionCounters:
    tracker: SequenceTracker = dataclasses.field(default_factory=SequenceTracker)
    live_inserted: int = 0
    replay_received: int = 0
    replay_inserted: int = 0
    retained: int = 0


class Collector:
    """Subscribes to one cell's telemetry and maintains the store."""

    def __init__(
        self,
        bus: Bus,
        store: SqlStore,
        path: CellPath,
        *,
        qos: int = 1,
        window_s: float = 30.0,
        recompute_s: float = 300.0,
        clock: Callable[[], int] = now_us,
    ) -> None:
        self._bus, self._store, self._path = bus, store, path
        self._qos = qos
        self._window_us = seconds_to_us(window_s)
        self._recompute_us = seconds_to_us(recompute_s)
        self._clock = clock
        self._inbox: queue.SimpleQueue[tuple[str, bytes, bool, int]] = queue.SimpleQueue()
        self._sessions: dict[str, _SessionCounters] = {}
        self._pending: dict[tuple[str, bool], list[SampleRow]] = {}
        self._dirty: set[str] = set()
        self.rejected = 0

    def start(self) -> None:
        self._bus.subscribe(data_filter(self._path), self._qos, self._on_message)

    def session_stats(self) -> list[SessionStats]:
        return [self._stats(session, counters) for session, counters in self._sessions.items()]

    def flush(self) -> int:
        """Store everything received so far; returns the number of new rows.

        Decoded rows stay pending until the database has committed them, so a
        database outage delays data instead of losing it.
        """
        while True:
            try:
                topic, payload, retained, received_us = self._inbox.get_nowait()
            except queue.Empty:
                break
            row = self._decode(topic, payload, received_us)
            if row is None:
                continue
            counters = self._sessions.setdefault(row.session, _SessionCounters())
            if retained:
                # Stored copy handed over at subscription time: not part of the
                # live stream, so it says nothing about loss or duplication.
                counters.retained += 1
            elif not counters.tracker.observe(row.seq):
                self._dirty.add(row.session)
                continue
            self._pending.setdefault((row.session, row.replay), []).append(row)
        inserted = 0
        for (session, is_replay), rows in list(self._pending.items()):
            added = self._store.insert_samples(rows)
            del self._pending[(session, is_replay)]
            counters = self._sessions[session]
            if is_replay:
                counters.replay_received += len(rows)
                counters.replay_inserted += added
            else:
                counters.live_inserted += added
            self._dirty.add(session)
            inserted += added
        for session in sorted(self._dirty):
            self._store.upsert_session(self._stats(session, self._sessions[session]))
        self._dirty.clear()
        return inserted

    def aggregate(self, since_us: int | None = None) -> int:
        """Recompute the OEE windows between ``since_us`` and the watermark.

        By default the last ``recompute_s`` seconds are redone on every call, so
        samples that arrive late (replays, redeliveries) correct the windows
        they belong to. Returns the number of windows written.
        """
        watermark = self._store.latest_timestamp(CELL_STATION, SIGNAL_HEARTBEAT)
        if watermark is None:
            return 0
        start_us = watermark - self._recompute_us if since_us is None else since_us
        start_us -= start_us % self._window_us
        windows: list[tuple[WindowTotals, bool]] = []
        for station in self._store.production_stations():
            ideal = Series(self._store.series(station, SIGNAL_IDEAL, start_us, watermark))
            ideal_cycle_s = ideal.last_value()
            if ideal_cycle_s is None:
                continue
            states = Series(self._store.series(station, SIGNAL_STATE, start_us, watermark))
            totals = Series(self._store.series(station, SIGNAL_TOTAL, start_us, watermark))
            goods = Series(self._store.series(station, SIGNAL_GOOD, start_us, watermark))
            for window_start, window_end in window_bounds(start_us, watermark, self._window_us):
                covered_end = min(window_end, watermark)
                window = totals_from_series(
                    station, states, totals, goods, ideal_cycle_s, window_start, covered_end
                )
                if window.covered_us == 0:
                    continue
                window = dataclasses.replace(window, end_us=window_end)
                windows.append((window, window_end <= watermark))
        self._store.upsert_oee(windows)
        return len(windows)

    def run(
        self,
        stop: threading.Event,
        heartbeat: Heartbeat | None = None,
        is_connected: Callable[[], bool] = lambda: True,
    ) -> None:
        """Flush and aggregate until ``stop`` is set, surviving store outages.

        The heartbeat only beats while the store answers and ``is_connected``
        holds, so the container is healthy exactly when data can flow.
        """
        next_aggregate = time.monotonic()
        while not stop.is_set():
            try:
                self.flush()
                if time.monotonic() >= next_aggregate:
                    self.aggregate()
                    next_aggregate = time.monotonic() + AGGREGATE_INTERVAL_S
                if heartbeat is not None and is_connected():
                    heartbeat.beat()
            except Exception:
                _log.exception("Store error; reconnecting in %.0f s", STORE_RETRY_DELAY_S)
                stop.wait(STORE_RETRY_DELAY_S)
                with contextlib.suppress(Exception):
                    self._store.reset()
                continue
            stop.wait(FLUSH_INTERVAL_S)

    def _on_message(self, topic: str, payload: bytes, retained: bool) -> None:
        self._inbox.put((topic, payload, retained, self._clock()))

    def _decode(self, topic: str, payload: bytes, received_us: int) -> SampleRow | None:
        try:
            _, station, signal = parse_data_topic(topic)
            message = Telemetry.from_json(payload)
        except (TopicError, PayloadError) as error:
            self.rejected += 1
            _log.warning("Rejected message on %s: %s", topic, error)
            return None
        if signal == SIGNAL_STATE and message.value not in STATE_VALUES:
            self.rejected += 1
            _log.warning("Rejected message on %s: %r is not a station state", topic, message.value)
            return None
        return SampleRow(
            station=station,
            signal=signal,
            source_us=message.source_us,
            value=message.value,
            session=message.session,
            seq=message.seq,
            gateway_us=message.gateway_us,
            collector_us=received_us,
            replay=message.replay,
        )

    def _stats(self, session: str, counters: _SessionCounters) -> SessionStats:
        tracker = counters.tracker
        return SessionStats(
            session=session,
            updated_us=self._clock(),
            received=tracker.received,
            duplicates=tracker.duplicates,
            missing=tracker.missing,
            highest_seq=tracker.highest,
            live_inserted=counters.live_inserted,
            replay_received=counters.replay_received,
            replay_inserted=counters.replay_inserted,
            retained=counters.retained,
        )


def main() -> None:
    from usine40.mqtt_bus import MqttBus
    from usine40.store import open_postgres, open_sqlite

    parser = argparse.ArgumentParser(description="Store one cell's MQTT telemetry and its OEE.")
    parser.parse_args()
    settings = Settings.from_env()
    configure_logging()
    stop = threading.Event()
    install_stop_signals(stop.set)

    store = _open_store_with_retry(settings, stop, open_postgres, open_sqlite)
    if store is None:
        return
    bus = MqttBus(settings.mqtt_host, settings.mqtt_port, "usine40-collector", clean_session=False)
    collector = Collector(
        bus,
        store,
        settings.path,
        qos=settings.mqtt_qos,
        window_s=settings.window_s,
        recompute_s=max(settings.recompute_s, settings.backfill_s),
    )
    collector.start()
    bus.start()
    _log.info("Collector started, window %.0f s", settings.window_s)
    try:
        collector.run(stop, Heartbeat(settings.health_file), lambda: bus.connected)
    finally:
        bus.close()
        store.close()


def _open_store_with_retry(
    settings: Settings,
    stop: threading.Event,
    open_postgres: Callable[[str], SqlStore],
    open_sqlite: Callable[[str], SqlStore],
) -> SqlStore | None:
    while not stop.is_set():
        try:
            if settings.database_url.startswith("sqlite:"):
                return open_sqlite(settings.database_url.removeprefix("sqlite:"))
            return open_postgres(settings.database_url)
        except Exception as error:
            _log.warning("Database not ready (%s); retrying", error)
            stop.wait(STORE_RETRY_DELAY_S)
    return None


if __name__ == "__main__":
    main()
