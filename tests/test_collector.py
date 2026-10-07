"""Store idempotency and collector behaviour on the broker substitute."""

from __future__ import annotations

import pytest

from usine40.bus import InMemoryBroker
from usine40.collector import Collector
from usine40.model import (
    CELL_STATION,
    SIGNAL_GOOD,
    SIGNAL_HEARTBEAT,
    SIGNAL_IDEAL,
    SIGNAL_STATE,
    SIGNAL_TOTAL,
    State,
)
from usine40.oee import WindowTotals
from usine40.payload import Telemetry
from usine40.store import SampleRow, SessionStats, open_sqlite
from usine40.topics import CellPath, data_topic

S = 1_000_000
PATH = CellPath("site1", "assembly", "cell1")


def _row(signal: str, source_us: int, value: float, seq: int = 1, replay: bool = False):
    stamps = (source_us, source_us)
    return SampleRow("machining", signal, source_us, value, "sess", seq, *stamps, replay)


class Feed:
    """Publishes telemetry the way a gateway session would."""

    def __init__(self, broker: InMemoryBroker, session: str = "sess") -> None:
        self._bus = broker.client()
        self._session = session
        self.seq = 0

    def send(self, station: str, signal: str, ts_s: float, value: float, **overrides) -> None:
        """Publish one sample; ``seq``, ``replay`` and ``retain`` can be overridden."""
        self.seq += 1
        message = Telemetry(
            session=self._session,
            seq=overrides.get("seq", self.seq),
            source_us=round(ts_s * S),
            gateway_us=round(ts_s * S),
            value=float(value),
            replay=overrides.get("replay", False),
        )
        retain = overrides.get("retain", False)
        topic = data_topic(PATH, station, signal)
        self._bus.publish(topic, message.to_json(), qos=1, retain=retain)

    def heartbeat(self, ts_s: float) -> None:
        self.send(CELL_STATION, SIGNAL_HEARTBEAT, ts_s, ts_s)


@pytest.fixture
def pipeline():
    broker = InMemoryBroker()
    store = open_sqlite()
    collector = Collector(broker.client(), store, PATH, window_s=30.0, recompute_s=300.0)
    collector.start()
    yield Feed(broker), collector, store, broker
    store.close()


def test_inserting_the_same_sample_twice_stores_it_once():
    store = open_sqlite()
    rows = [_row(SIGNAL_STATE, 10 * S, 1.0), _row(SIGNAL_STATE, 20 * S, 4.0, seq=2)]
    assert store.insert_samples(rows) == 2
    assert store.insert_samples(rows) == 0
    assert store.insert_samples([*rows, _row(SIGNAL_STATE, 30 * S, 1.0, seq=3)]) == 1
    assert len(store.sample_keys()) == 3


def test_first_write_wins_when_a_key_is_replayed_with_another_value():
    store = open_sqlite()
    store.insert_samples([_row(SIGNAL_STATE, 10 * S, 1.0)])
    store.insert_samples([_row(SIGNAL_STATE, 10 * S, 4.0, replay=True)])
    assert store.series("machining", SIGNAL_STATE, 0, 60 * S) == [(10 * S, 1.0)]


def test_series_returns_the_last_point_before_the_range_then_the_range():
    store = open_sqlite()
    store.insert_samples(
        [_row(SIGNAL_STATE, t * S, float(v), seq=t) for t, v in [(5, 1), (20, 4), (40, 1), (70, 2)]]
    )
    assert store.series("machining", SIGNAL_STATE, 30 * S, 60 * S) == [(20 * S, 4.0), (40 * S, 1.0)]
    assert store.series("machining", SIGNAL_STATE, 0, 5 * S) == []
    assert store.latest_timestamp("machining", SIGNAL_STATE) == 70 * S
    assert store.latest_timestamp("machining", SIGNAL_TOTAL) is None
    assert store.production_stations() == ["machining"]


def test_oee_window_upsert_replaces_the_previous_version():
    store = open_sqlite()
    draft = WindowTotals("machining", 0, 30 * S, (0, 10 * S, 0, 0, 0), 4, 4, 2.0)
    final = WindowTotals("machining", 0, 30 * S, (0, 28 * S, 0, 0, 2 * S), 13, 12, 2.0)
    store.upsert_oee([(draft, False)])
    assert store.oee_windows(complete_only=True) == []
    assert store.oee_windows(complete_only=False) == [draft]
    store.upsert_oee([(final, True)])
    assert store.oee_windows(complete_only=True) == [final]


def test_session_statistics_are_upserted():
    store = open_sqlite()
    store.upsert_session(SessionStats("a", 1, 10, 0, 0, 10, 10, 0, 0, 0))
    store.upsert_session(SessionStats("a", 2, 25, 3, 1, 23, 21, 0, 0, 4))
    rows = store.query("SELECT session, received, duplicates, missing FROM ingest_session")
    assert rows == [("a", 25, 3, 1)]


def test_collector_stores_each_message_once_and_counts_wire_duplicates(pipeline):
    feed, collector, store, _ = pipeline
    feed.send("machining", SIGNAL_STATE, 1.0, State.RUNNING)
    feed.send("machining", SIGNAL_STATE, 2.0, State.FAULT)
    feed.send("machining", SIGNAL_STATE, 2.0, State.FAULT, seq=2)
    assert collector.flush() == 2
    (stats,) = collector.session_stats()
    assert (stats.received, stats.duplicates, stats.live_inserted) == (3, 1, 2)
    assert len(store.sample_keys()) == 2


def test_collector_counts_a_sequence_gap_as_missing(pipeline):
    feed, collector, _, _ = pipeline
    feed.send("machining", SIGNAL_TOTAL, 1.0, 1)
    feed.seq += 4
    feed.send("machining", SIGNAL_TOTAL, 9.0, 6)
    collector.flush()
    (stats,) = collector.session_stats()
    assert stats.missing == 4
    assert stats.highest_seq == 6


def test_collector_rejects_malformed_messages_without_storing_them(pipeline):
    feed, collector, store, broker = pipeline
    raw = broker.client()
    raw.publish(data_topic(PATH, "machining", SIGNAL_STATE), b'{"value": 1}', qos=1)
    raw.publish(data_topic(PATH, "machining", SIGNAL_STATE), b"garbage", qos=1)
    feed.send("machining", SIGNAL_STATE, 1.0, State.RUNNING)
    assert collector.flush() == 1
    assert collector.rejected == 2
    assert len(store.sample_keys()) == 1


@pytest.mark.parametrize("value", [9.0, 5.0, -1.0, 2.5])
def test_collector_drops_a_state_value_that_is_not_a_station_state(pipeline, value):
    feed, collector, store, _ = pipeline
    _produce(feed, 20)
    feed.send("machining", SIGNAL_STATE, 10.0, value)
    feed.heartbeat(20.0)
    collector.flush()
    assert collector.rejected == 1
    assert [v for _, v in store.series("machining", SIGNAL_STATE, 0, 60 * S)] == [State.RUNNING]
    assert collector.aggregate() == 1
    (window,) = store.oee_windows(complete_only=False)
    assert window.state_us[State.RUNNING] == 20 * S


def test_replaying_samples_that_are_already_stored_adds_nothing(pipeline):
    feed, collector, store, broker = pipeline
    for second in range(1, 6):
        feed.send("machining", SIGNAL_TOTAL, float(second), second)
    collector.flush()
    restarted = Feed(broker, session="after-restart")
    for second in range(3, 8):
        restarted.send("machining", SIGNAL_TOTAL, float(second), second, replay=True)
    assert collector.flush() == 2
    stats = {s.session: s for s in collector.session_stats()}
    assert stats["after-restart"].replay_received == 5
    assert stats["after-restart"].replay_inserted == 2
    assert len(store.sample_keys()) == 7


def test_late_collector_learns_current_values_from_retained_messages():
    broker = InMemoryBroker()
    feed = Feed(broker)
    feed.send("machining", SIGNAL_IDEAL, 0.0, 2.0, retain=True)
    feed.send("machining", SIGNAL_STATE, 1.0, State.RUNNING, retain=True)
    feed.send("machining", SIGNAL_STATE, 5.0, State.FAULT, retain=True)
    store = open_sqlite()
    collector = Collector(broker.client(), store, PATH)
    collector.start()
    assert collector.flush() == 2
    assert store.series("machining", SIGNAL_IDEAL, 0, 60 * S) == [(0, 2.0)]
    assert store.series("machining", SIGNAL_STATE, 0, 60 * S) == [(5 * S, 4.0)]


def test_retained_copies_are_not_counted_as_wire_duplicates(pipeline):
    feed, collector, store, _ = pipeline
    feed.send("machining", SIGNAL_STATE, 1.0, State.RUNNING, retain=True)
    feed.send("machining", SIGNAL_TOTAL, 2.0, 1, retain=True)
    collector.flush()
    collector.start()  # a reconnection subscribes again and gets the stored copies
    assert collector.flush() == 0
    (stats,) = collector.session_stats()
    assert (stats.received, stats.duplicates, stats.retained) == (2, 0, 2)
    assert len(store.sample_keys()) == 2


def _produce(feed: Feed, until_s: int) -> None:
    """One part every 2 s from t=0, always good, station always running."""
    feed.send("machining", SIGNAL_IDEAL, 0.0, 2.0)
    feed.send("machining", SIGNAL_STATE, 0.0, State.RUNNING)
    feed.send("machining", SIGNAL_TOTAL, 0.0, 0)
    feed.send("machining", SIGNAL_GOOD, 0.0, 0)
    for part, second in enumerate(range(2, until_s + 1, 2), start=1):
        feed.send("machining", SIGNAL_TOTAL, float(second), part)
        feed.send("machining", SIGNAL_GOOD, float(second), part)


def test_no_oee_is_computed_before_the_first_heartbeat(pipeline):
    feed, collector, store, _ = pipeline
    _produce(feed, 60)
    collector.flush()
    assert collector.aggregate() == 0
    assert store.oee_windows(complete_only=False) == []


def test_oee_stops_at_the_watermark_instead_of_stretching_the_last_state(pipeline):
    feed, collector, store, _ = pipeline
    _produce(feed, 40)
    feed.heartbeat(40.0)
    collector.flush()
    collector.aggregate()
    complete = store.oee_windows(complete_only=True)
    everything = store.oee_windows(complete_only=False)
    assert [(w.start_us, w.total) for w in complete] == [(0, 14)]
    assert complete[0].oee == pytest.approx(2.0 * 14 / 30)
    open_window = everything[-1]
    assert open_window.start_us == 30 * S
    assert open_window.covered_us == 10 * S


def test_late_samples_correct_a_window_that_was_already_complete(pipeline):
    feed, collector, store, _ = pipeline
    _produce(feed, 60)
    feed.heartbeat(65.0)
    collector.flush()
    collector.aggregate()
    assert store.oee_windows()[0].availability == pytest.approx(1.0)

    feed.send("machining", SIGNAL_STATE, 10.0, State.FAULT, replay=True)
    feed.send("machining", SIGNAL_STATE, 25.0, State.RUNNING, replay=True)
    collector.flush()
    collector.aggregate()
    first = store.oee_windows()[0]
    assert first.state_us[State.FAULT] == 15 * S
    assert first.availability == pytest.approx(0.5)


def test_station_without_an_ideal_cycle_time_is_skipped(pipeline):
    feed, collector, store, _ = pipeline
    feed.send("machining", SIGNAL_STATE, 0.0, State.RUNNING)
    feed.heartbeat(45.0)
    collector.flush()
    assert collector.aggregate() == 0
    assert store.oee_windows(complete_only=False) == []
