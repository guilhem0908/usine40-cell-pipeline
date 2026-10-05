"""Topic tree, payload contract, sequence accounting and the broker substitute."""

from __future__ import annotations

import json
from importlib import resources

import jsonschema
import pytest

from usine40.bus import InMemoryBroker, topic_matches
from usine40.payload import PayloadError, Telemetry
from usine40.sequence import SequenceTracker
from usine40.topics import (
    CellPath,
    TopicError,
    data_filter,
    data_topic,
    parse_data_topic,
    status_topic,
)

PATH = CellPath("site1", "assembly", "cell1")
MESSAGE = Telemetry("9f2c41d07a3e", 1842, 1_759_696_245_150_000, 1_759_696_245_231_907, 4.0)


def _schema() -> dict:
    text = resources.files("usine40").joinpath("schemas/telemetry.schema.json").read_text("utf-8")
    return json.loads(text)


def _altered(**changes: object) -> bytes:
    document = json.loads(MESSAGE.to_json())
    for key, value in changes.items():
        if value is ...:
            del document[key]
        else:
            document[key] = value
    return json.dumps(document).encode()


def test_data_topic_round_trips_through_the_parser():
    topic = data_topic(PATH, "machining", "state")
    assert topic == "usine40/v1/site1/assembly/cell1/data/machining/state"
    assert parse_data_topic(topic) == (PATH, "machining", "state")


def test_cell_filter_matches_telemetry_but_not_status():
    topic_filter = data_filter(PATH)
    assert topic_matches(topic_filter, data_topic(PATH, "infeed", "total_count"))
    assert not topic_matches(topic_filter, status_topic(PATH, "gateway"))
    other_cell = CellPath("site1", "assembly", "cell2")
    assert not topic_matches(topic_filter, data_topic(other_cell, "infeed", "state"))


@pytest.mark.parametrize("level", ["", "a/b", "a+b", "#", "x\x00"])
def test_topic_levels_cannot_contain_separators_or_wildcards(level):
    with pytest.raises(TopicError):
        data_topic(PATH, level, "state")
    with pytest.raises(TopicError):
        CellPath("site1", level, "cell1")


@pytest.mark.parametrize(
    "topic",
    [
        "usine40/v1/site1/assembly/cell1/status/gateway",
        "usine40/v2/site1/assembly/cell1/data/machining/state",
        "factory/v1/site1/assembly/cell1/data/machining/state",
        "usine40/v1/site1/assembly/cell1/data/machining",
        "usine40/v1/site1/assembly/cell1/status/machining/state",
        "usine40/v1/site1/assembly/cell1/data//state",
    ],
)
def test_parser_rejects_topics_outside_the_telemetry_tree(topic):
    with pytest.raises(TopicError):
        parse_data_topic(topic)


@pytest.mark.parametrize(
    ("topic_filter", "topic", "expected"),
    [
        ("sport/tennis/player1/#", "sport/tennis/player1", True),
        ("sport/tennis/player1/#", "sport/tennis/player1/ranking", True),
        ("sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon", True),
        ("sport/#", "sport", True),
        ("#", "any/topic/at/all", True),
        ("sport/tennis/+", "sport/tennis/player1", True),
        ("sport/tennis/+", "sport/tennis/player1/ranking", False),
        ("sport/+", "sport", False),
        ("sport/+", "sport/", True),
        ("+/+", "/finance", True),
        ("/+", "/finance", True),
        ("+", "/finance", False),
        ("sport/tennis", "sport/tennis/player1", False),
        ("sport/tennis/player1", "sport/tennis", False),
    ],
)
def test_topic_filters_follow_the_mqtt_specification_examples(topic_filter, topic, expected):
    assert topic_matches(topic_filter, topic) is expected


def test_payload_round_trips_with_integer_timestamps_intact():
    decoded = Telemetry.from_json(MESSAGE.to_json())
    assert decoded == MESSAGE
    assert json.loads(MESSAGE.to_json())["source_us"] == 1_759_696_245_150_000


@pytest.mark.parametrize(
    "payload",
    [
        b"not json",
        b"\xff\xfe",
        b"[1, 2, 3]",
        _altered(seq=...),
        _altered(extra=1),
        _altered(v=2),
        _altered(v=True),
        _altered(seq=-1),
        _altered(seq=True),
        _altered(seq="12"),
        _altered(source_us=1.5),
        _altered(session=""),
        _altered(session=7),
        _altered(value="4"),
        _altered(value=None),
        _altered(value=True),
        _altered(replay=0),
        b'{"v":1,"session":"s","seq":1,"source_us":1,"gateway_us":1,"value":NaN,"replay":false}',
    ],
)
def test_malformed_payloads_are_rejected(payload):
    with pytest.raises(PayloadError):
        Telemetry.from_json(payload)


def test_encoder_refuses_non_finite_values():
    with pytest.raises(ValueError, match="Out of range"):
        Telemetry("s", 1, 1, 1, float("inf")).to_json()


def test_decoder_and_published_json_schema_accept_the_same_documents():
    validator = jsonschema.Draft202012Validator(_schema())
    jsonschema.Draft202012Validator.check_schema(_schema())
    valid = [MESSAGE.to_json(), _altered(replay=True), _altered(value=3), _altered(seq=0)]
    invalid = [
        _altered(seq=...),
        _altered(extra=1),
        _altered(v=2),
        _altered(seq=-1),
        _altered(seq="12"),
        _altered(source_us=1.5),
        _altered(session=""),
        _altered(value="4"),
        _altered(replay=0),
    ]
    for payload in valid:
        assert validator.is_valid(json.loads(payload))
        Telemetry.from_json(payload)
    for payload in invalid:
        assert not validator.is_valid(json.loads(payload))
        with pytest.raises(PayloadError):
            Telemetry.from_json(payload)


def test_in_order_stream_has_no_loss_and_no_duplicate():
    tracker = SequenceTracker()
    assert all(tracker.observe(seq) for seq in range(1, 101))
    assert (tracker.received, tracker.duplicates, tracker.missing) == (100, 0, 0)


def test_gap_is_missing_until_the_late_message_arrives():
    tracker = SequenceTracker()
    for seq in (1, 2, 6, 7):
        tracker.observe(seq)
    assert tracker.missing == 3
    assert tracker.observe(4) is True
    assert tracker.missing == 2
    assert tracker.highest == 7


def test_repeated_sequence_number_is_a_duplicate():
    tracker = SequenceTracker()
    assert [tracker.observe(seq) for seq in (1, 2, 2, 3, 1)] == [True, True, False, True, False]
    assert tracker.duplicates == 2
    assert tracker.missing == 0


def test_late_joiner_does_not_report_the_past_as_lost():
    tracker = SequenceTracker()
    tracker.observe(5000)
    tracker.observe(5001)
    assert tracker.missing == 0
    assert tracker.observe(4999) is False


def test_broker_substitute_delivers_retained_message_to_late_subscriber():
    broker = InMemoryBroker()
    broker.client().publish("a/status", b"online", qos=1, retain=True)
    broker.client().publish("a/data", b"1", qos=1)
    received: list[tuple[str, bytes, bool]] = []
    broker.client().subscribe("a/#", 1, lambda *message: received.append(message))
    assert received == [("a/status", b"online", True)]
    broker.client().publish("a/data", b"2", qos=1)
    assert received[-1] == ("a/data", b"2", False)


def test_broker_substitute_drops_qos0_and_queues_qos1_during_an_outage():
    broker = InMemoryBroker()
    received: list[bytes] = []
    broker.client().subscribe("t", 1, lambda _topic, payload, _retained: received.append(payload))
    publisher = broker.client()
    publisher.publish("t", b"before", qos=1)
    broker.stop()
    publisher.publish("t", b"lost", qos=0)
    publisher.publish("t", b"queued-1", qos=1)
    publisher.publish("t", b"queued-2", qos=1)
    assert received == [b"before"]
    broker.start()
    assert received == [b"before", b"queued-1", b"queued-2"]
    assert broker.dropped == 1


def test_broker_substitute_redelivers_unacknowledged_qos1_messages():
    broker = InMemoryBroker()
    received: list[bytes] = []
    broker.client().subscribe("t", 1, lambda _topic, payload, _retained: received.append(payload))
    publisher = broker.client()
    for index in range(5):
        publisher.publish("t", str(index).encode(), qos=1)
    broker.stop()
    publisher.publish("t", b"5", qos=1)
    broker.start(redeliver=2)
    assert received == [b"0", b"1", b"2", b"3", b"4", b"3", b"4", b"5"]
