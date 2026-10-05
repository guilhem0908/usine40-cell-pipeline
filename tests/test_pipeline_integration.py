"""End-to-end runs without Docker: real OPC UA server, gateway and collector in one process.

The broker is the in-memory substitute and the store is SQLite, so these tests
check the logic of the chain (nothing lost, nothing counted twice, OEE equal to
the ground truth, behaviour around restarts). Timings and the behaviour of a
real broker are measured by the compose scenario instead.
"""

from __future__ import annotations

import asyncio

import pytest
from asyncua import Client

from usine40.model import CELL_STATION, SIGNAL_HEARTBEAT, SIGNAL_STATE, State
from usine40.oee import combine
from usine40.opcua_server import INJECT_FAULT_METHOD, NAMESPACE_URI, SIGNAL_LOAD_RATE
from usine40.plc import LoadGenerator
from usine40.rig import CELL_NAME, Rig
from usine40.sim import MAX_INJECTED_FAULT_S, FaultWindow, PlannedStop, default_cell

from cells import steady_cell

pytestmark = pytest.mark.integration


def _worst_error(comparison) -> float:
    return max(error.max_pp for error in comparison.errors.values())


@pytest.mark.parametrize("seed", [1, 2])
def test_pipeline_oee_equals_ground_truth_on_every_window(seed):
    async def scenario():
        stops = (PlannedStop(120.0, 40.0),)
        async with Rig(default_cell(seed, planned_stops=stops)) as rig:
            await rig.advance(600)
            return rig.compare(), rig.missing()

    comparison, missing = asyncio.run(scenario())
    assert missing == 0
    assert comparison.matched == 3 * 20
    assert comparison.truth_only == comparison.pipeline_only == 0
    assert comparison.count_mismatches == 0
    assert _worst_error(comparison) < 1e-9


def test_gateway_discovers_stations_and_signals_by_browsing():
    async def scenario():
        async with Rig(steady_cell()) as rig:
            return set(rig.gateway._signals.values())

    signals = asyncio.run(scenario())
    assert (CELL_STATION, SIGNAL_HEARTBEAT) in signals
    assert (CELL_STATION, SIGNAL_LOAD_RATE) in signals
    for station in ("infeed", "machining", "inspection"):
        assert {s for st, s in signals if st == station} == {
            "state",
            "total_count",
            "good_count",
            "ideal_cycle_s",
        }


def test_scripted_breakdown_reaches_the_store_with_its_exact_duration():
    async def scenario():
        faults = (FaultWindow("machining", 40.0, 17.5),)
        async with Rig(steady_cell(scripted_faults=faults)) as rig:
            await rig.advance(120)
            pipeline = rig.pipeline()
            return combine([w for w in pipeline if w.station == "machining"])

    machining = asyncio.run(scenario())
    assert machining.state_us[State.FAULT] == 17_500_000


def test_gateway_restart_without_replay_loses_the_transitions_in_between():
    async def scenario():
        async with Rig(default_cell(3), backfill_s=0.0) as rig:
            await rig.advance(90)
            await rig.stop_gateway()
            await rig.advance(60)
            await rig.start_gateway()
            await rig.advance(90)
            truth = {w.station: w for w in map(combine, _by_station(rig.truth()))}
            pipeline = {w.station: w for w in map(combine, _by_station(rig.pipeline()))}
            return rig.missing(), rig.compare(), truth, pipeline

    missing, comparison, truth, pipeline = asyncio.run(scenario())
    assert missing > 0
    assert _worst_error(comparison) > 1.0
    # Cumulative counters make part counts self-healing even though samples were lost.
    for station in truth:
        assert pipeline[station].total == truth[station].total
        assert pipeline[station].good == truth[station].good


def test_gateway_restart_with_history_replay_recovers_everything():
    async def scenario():
        async with Rig(default_cell(3), backfill_s=120.0) as rig:
            await rig.advance(90)
            await rig.stop_gateway()
            await rig.advance(60)
            await rig.start_gateway()
            await rig.advance(90)
            sessions = rig.collector.session_stats()
            return rig.missing(), rig.compare(), sessions

    missing, comparison, sessions = asyncio.run(scenario())
    assert missing == 0
    assert comparison.count_mismatches == 0
    assert _worst_error(comparison) < 1e-9
    assert len(sessions) == 2
    assert sessions[1].replay_inserted > 0
    assert sessions[1].replay_received > sessions[1].replay_inserted


def test_outage_longer_than_the_replay_horizon_is_only_partly_recovered():
    async def scenario():
        async with Rig(default_cell(3), backfill_s=20.0) as rig:
            await rig.advance(60)
            await rig.stop_gateway()
            await rig.advance(60)
            await rig.start_gateway()
            await rig.advance(30)
            return rig.missing()

    assert asyncio.run(scenario()) > 0


def test_broker_outage_loses_qos0_messages_but_not_qos1_messages():
    async def scenario(qos: int):
        async with Rig(default_cell(4), qos=qos, backfill_s=0.0) as rig:
            await rig.advance(60)
            rig.broker.stop()
            await rig.advance(30)
            rig.broker.start()
            await rig.advance(60)
            (stats,) = rig.collector.session_stats()
            return rig.missing(), stats, rig.broker.dropped

    missing_qos0, stats_qos0, dropped = asyncio.run(scenario(0))
    assert missing_qos0 == dropped > 0
    assert stats_qos0.missing == dropped

    missing_qos1, stats_qos1, dropped = asyncio.run(scenario(1))
    assert (missing_qos1, stats_qos1.missing, dropped) == (0, 0, 0)


def test_redelivered_qos1_messages_are_counted_but_not_stored_twice():
    async def scenario():
        async with Rig(default_cell(4), qos=1, backfill_s=0.0) as rig:
            await rig.advance(60)
            rig.broker.stop()
            await rig.advance(10)
            rig.broker.start(redeliver=8)
            await rig.advance(50)
            (stats,) = rig.collector.session_stats()
            return rig.missing(), stats, rig.compare()

    missing, stats, comparison = asyncio.run(scenario())
    assert stats.duplicates == 8
    assert stats.live_inserted == stats.received - stats.duplicates
    assert missing == 0
    assert _worst_error(comparison) < 1e-9


def test_fault_injected_through_the_opc_ua_method_is_applied_and_bounded():
    async def scenario():
        async with Rig(steady_cell()) as rig:
            await rig.advance(20)
            async with Client(rig._url) as client:
                namespace = await client.get_namespace_index(NAMESPACE_URI)
                cell = await client.nodes.objects.get_child(f"{namespace}:{CELL_NAME}")
                method = f"{namespace}:{INJECT_FAULT_METHOD}"
                accepted = await cell.call_method(method, "machining", 12.0)
                unknown = await cell.call_method(method, "lathe", 12.0)
                huge = await cell.call_method(method, "infeed", 1e9)
            await rig.advance(40)
            states = rig.store.series("machining", SIGNAL_STATE, 0, rig.watermark_us())
            return accepted, unknown, huge, states

    accepted, unknown, huge, states = asyncio.run(scenario())
    assert accepted == pytest.approx(12.0)
    assert unknown == 0.0
    assert huge == pytest.approx(MAX_INJECTED_FAULT_S)
    fault_at = next(ts for ts, value in states if value == State.FAULT)
    resumed_at = next(ts for ts, value in states if ts > fault_at)
    assert resumed_at - fault_at == 12_000_000


def test_load_generator_respects_its_rate_limit_and_numbers_updates_per_tag():
    load = LoadGenerator(tags=10, scan_period_s=0.05)
    assert load.max_rate == pytest.approx(200.0)
    assert load.set_rate(1e9) == pytest.approx(200.0)
    assert load.set_rate(-3.0) == 0.0
    assert load.set_rate(float("nan")) == 0.0
    load.set_rate(50.0)
    samples = [sample for scan in range(200) for sample in load.samples(scan * 50_000)]
    assert len(samples) == pytest.approx(50.0 * 200 * 0.05, abs=1)
    assert len({sample.key for sample in samples}) == len(samples)
    for tag in {sample.signal for sample in samples}:
        values = [sample.value for sample in samples if sample.signal == tag]
        assert values == [float(n) for n in range(1, len(values) + 1)]


def _by_station(windows):
    stations = sorted({window.station for window in windows})
    return [[w for w in windows if w.station == station] for station in stations]
