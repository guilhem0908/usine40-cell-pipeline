# Design notes

The README gives the numbers. This page gives the reasoning behind each choice, so that every one of them can be defended.

## 1. The simulated cell

Three stations in series separated by two buffers of four parts: `infeed` (ideal cycle 1.6 s), `machining` (2.0 s, the bottleneck) and `inspection` (1.4 s). The model is advanced like a PLC program, one fixed scan of 50 ms at a time (`sim.py`).

* **Scan-based, not event-driven.** Every duration is an integer number of scans, so a run is reproducible bit for bit from the seed and every timestamp falls on an exact microsecond grid. That is what allows the pipeline output to be compared with the ground truth for equality instead of with a tolerance.
* **Per-station random streams.** Each station draws from `random.Random(f"{seed}/{name}")`. Adding a station does not change the draws of the others.
* **Cycle time `ideal * (1 + X)`, `X ~ Exp(mean = slowdown)`.** A station never beats its ideal cycle, so performance stays at or below one (apart from a part that straddles a window boundary).
* **Breakdowns.** Time between failures is exponential and counted in run time; repair time is exponential. Scripted faults and live `InjectFault` calls (clamped to 120 s) go through the same path, so the injected fault is a real state `FAULT`, not a flag on the dashboard.
* **BLOCKED and STARVED emerge** from the buffers; they are not drawn. This is why the bottleneck shows a high availability and its neighbours do not.
* **What it is not.** It is a Python model of a controller, not IEC 61131-3 code, and its five states are a simplification of PackML (they do not implement its state machine).

## 2. OEE definition

For one station over one window: planned time `T_p` = covered time minus IDLE time, run time `T_r` = RUNNING time, `A = T_r / T_p`, `P = C_ideal * N / T_r`, `Q = N_good / N`, `OEE = A * P * Q = C_ideal * N_good / T_p`.

* IDLE is a planned stop of the whole cell and is excluded from planned time. BLOCKED, STARVED and FAULT count against availability. Some plants book blocked and starved time as a loss of the line instead of the machine; the choice is made in one place (`WindowTotals`) and stated here because it changes the ranking of stations.
* Over several windows the ratios are recomputed from summed durations and counts (`oee.combine`). Averaging per-window ratios would weight a quiet window like a busy one.
* The three-factor decomposition is Nakajima's; ISO 22400-2 names the same three ratios (availability, effectiveness, quality ratio). The text of the standard was not consulted for this project.

## 3. Ground truth and why agreement is meaningful

`ground_truth.py` is a second implementation that works on a different representation: it walks the simulator's own events (state changes, individual finished parts) and spreads each state interval over the windows it crosses. The pipeline instead reads stored samples, slices them per window and differentiates cumulative counters. They share the formulas of section 2 and nothing else.

An error of zero therefore says the plumbing (OPC UA, MQTT, SQL, window slicing) lost or invented nothing. It does not say that OEE is the right KPI, nor that the formulas are right; those are definitions.

## 4. Transport

* **Timestamps are integer microseconds** from the PLC scan to the database key. Floats would round differently at each hop and break idempotent inserts.
* **A sample is identified by `(station, signal, source_us)`.** That primary key makes MQTT redelivery and history replay harmless: the second insert is ignored.
* **Counters are cumulative.** A lost counter sample does not lose the part: the next sample carries the running total, so the totals over the whole run survive. But the part is counted in the window of the sample that carries it, so a loss moves parts from the windows of the gap into a later window, and with them the per-window OEE (possibly above 100 % in the later window, below the truth in the others). A lost state change is worse in another way: nothing later restores it, the station keeps its previous state until the next change, and availability and performance are wrong. Their product, OEE, is not, as long as the window has no planned stop, because `OEE = C_ideal * N_good / T_p` does not depend on how the planned time is split between states. The outage experiments report both effects separately: the worst availability error and, for the windows whose OEE is wrong, whether the part count was wrong too.
* **Sessions and sequence numbers.** The gateway numbers its messages per session; the collector turns gaps and repeats into `missing` and `duplicates` per session (`SequenceTracker`). Retained messages handed over at subscription time are not part of the live stream and are not counted.
* **Retained telemetry.** Publishing with the retain flag lets a late subscriber receive the current value of every signal, including the ideal cycle time that never changes.
* **History replay.** After a start the gateway subscribes first and then re-reads the last `backfill_s` seconds from the server's OPC UA history. Subscribing first makes the replay and the live stream overlap rather than leave a gap; the overlap is removed by the primary key. The first value a subscription delivers can be arbitrarily old and is flagged `replay` like the history, so it cannot pollute the latency statistics.
* **QoS 1** for telemetry. QoS 0 is kept as a measurable option, not a recommendation.
* **Not Sparkplug B.** The topic tree is ISA-95-shaped (site / area / cell / station / signal) with a version level; payloads are plain JSON documented by `telemetry.schema.json`.

## 5. Collector and watermark

OEE windows are computed only up to the watermark, the source timestamp of the last stored PLC heartbeat. When data stops, the figures stop; the last known state is not stretched up to the present. The last `recompute_s` seconds are recomputed on every pass so that late samples (replays, redeliveries) correct the windows they belong to. A window is flagged `complete` once the watermark has passed its end.

The collector also validates what a message means, not only its shape: a `state` sample whose value is not one of the five station states is counted in `rejected` and dropped, so that a buggy or hostile publisher on the anonymous broker cannot put a row in the database that stops the aggregation. `Series.durations_by_state` ignores such a value as well, in case a row got in by another route.

## 6. How latency is measured

Each stored row carries four times: `source_us` (PLC scan), `gateway_us` (publish), `collector_us` (MQTT message received) and `db_us` (database clock at insert). The hops are their differences. `db_us` comes from the database server's clock, and the harness reads "now" from the same clock, so no offset between the Windows host and the Docker VM enters any duration. The PLC, gateway and collector containers share the VM clock.

Latency percentiles use live rows only (`replay = false`). Under Docker Desktop on Windows the containers run in a Linux VM; the figures are those of one laptop and are labelled as such.

## 7. Alerts

Two rules are provisioned, evaluated every 10 s by Grafana's scheduler: a station whose last state is FAULT, and no PLC heartbeat stored for more than 10 s. A 10 s evaluation tick bounds the detection delay of an injected breakdown from above by the tick plus the pipeline latency; the measured delays are in the README. `noDataState: OK` is deliberate: the stale-data rule exists so that missing data does not silently look healthy.

## 8. Deployment

* One image for the three Python services; Mosquitto, PostgreSQL 17 and Grafana 11 are used unchanged.
* Every service has a healthcheck and `depends_on` waits for health. The gateway waits for the collector so that the collector holds its subscription before the first publish.
* All ports are published on `127.0.0.1` only. The passwords in `compose.yaml` are demonstration defaults, overridable through `.env`.
* PostgreSQL is used as a plain relational store, not as a time-series extension: at the volumes measured here an indexed `BIGINT` key is sufficient, and the schema runs unchanged on SQLite for the tests.
