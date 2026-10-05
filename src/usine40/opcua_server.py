"""OPC UA face of the simulated PLC (asyncua server).

Address space, under ``Objects``::

    Cell1                          Object
      Heartbeat                    UInt32, +1 every second
      LoadChangesPerSecond         Double, writable, clamped by the PLC
      InjectFault(station, s)      Method, returns the accepted duration
      infeed | machining | ...     Object, one per station
        State                      Int32  (0 IDLE, 1 RUNNING, 2 BLOCKED, 3 STARVED, 4 FAULT)
        TotalCount, GoodCount      UInt32 cumulative counters
        IdealCycleTime             Double, seconds
      sensors                      Object
        analog_000 ...             Double, synthetic tags for load tests

Every value is written with an explicit SourceTimestamp taken from the
simulator scan, and the server keeps a bounded in-memory history of each
production signal so that a gateway can re-read what it missed (HistoryRead).
The endpoint is unencrypted and anonymous: this is a local demonstrator.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from asyncua import Server, ua

from usine40.model import (
    CELL_STATION,
    SENSORS_STATION,
    SIGNAL_GOOD,
    SIGNAL_HEARTBEAT,
    SIGNAL_IDEAL,
    SIGNAL_STATE,
    SIGNAL_TOTAL,
    Sample,
)
from usine40.timebase import us_to_datetime

NAMESPACE_URI = "urn:usine40:cell"
SIGNAL_LOAD_RATE = "load_changes_per_s"
INJECT_FAULT_METHOD = "InjectFault"
HISTORY_DEPTH = 20_000
"""Values kept per node; about 2.7 hours of state changes at two per second."""

BROWSE_NAMES = {
    SIGNAL_STATE: "State",
    SIGNAL_TOTAL: "TotalCount",
    SIGNAL_GOOD: "GoodCount",
    SIGNAL_IDEAL: "IdealCycleTime",
    SIGNAL_HEARTBEAT: "Heartbeat",
    SIGNAL_LOAD_RATE: "LoadChangesPerSecond",
}
SIGNAL_NAMES = {browse_name: signal for signal, browse_name in BROWSE_NAMES.items()}

_VARIANT_TYPES = {
    SIGNAL_STATE: ua.VariantType.Int32,
    SIGNAL_TOTAL: ua.VariantType.UInt32,
    SIGNAL_GOOD: ua.VariantType.UInt32,
    SIGNAL_HEARTBEAT: ua.VariantType.UInt32,
}
_INTEGER_TYPES = (ua.VariantType.Int32, ua.VariantType.UInt32)


def analog_signal(index: int) -> str:
    return f"analog_{index:03d}"


class CellServer:
    """Builds the address space and writes timestamped samples into it."""

    def __init__(
        self,
        endpoint: str,
        cell_name: str,
        stations: Iterable[str],
        *,
        inject_fault: Callable[[str, float], float] | None = None,
        analog_tags: int = 0,
    ) -> None:
        self._endpoint = endpoint
        self._cell_name = cell_name
        self._stations = tuple(stations)
        self._inject_fault = inject_fault
        self._analog_tags = analog_tags
        self._server = Server()
        self._nodes: dict[tuple[str, str], tuple[ua.NodeId, ua.VariantType]] = {}
        self._load_rate_node = None

    async def build(self) -> None:
        """Create the address space; the server does not accept clients yet."""
        await self._server.init()
        self._server.set_endpoint(self._endpoint)
        self._server.set_server_name("usine40 simulated cell")
        self._server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        namespace = await self._server.register_namespace(NAMESPACE_URI)
        cell = await self._server.nodes.objects.add_object(namespace, self._cell_name)

        async def add(parent, station: str, signal: str, initial: float):
            variant_type = _VARIANT_TYPES.get(signal, ua.VariantType.Double)
            browse_name = BROWSE_NAMES.get(signal, signal)
            owner = "" if station == CELL_STATION else f".{station}"
            node_id = ua.NodeId(f"{self._cell_name}{owner}.{browse_name}", namespace)
            value = int(initial) if variant_type in _INTEGER_TYPES else float(initial)
            qualified_name = ua.QualifiedName(browse_name, namespace)
            node = await parent.add_variable(node_id, qualified_name, value, variant_type)
            self._nodes[(station, signal)] = (node.nodeid, variant_type)
            return node

        await add(cell, CELL_STATION, SIGNAL_HEARTBEAT, 0)
        self._load_rate_node = await add(cell, CELL_STATION, SIGNAL_LOAD_RATE, 0.0)
        await self._load_rate_node.set_writable()
        for station in self._stations:
            station_object = await cell.add_object(namespace, station)
            for signal in (SIGNAL_STATE, SIGNAL_TOTAL, SIGNAL_GOOD, SIGNAL_IDEAL):
                await add(station_object, station, signal, 0)
        if self._analog_tags:
            sensors = await cell.add_object(namespace, SENSORS_STATION)
            for index in range(self._analog_tags):
                await add(sensors, SENSORS_STATION, analog_signal(index), 0.0)
        if self._inject_fault is not None:
            await cell.add_method(
                ua.NodeId(f"{self._cell_name}.{INJECT_FAULT_METHOD}", namespace),
                ua.QualifiedName(INJECT_FAULT_METHOD, namespace),
                self._handle_inject_fault,
                [_argument("station", ua.VariantType.String), _argument("duration_s")],
                [_argument("accepted_s")],
            )

    async def open(self) -> None:
        """Start accepting clients and start recording the history of production signals."""
        await self._server.start()
        historized = [
            self._server.get_node(node_id)
            for (station, signal), (node_id, _) in self._nodes.items()
            if station != SENSORS_STATION and signal != SIGNAL_LOAD_RATE
        ]
        await self._server.historize_node_data_change(historized, period=None, count=HISTORY_DEPTH)

    async def stop(self) -> None:
        await self._server.stop()

    async def write(self, samples: Iterable[Sample]) -> None:
        """Write samples to their nodes with the sample time as source timestamp."""
        for sample in samples:
            node_id, variant_type = self._nodes[(sample.station, sample.signal)]
            integer = variant_type in _INTEGER_TYPES
            value = int(sample.value) if integer else float(sample.value)
            moment = us_to_datetime(sample.ts_us)
            data_value = ua.DataValue(
                ua.Variant(value, variant_type), SourceTimestamp=moment, ServerTimestamp=moment
            )
            await self._server.write_attribute_value(node_id, data_value)

    async def read_load_rate(self) -> float:
        """Current value of the writable load setpoint (changes per second)."""
        return float(await self._load_rate_node.read_value())

    async def _handle_inject_fault(
        self, _parent: ua.NodeId, station: ua.Variant, duration: ua.Variant
    ) -> list[ua.Variant]:
        try:
            accepted = self._inject_fault(str(station.Value), float(duration.Value))
        except (TypeError, ValueError):
            accepted = 0.0
        return [ua.Variant(accepted, ua.VariantType.Double)]


def _argument(name: str, variant_type: ua.VariantType = ua.VariantType.Double) -> ua.Argument:
    argument = ua.Argument()
    argument.Name = name
    argument.DataType = ua.NodeId(int(variant_type.value))
    argument.ValueRank = -1
    return argument
