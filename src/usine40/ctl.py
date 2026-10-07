"""Operator commands sent to the simulated PLC over OPC UA.

    usine40-ctl fault machining 20     # break the machining station down for 20 s
    usine40-ctl load 500               # make the PLC change 500 analog tags per second

Both commands are bounded on the PLC side: a fault lasts at most
``MAX_INJECTED_FAULT_S`` and the load is clamped to what the tag set allows.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from asyncua import Client, Node, ua

from usine40.config import ENV_PREFIX
from usine40.opcua_server import BROWSE_NAMES, INJECT_FAULT_METHOD, NAMESPACE_URI, SIGNAL_LOAD_RATE

DEFAULT_URL = "opc.tcp://127.0.0.1:14840/usine40"


async def _cell(client: Client) -> tuple[Node, int]:
    namespace = await client.get_namespace_index(NAMESPACE_URI)
    for description in await client.nodes.objects.get_children_descriptions():
        if description.NodeId.NamespaceIndex == namespace:
            return client.get_node(description.NodeId), namespace
    raise ua.UaError(f"No cell object found in namespace {NAMESPACE_URI}.")


async def inject_fault(url: str, station: str, duration_s: float) -> float:
    """Call the PLC's InjectFault method; returns the accepted duration (0 if refused)."""
    async with Client(url) as client:
        cell, namespace = await _cell(client)
        method = f"{namespace}:{INJECT_FAULT_METHOD}"
        return float(await cell.call_method(method, station, float(duration_s)))


async def set_load(url: str, changes_per_s: float) -> None:
    """Write the synthetic load setpoint; the PLC clamps it on its next housekeeping pass."""
    async with Client(url) as client:
        cell, namespace = await _cell(client)
        node = await cell.get_child(f"{namespace}:{BROWSE_NAMES[SIGNAL_LOAD_RATE]}")
        setpoint = ua.Variant(float(changes_per_s), ua.VariantType.Double)
        await node.write_value(ua.DataValue(setpoint))


def main() -> None:
    parser = argparse.ArgumentParser(description="Send a command to the simulated PLC.")
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"OPC UA endpoint (default {DEFAULT_URL}, the port published by compose.yaml; "
        f"inside the compose network use the {ENV_PREFIX}OPCUA_URL value)",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    fault = commands.add_parser("fault", help="inject a breakdown")
    fault.add_argument("station")
    fault.add_argument("seconds", type=float)
    load = commands.add_parser("load", help="set the synthetic load in tag changes per second")
    load.add_argument("changes_per_s", type=float)
    arguments = parser.parse_args()
    logging.getLogger("asyncua").setLevel(logging.ERROR)

    if arguments.command == "fault":
        accepted = asyncio.run(inject_fault(arguments.url, arguments.station, arguments.seconds))
        if accepted <= 0:
            raise SystemExit(f"The PLC refused the fault on station {arguments.station!r}.")
        print(f"Fault accepted on {arguments.station} for {accepted:g} s.")
    else:
        asyncio.run(set_load(arguments.url, arguments.changes_per_s))
        print(f"Load setpoint written: {arguments.changes_per_s:g} changes/s.")


if __name__ == "__main__":
    main()
