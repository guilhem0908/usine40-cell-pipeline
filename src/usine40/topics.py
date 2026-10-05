"""MQTT topic tree.

The hierarchy follows the ISA-95 equipment levels (site / area / work cell)
and is versioned so that a payload change can live next to the old one::

    usine40/v1/{site}/{area}/{cell}/data/{station}/{signal}    telemetry, JSON
    usine40/v1/{site}/{area}/{cell}/status/{component}         retained liveness

It is a plain JSON-over-MQTT convention, not Sparkplug B.
"""

from __future__ import annotations

from dataclasses import dataclass

ROOT = "usine40"
VERSION = "v1"
DATA = "data"
STATUS = "status"

_FORBIDDEN = frozenset("/+#\x00")
_DATA_LEVELS = 8


class TopicError(ValueError):
    """Raised for a topic that does not belong to the tree above."""


def _check_level(level: str) -> str:
    if not level or any(char in _FORBIDDEN for char in level):
        raise TopicError(f"Invalid topic level {level!r}.")
    return level


@dataclass(frozen=True, slots=True)
class CellPath:
    """Location of a cell in the plant hierarchy."""

    site: str
    area: str
    cell: str

    def __post_init__(self) -> None:
        for level in (self.site, self.area, self.cell):
            _check_level(level)

    @property
    def prefix(self) -> str:
        return f"{ROOT}/{VERSION}/{self.site}/{self.area}/{self.cell}"


def data_topic(path: CellPath, station: str, signal: str) -> str:
    return f"{path.prefix}/{DATA}/{_check_level(station)}/{_check_level(signal)}"


def data_filter(path: CellPath) -> str:
    """Subscription filter matching every telemetry topic of the cell."""
    return f"{path.prefix}/{DATA}/+/+"


def status_topic(path: CellPath, component: str) -> str:
    return f"{path.prefix}/{STATUS}/{_check_level(component)}"


def parse_data_topic(topic: str) -> tuple[CellPath, str, str]:
    """Split a telemetry topic into ``(cell path, station, signal)``."""
    levels = topic.split("/")
    if len(levels) != _DATA_LEVELS or levels[0] != ROOT or levels[1] != VERSION:
        raise TopicError(f"Not a {ROOT}/{VERSION} telemetry topic: {topic!r}.")
    if levels[5] != DATA:
        raise TopicError(f"Not a telemetry topic: {topic!r}.")
    site, area, cell, station, signal = levels[2], levels[3], levels[4], levels[6], levels[7]
    return CellPath(site, area, cell), _check_level(station), _check_level(signal)
