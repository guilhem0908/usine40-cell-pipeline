"""Loss and duplicate accounting from gateway sequence numbers."""

from __future__ import annotations


class SequenceTracker:
    """Follows the sequence numbers of one gateway session.

    The gateway numbers its messages 1, 2, 3, ... per session. On the receiving
    side a number seen twice is a duplicate (QoS 1 redelivery), and a number
    skipped is missing until it shows up late or for good. The first number
    observed is taken as the start of the stream, so a collector that joins
    late does not report the past as lost.
    """

    def __init__(self) -> None:
        self._first: int | None = None
        self._highest = 0
        self._missing: set[int] = set()
        self.received = 0
        self.duplicates = 0

    @property
    def missing(self) -> int:
        """Messages published before the highest one seen that never arrived."""
        return len(self._missing)

    @property
    def highest(self) -> int:
        return self._highest

    def observe(self, seq: int) -> bool:
        """Record one message; returns False if it is a duplicate."""
        self.received += 1
        if self._first is None:
            self._first = self._highest = seq
            return True
        if seq > self._highest:
            self._missing.update(range(self._highest + 1, seq))
            self._highest = seq
            return True
        if seq in self._missing:
            self._missing.discard(seq)
            return True
        self.duplicates += 1
        return False
