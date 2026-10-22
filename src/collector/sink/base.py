from __future__ import annotations

from typing import Protocol


class Sink(Protocol):
    """Destination for closed candles.

    Contract: ``write`` is idempotent. Writing the same row twice is a no-op,
    never an update, never an error. This is what makes at-least-once
    collection safe.
    """

    async def write(self, rows: list[dict]) -> int: ...

    async def close(self) -> None: ...
