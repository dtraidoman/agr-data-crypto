from __future__ import annotations

import logging

from .base import Sink

log = logging.getLogger(__name__)


class MemorySink(Sink):
    """In-process sink with the same dedup semantics as the SQL primary key."""

    def __init__(self) -> None:
        self.rows: dict[tuple[str, str, str, int], dict] = {}
        self.writes = 0
        self.duplicates = 0

    async def write(self, rows: list[dict]) -> int:
        accepted = 0
        for row in rows:
            key = (row["exchange"], row["symbol"], row["interval"], int(row["open_time"]))
            if key in self.rows:
                self.duplicates += 1
                continue
            self.rows[key] = row
            accepted += 1
        self.writes += 1
        return accepted

    async def close(self) -> None:
        return None
