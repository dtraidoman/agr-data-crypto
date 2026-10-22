from __future__ import annotations

import time
from collections.abc import Iterable

from .models import Candle, Stream
from .sink.base import Sink


class Pipeline:
    """In-memory buffer between the exchange feed and the sink.

    Two invariants make the at-least-once design safe:

    1. Only closed candles enter the buffer, so a row never needs updating.
    2. A cursor only moves *after* the rows up to it are durably written.
       If the process dies between write and cursor flush, the successor
       re-fetches from the old cursor and the primary key rejects duplicates.
    """

    def __init__(
        self,
        sink: Sink,
        *,
        flush_rows: int = 200,
        flush_interval_sec: float = 5.0,
    ) -> None:
        self._sink = sink
        self._flush_rows = max(1, flush_rows)
        self._flush_interval = max(0.1, flush_interval_sec)
        self._rows: dict[tuple[str, str, str, int], dict] = {}
        self._pending: dict[Stream, int] = {}
        self._cursors: dict[Stream, int] = {}
        self._last_flush = time.monotonic()
        self.stats: dict[str, int] = {
            "received": 0,
            "buffered_duplicates": 0,
            "written": 0,
            "flushes": 0,
            "flush_errors": 0,
        }

    # ------------------------------------------------------------------ input
    def add(self, candles: Iterable[Candle]) -> int:
        added = 0
        for candle in candles:
            if not candle.is_closed:
                continue
            key = candle.pk
            if key in self._rows:
                self.stats["buffered_duplicates"] += 1
            self._rows[key] = candle.to_row()
            stream = candle.stream
            if candle.open_time > self._pending.get(stream, 0):
                self._pending[stream] = candle.open_time
            self.stats["received"] += 1
            added += 1
        return added

    # ---------------------------------------------------------------- cursors
    @property
    def cursors(self) -> dict[Stream, int]:
        """Committed cursors: safe to publish to Redis."""
        return dict(self._cursors)

    @property
    def buffered(self) -> int:
        return len(self._rows)

    def due(self) -> bool:
        if not self._rows:
            return False
        if len(self._rows) >= self._flush_rows:
            return True
        return (time.monotonic() - self._last_flush) >= self._flush_interval

    # ------------------------------------------------------------------ output
    async def flush(self, force: bool = False) -> int:
        if not self._rows:
            return 0
        if not force and not self.due():
            return 0

        keys = list(self._rows)
        rows = [self._rows[key] for key in keys]
        pending_snapshot = dict(self._pending)

        try:
            written = await self._sink.write(rows)
        except Exception:
            self.stats["flush_errors"] += 1
            # Rows stay in the buffer and will be retried; cursors stay put.
            raise

        for key in keys:
            self._rows.pop(key, None)

        for stream, value in pending_snapshot.items():
            if value > self._cursors.get(stream, 0):
                self._cursors[stream] = value
            # A candle may have arrived while we were awaiting the sink; only
            # clear the value we actually persisted.
            if self._pending.get(stream) == value:
                self._pending.pop(stream, None)

        self._last_flush = time.monotonic()
        self.stats["written"] += written
        self.stats["flushes"] += 1
        return written

    async def drain(self) -> int:
        return await self.flush(force=True)

    async def close(self) -> None:
        await self._sink.close()
