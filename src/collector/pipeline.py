from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import OrderedDict
from typing import Any, Protocol

log = logging.getLogger(__name__)


class Sink(Protocol):
    async def write(self, table: str, rows: list[dict[str, Any]], *, merge: bool) -> None: ...


class Pipeline:
    """Deduplicating multi-table write buffer with retry-safe flush."""

    def __init__(
        self,
        sink: Sink,
        *,
        flush_rows: int = 500,
        flush_interval_sec: float = 5.0,
        max_buffered_rows: int = 50_000,
    ) -> None:
        self._sink = sink
        self._flush_rows = max(1, flush_rows)
        self._flush_interval = max(0.5, flush_interval_sec)
        self._max = max_buffered_rows
        self._buffers: dict[tuple[str, bool], OrderedDict[tuple[Any, ...], dict[str, Any]]] = {}

    @property
    def pending_rows(self) -> int:
        return sum(len(buffer) for buffer in self._buffers.values())

    def add(self, table: str, key: tuple[Any, ...], row: dict[str, Any], *, merge: bool = False) -> None:
        buffer = self._buffers.setdefault((table, merge), OrderedDict())
        buffer[key] = row
        if self.pending_rows > self._max:
            self._trim()

    def add_model(self, model: Any, *, merge: bool = False) -> None:
        self.add(model.table, model.key, model.to_row(), merge=merge)

    def _trim(self) -> None:
        for buffer in self._buffers.values():
            while buffer and self.pending_rows > self._max:
                buffer.popitem(last=False)
        log.warning("pipeline buffer exceeded limit; oldest rows dropped")

    async def flush(self) -> None:
        for (table, merge), buffer in list(self._buffers.items()):
            if not buffer:
                continue
            items = list(buffer.items())
            for offset in range(0, len(items), self._flush_rows):
                chunk = items[offset : offset + self._flush_rows]
                rows = [row for _, row in chunk]
                try:
                    await self._sink.write(table, rows, merge=merge)
                except Exception:
                    log.exception("sink write failed: table=%s rows=%d", table, len(rows))
                    return
                for key, _ in chunk:
                    buffer.pop(key, None)
            if not buffer:
                self._buffers.pop((table, merge), None)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._flush_interval)
            await self.flush()
        await self.flush()
