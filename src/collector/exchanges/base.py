from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from typing import Protocol

from ..models import Candle, Stream


class ExchangeAdapter(Protocol):
    name: str

    async def stream_klines(
        self,
        symbols: Sequence[str],
        interval: str,
        on_candle: Callable[[Candle], None],
        stop: asyncio.Event,
    ) -> None: ...

    async def backfill_klines(self, stream: Stream, start_ms: int, end_ms: int) -> list[Candle]: ...


class CompositeAdapter:
    """Pairs a websocket feed with a REST catch-up client under one name."""

    def __init__(self, name: str, ws: ExchangeAdapter, rest: ExchangeAdapter) -> None:
        self.name = name
        self._ws = ws
        self._rest = rest

    async def stream_klines(
        self,
        symbols: Sequence[str],
        interval: str,
        on_candle: Callable[[Candle], None],
        stop: asyncio.Event,
    ) -> None:
        await self._ws.stream_klines(symbols, interval, on_candle, stop)

    async def backfill_klines(self, stream: Stream, start_ms: int, end_ms: int) -> list[Candle]:
        return await self._rest.backfill_klines(stream, start_ms, end_ms)


def backoff_seconds(
    attempt: int,
    *,
    base: float = 1.0,
    cap: float = 30.0,
    jitter: float = 0.25,
) -> float:
    """Exponential backoff with jitter, capped.

    Jitter matters on GitHub runners: without it, two roles reconnecting after
    a network blip can synchronise their attempts and look like a flood.
    """
    import random

    raw = min(cap, base * (2 ** max(0, attempt - 1)))
    return raw * (1.0 + random.uniform(-jitter, jitter))
