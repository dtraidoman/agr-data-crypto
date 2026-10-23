from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from typing import Protocol

from ..models import Liquidation, LongShort, OpenInterest, Trade


class RestAdapter(Protocol):
    name: str

    async def fetch_open_interest(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> list[OpenInterest]: ...

    async def fetch_long_short(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> list[LongShort]: ...

    async def fetch_recent_trades(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> list[Trade]: ...


class WsAdapter(Protocol):
    name: str

    async def stream_trades(
        self,
        symbols: Sequence[str],
        on_trade: Callable[[Trade], None],
        stop: asyncio.Event,
        connected: asyncio.Event | None = None,
    ) -> None: ...

    async def stream_liquidations(
        self,
        symbols: Sequence[str],
        on_liquidation: Callable[[Liquidation], None],
        stop: asyncio.Event,
        connected: asyncio.Event | None = None,
    ) -> None: ...


def backoff_seconds(attempt: int, maximum: int = 30) -> float:
    return min(maximum, 2 ** min(attempt, 6))
