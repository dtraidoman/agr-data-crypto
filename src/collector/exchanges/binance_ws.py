from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable, Sequence

from websockets.asyncio.client import connect

from ..models import Candle
from ..timeutil import now_ms
from .base import backoff_seconds
from .parsers import parse_binance_ws_kline

log = logging.getLogger(__name__)


class BinanceWs:
    """One combined-stream connection for all symbols of the role.

    Binance allows 200-1024 streams per connection, so 3 kline streams use
    ~1% of the budget. The binding constraints are elsewhere: the 24h
    connection lifetime and the connection-attempt rate limit per IP.
    """

    name = "binance"
    BASE = "wss://stream.binance.com:9443"

    def __init__(
        self,
        *,
        planned_reconnect_sec: int = 6 * 3600,
        max_backoff_sec: int = 30,
        max_attempts_per_minute: int = 5,
    ) -> None:
        self._planned_reconnect_sec = planned_reconnect_sec
        self._max_backoff_sec = max_backoff_sec
        self._max_attempts_per_minute = max_attempts_per_minute

    async def stream_klines(
        self,
        symbols: Sequence[str],
        interval: str,
        on_candle: Callable[[Candle], None],
        stop: asyncio.Event,
    ) -> None:
        if not symbols:
            return
        streams = "/".join(f"{symbol.lower()}@kline_{interval}" for symbol in symbols)
        url = f"{self.BASE}/stream?streams={streams}"
        attempt = 0
        attempts: deque[float] = deque()

        while not stop.is_set():
            await self._pace_attempts(attempts)
            try:
                async with connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=2048,
                    open_timeout=15,
                ) as ws:
                    log.info("binance ws connected: %s", ",".join(symbols))
                    attempt = 0
                    connected_at = time.monotonic()
                    while not stop.is_set():
                        if time.monotonic() - connected_at > self._planned_reconnect_sec:
                            log.info("binance planned reconnect after %ss", self._planned_reconnect_sec)
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except TimeoutError:
                            continue
                        candle = parse_binance_ws_kline(raw, now_ms())
                        if candle is not None:
                            on_candle(candle)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - any socket error means "reconnect"
                attempt += 1
                delay = backoff_seconds(min(attempt, 6), cap=self._max_backoff_sec)
                log.warning("binance ws error (%s), reconnect in %.1fs", exc, delay)
                if await _sleep_or_stop(stop, delay):
                    return

    async def _pace_attempts(self, attempts: deque[float]) -> None:
        now = time.monotonic()
        while attempts and now - attempts[0] > 60:
            attempts.popleft()
        if len(attempts) >= self._max_attempts_per_minute:
            wait = 60 - (now - attempts[0])
            log.warning("binance ws attempt rate cap reached, waiting %.1fs", wait)
            await asyncio.sleep(max(0.0, wait))
        attempts.append(time.monotonic())


async def _sleep_or_stop(stop: asyncio.Event, delay: float) -> bool:
    """Sleep, but wake up immediately if shutdown was requested."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay)
        return True
    except TimeoutError:
        return False
