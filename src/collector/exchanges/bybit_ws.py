from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections import deque
from collections.abc import Callable, Sequence

from websockets.asyncio.client import connect

from ..models import Candle
from ..timeutil import now_ms
from .base import backoff_seconds
from .parsers import bybit_interval_code, parse_bybit_ws_kline

log = logging.getLogger(__name__)

PING_INTERVAL_SEC = 20.0


class BybitWs:
    """One public spot connection; all symbols go into a single SUBSCRIBE.

    Bybit requires the *client* to send {"op":"ping"} every 20 seconds.
    Unlike Binance, the server does not ping us, and there is no automatic
    pong: forgetting this is a guaranteed disconnect every 20-30 seconds.
    """

    name = "bybit"
    URL = "wss://stream.bybit.com/v5/public/spot"

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
        code = bybit_interval_code(interval)
        topics = [f"kline.{code}.{symbol}" for symbol in symbols]
        attempt = 0
        attempts: deque[float] = deque()

        while not stop.is_set():
            await self._pace_attempts(attempts)
            pinger: asyncio.Task | None = None
            try:
                async with connect(
                    self.URL,
                    ping_interval=None,  # heartbeat is application-level here
                    close_timeout=5,
                    max_queue=2048,
                    open_timeout=15,
                ) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": topics}))
                    pinger = asyncio.create_task(self._ping_loop(ws, stop))
                    log.info("bybit ws connected: %s", ",".join(symbols))
                    attempt = 0
                    connected_at = time.monotonic()
                    while not stop.is_set():
                        if time.monotonic() - connected_at > self._planned_reconnect_sec:
                            log.info("bybit planned reconnect after %ss", self._planned_reconnect_sec)
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except TimeoutError:
                            continue
                        candle = parse_bybit_ws_kline(raw, now_ms())
                        if candle is not None:
                            on_candle(candle)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                delay = backoff_seconds(min(attempt, 6), cap=self._max_backoff_sec)
                log.warning("bybit ws error (%s), reconnect in %.1fs", exc, delay)
                if await _sleep_or_stop(stop, delay):
                    return
            finally:
                if pinger is not None:
                    pinger.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await pinger

    async def _ping_loop(self, ws, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                await asyncio.sleep(PING_INTERVAL_SEC)
                await ws.send(json.dumps({"op": "ping"}))
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - the reader loop owns the reconnect
            return

    async def _pace_attempts(self, attempts: deque[float]) -> None:
        now = time.monotonic()
        while attempts and now - attempts[0] > 60:
            attempts.popleft()
        if len(attempts) >= self._max_attempts_per_minute:
            wait = 60 - (now - attempts[0])
            log.warning("bybit ws attempt rate cap reached, waiting %.1fs", wait)
            await asyncio.sleep(max(0.0, wait))
        attempts.append(time.monotonic())


async def _sleep_or_stop(stop: asyncio.Event, delay: float) -> bool:
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay)
        return True
    except TimeoutError:
        return False
