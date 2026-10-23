from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Sequence
from typing import Any

import websockets

from ..models import Liquidation, Trade
from ..timeutil import now_ms
from .base import backoff_seconds
from .parsers import parse_binance_agg_trade, parse_binance_force_order

log = logging.getLogger(__name__)


class BinanceWs:
    name = "binance"
    WS_BASE = "wss://fstream.binance.com"

    def __init__(self, *, planned_reconnect_sec: int = 21_600, max_backoff_sec: int = 30) -> None:
        self._planned_reconnect_sec = planned_reconnect_sec
        self._max_backoff_sec = max_backoff_sec

    async def _run(
        self,
        url: str,
        handler: Callable[[dict[str, Any]], None],
        stop: asyncio.Event,
        connected: asyncio.Event | None,
    ) -> None:
        attempt = 0
        while not stop.is_set():
            started = now_ms()
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**24) as ws:
                    attempt = 0
                    if connected is not None:
                        connected.set()
                    log.info("binance ws connected: %s", url)
                    while not stop.is_set():
                        if now_ms() - started > self._planned_reconnect_sec * 1000:
                            log.info("binance ws planned reconnect")
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=60)
                        except TimeoutError:
                            continue
                        payload = json.loads(raw)
                        data = payload.get("data", payload)
                        if isinstance(data, dict):
                            handler(data)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                log.warning("binance ws error (%s): %s", url, exc)
                await asyncio.sleep(backoff_seconds(attempt, self._max_backoff_sec))

    async def stream_trades(
        self,
        symbols: Sequence[str],
        on_trade: Callable[[Trade], None],
        stop: asyncio.Event,
        connected: asyncio.Event | None = None,
    ) -> None:
        streams = "/".join(f"{symbol.lower()}@aggTrade" for symbol in symbols)
        url = f"{self.WS_BASE}/stream?streams={streams}"

        def handler(data: dict[str, Any]) -> None:
            if data.get("e") == "aggTrade":
                on_trade(parse_binance_agg_trade(data))

        await self._run(url, handler, stop, connected)

    async def stream_liquidations(
        self,
        symbols: Sequence[str],
        on_liquidation: Callable[[Liquidation], None],
        stop: asyncio.Event,
        connected: asyncio.Event | None = None,
    ) -> None:
        allowed = {symbol.upper() for symbol in symbols}
        url = f"{self.WS_BASE}/ws/!forceOrder@arr"

        def handler(data: dict[str, Any]) -> None:
            if data.get("e") != "forceOrder":
                return
            try:
                event = parse_binance_force_order(data)
            except (KeyError, ValueError, TypeError):
                log.debug("bad binance liquidation payload", exc_info=True)
                return
            if event.symbol in allowed:
                on_liquidation(event)

        await self._run(url, handler, stop, connected)
