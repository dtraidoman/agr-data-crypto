from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable, Sequence
from typing import Any

import websockets

from ..models import Liquidation, Trade
from ..timeutil import now_ms
from .base import backoff_seconds
from .parsers import parse_bybit_liquidation, parse_bybit_trade

log = logging.getLogger(__name__)


class BybitWs:
    name = "bybit"
    WS_URL = "wss://stream.bybit.com/v5/public/linear"

    def __init__(self, *, planned_reconnect_sec: int = 21_600, max_backoff_sec: int = 30) -> None:
        self._planned_reconnect_sec = planned_reconnect_sec
        self._max_backoff_sec = max_backoff_sec

    async def _ping(self, ws: Any, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                await asyncio.sleep(20)
                await ws.send(json.dumps({"op": "ping"}))
        except Exception:
            return

    async def _run(
        self,
        topics: Sequence[str],
        handler: Callable[[dict[str, Any]], None],
        stop: asyncio.Event,
        connected: asyncio.Event | None,
    ) -> None:
        attempt = 0
        while not stop.is_set():
            started = now_ms()
            ping_task: asyncio.Task[None] | None = None
            try:
                async with websockets.connect(self.WS_URL, ping_interval=None, max_size=2**24) as ws:
                    attempt = 0
                    await ws.send(json.dumps({"op": "subscribe", "args": list(topics)}))
                    if connected is not None:
                        connected.set()
                    ping_task = asyncio.create_task(self._ping(ws, stop))
                    log.info("bybit ws connected: %s", ",".join(topics))
                    while not stop.is_set():
                        if now_ms() - started > self._planned_reconnect_sec * 1000:
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=60)
                        except TimeoutError:
                            continue
                        payload = json.loads(raw)
                        if payload.get("op") in {"pong", "subscribe"} or "ret_msg" in payload:
                            continue
                        data = payload.get("data")
                        if isinstance(data, dict):
                            data = [data]
                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict):
                                    handler(item)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                log.warning("bybit ws error: %s", exc)
                await asyncio.sleep(backoff_seconds(attempt, self._max_backoff_sec))
            finally:
                if ping_task is not None:
                    ping_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await ping_task

    async def stream_trades(
        self,
        symbols: Sequence[str],
        on_trade: Callable[[Trade], None],
        stop: asyncio.Event,
        connected: asyncio.Event | None = None,
    ) -> None:
        topics = [f"publicTrade.{symbol.upper()}" for symbol in symbols]

        def handler(item: dict[str, Any]) -> None:
            try:
                symbol = str(item.get("s", "")).upper()
                on_trade(parse_bybit_trade(item, symbol))
            except (KeyError, ValueError, TypeError):
                log.debug("bad bybit trade payload", exc_info=True)

        await self._run(topics, handler, stop, connected)

    async def stream_liquidations(
        self,
        symbols: Sequence[str],
        on_liquidation: Callable[[Liquidation], None],
        stop: asyncio.Event,
        connected: asyncio.Event | None = None,
    ) -> None:
        topics = [f"allLiquidation.{symbol.upper()}" for symbol in symbols]

        def handler(item: dict[str, Any]) -> None:
            try:
                symbol = str(item.get("s", "")).upper()
                on_liquidation(parse_bybit_liquidation(item, symbol))
            except (KeyError, ValueError, TypeError):
                log.debug("bad bybit liquidation payload", exc_info=True)

        await self._run(topics, handler, stop, connected)
