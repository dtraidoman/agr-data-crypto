from __future__ import annotations

import asyncio
import json
from typing import Any

from pytest import MonkeyPatch

from collector.exchanges import bybit_ws
from collector.exchanges.bybit_ws import BybitWs
from collector.models import Trade
from collector.pipeline import Pipeline
from collector.sink import MemorySink
from collector.streams import CvdStream


class FakeConnection:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def __aenter__(self) -> FakeConnection:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def send(self, raw: str) -> None:
        self.sent.append(raw)

    async def recv(self) -> str:
        await asyncio.sleep(0.01)
        return json.dumps(
            {
                "topic": "publicTrade.BTCUSDT",
                "type": "snapshot",
                "data": [{"i": "1", "p": "1", "v": "1", "T": 1, "S": "Buy", "s": "BTCUSDT"}],
            }
        )


async def test_bybit_ws_stop_does_not_leak_cancelled_error(monkeypatch: MonkeyPatch) -> None:
    connection = FakeConnection()
    monkeypatch.setattr(bybit_ws.websockets, "connect", lambda *args, **kwargs: connection)
    stop = asyncio.Event()
    ws = BybitWs()

    task = asyncio.create_task(ws._run(["publicTrade.BTCUSDT"], lambda item: None, stop, None))
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, timeout=5)


class CancellingWs:
    name = "bybit"

    async def stream_trades(
        self,
        symbols: list[str],
        on_trade: Any,
        stop: asyncio.Event,
        connected: asyncio.Event | None = None,
    ) -> None:
        if connected is not None:
            connected.set()
        while not stop.is_set():
            await asyncio.sleep(0.01)


class EmptyRest:
    name = "bybit"

    async def fetch_recent_trades(self, symbol: str, start_ms: int, end_ms: int) -> list[Trade]:
        return []


async def test_cvd_stream_shutdown_absorbs_websocket_cancellation() -> None:
    sink = MemorySink()
    pipeline = Pipeline(sink)
    stream = CvdStream(
        CancellingWs(),  # type: ignore[arg-type]
        EmptyRest(),  # type: ignore[arg-type]
        sink,
        pipeline,
        "bybit",
        ["BTCUSDT"],
    )
    stop = asyncio.Event()

    task = asyncio.create_task(stream.run(stop))
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, timeout=5)
