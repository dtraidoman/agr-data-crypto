from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Sequence
from typing import Protocol

from .aggregation import CvdAggregator, aggregate_window
from .exchanges.base import RestAdapter, WsAdapter
from .models import Liquidation, Trade
from .pipeline import Pipeline
from .timeutil import now_ms

log = logging.getLogger(__name__)

FIVE_MIN_MS = 300_000
MINUTE_MS = 60_000
SEED_CHUNK_MS = 10 * MINUTE_MS
MAX_SEED_CHUNKS = 36


class LatestSink(Protocol):
    async def write(self, table: str, rows: list[dict], *, merge: bool) -> None: ...

    async def latest_ts(self, table: str, exchange: str, symbol: str) -> int | None: ...


class MetricsPoller:
    def __init__(
        self,
        rest: RestAdapter,
        pipeline: Pipeline,
        symbols: Sequence[str],
        *,
        poll_sec: int = 300,
        startup_lookback_sec: int = 86_400,
        overlap_sec: int = 900,
    ) -> None:
        self._rest = rest
        self._pipeline = pipeline
        self._symbols = list(symbols)
        self._poll_sec = max(60, poll_sec)
        self._startup_lookback = startup_lookback_sec
        self._overlap = overlap_sec

    async def run(self, stop: asyncio.Event) -> None:
        first = True
        while not stop.is_set():
            lookback = self._startup_lookback if first else self._overlap
            end = now_ms() - 5_000
            start = end - lookback * 1000
            for symbol in self._symbols:
                try:
                    for row in await self._rest.fetch_open_interest(symbol, start, end):
                        self._pipeline.add_model(row, merge=True)
                    for row in await self._rest.fetch_long_short(symbol, start, end):
                        self._pipeline.add_model(row, merge=True)
                except Exception:
                    log.exception("metrics poll failed for %s %s", self._rest.name, symbol)
            log.info("metrics poll %s lookback=%ds pending=%d", self._rest.name, lookback, self._pipeline.pending_rows)
            first = False
            next_tick = (now_ms() // FIVE_MIN_MS + 1) * FIVE_MIN_MS + 10_000
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=max(5, (next_tick - now_ms()) / 1000))


class CvdStream:
    def __init__(
        self,
        ws: WsAdapter,
        rest: RestAdapter,
        sink: LatestSink,
        pipeline: Pipeline,
        exchange: str,
        symbols: Sequence[str],
        *,
        trade_lookback_min: int = 10,
    ) -> None:
        self._ws = ws
        self._rest = rest
        self._sink = sink
        self._pipeline = pipeline
        self._exchange = exchange
        self._symbols = list(symbols)
        self._trade_lookback_ms = trade_lookback_min * 60_000

    async def _warmup(self, cutoff: int) -> list[Trade]:
        """Persist complete minutes and return trades for the still-open minute."""
        current_minute = cutoff // 60_000 * 60_000
        tail: list[Trade] = []
        for symbol in self._symbols:
            last = await self._sink.latest_ts("deriv_cvd_1m", self._exchange, symbol)
            start = (last + 60_000) if last is not None else cutoff - self._trade_lookback_ms
            start = max(start, cutoff - 6 * 3600 * 1000)
            if start >= cutoff:
                continue
            try:
                trades = await self._rest.fetch_recent_trades(symbol, start, cutoff)
            except Exception:
                log.warning("cvd warmup fetch failed %s %s", self._exchange, symbol, exc_info=True)
                continue
            if not trades:
                log.info("cvd warmup: no trades for %s %s in [%d,%d)", self._exchange, symbol, start, cutoff)
                continue
            first_trade = min(trade.ts for trade in trades)
            last_trade = max(trade.ts for trade in trades)
            # The recent-trades page is contiguous, but it can begin in the
            # middle of a minute. Round the first usable minute up so a partial
            # bucket is never written. Round start up for the same reason.
            effective_start = max(
                (start + MINUTE_MS - 1) // MINUTE_MS * MINUTE_MS,
                (first_trade + MINUTE_MS - 1) // MINUTE_MS * MINUTE_MS,
            )
            # Never close the current minute from REST: it is still open and
            # its live trades will arrive after warmup. Also do not invent
            # zero-volume minutes after the last REST trade, because the final
            # page may be truncated by the endpoint's page limit.
            coverage_end = min(current_minute, (last_trade // MINUTE_MS + 1) * MINUTE_MS)
            if coverage_end > effective_start:
                for bucket in aggregate_window(
                    self._exchange,
                    symbol,
                    trades,
                    effective_start,
                    coverage_end,
                    source="live",
                    fill_empty=True,
                ):
                    self._pipeline.add_model(bucket, merge=False)
                log.info(
                    "cvd warmup %s %s: %d trades from %d to %d",
                    self._exchange,
                    symbol,
                    len(trades),
                    effective_start,
                    coverage_end,
                )
            tail.extend(trade for trade in trades if trade.ts >= current_minute)
        return tail

    async def run(self, stop: asyncio.Event) -> None:
        connected = asyncio.Event()
        pending: list[Trade] = []
        live = False
        aggregator = CvdAggregator(self._exchange, source="live")

        def on_trade(trade: Trade) -> None:
            nonlocal live
            if live:
                aggregator.add(trade)
            else:
                pending.append(trade)

        ws_task = asyncio.create_task(self._ws.stream_trades(self._symbols, on_trade, stop, connected))
        try:
            deadline = asyncio.get_running_loop().time() + 30
            while (
                not stop.is_set()
                and not connected.is_set()
                and asyncio.get_running_loop().time() < deadline
            ):
                await asyncio.sleep(0.2)
            if stop.is_set():
                return
            if ws_task.done():
                exc = ws_task.exception()
                if exc is not None:
                    raise exc
                log.warning("cvd ws ended before connecting: %s", self._exchange)
                return
            if not connected.is_set():
                log.warning("cvd ws did not connect in time: %s", self._exchange)
            cutoff = now_ms()
            tail = await self._warmup(cutoff)
            live = True
            live_start = cutoff // 60_000 * 60_000
            for trade in [*tail, *pending]:
                if trade.ts >= live_start:
                    aggregator.add(trade)
            pending.clear()
            while not stop.is_set():
                if ws_task.done():
                    exc = ws_task.exception()
                    if exc is not None:
                        raise exc
                    log.warning("cvd ws ended: %s", self._exchange)
                    return
                for bucket in aggregator.pop_closed():
                    self._pipeline.add_model(bucket, merge=False)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=1.0)
        finally:
            for bucket in aggregator.pop_closed():
                self._pipeline.add_model(bucket, merge=False)
            ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ws_task


class RestCvdStream:
    """CVD from REST trade polling.

    Some networks accept Binance websocket connections but never deliver
    messages. Polling ``aggTrades`` keeps minute buckets complete; the
    still-open minute is re-fetched on every poll and only closed minutes are
    written.
    """

    def __init__(
        self,
        rest: RestAdapter,
        sink: LatestSink,
        pipeline: Pipeline,
        exchange: str,
        symbols: Sequence[str],
        *,
        poll_sec: int = 15,
        seed_lookback_min: int = 10,
        assume_full_range: bool = True,
    ) -> None:
        self._rest = rest
        self._sink = sink
        self._pipeline = pipeline
        self._exchange = exchange
        self._symbols = list(symbols)
        self._poll_sec = max(5, poll_sec)
        self._seed_lookback_ms = max(1, seed_lookback_min) * MINUTE_MS
        # ``fetch_recent_trades`` covers [start, end] on Binance. Bybit's
        # recent-trade endpoint only returns the latest 1000 trades, so its
        # first page can start well after ``cursor`` and must be handled
        # conservatively.
        self._assume_full_range = assume_full_range

    def _write(self, symbol: str, trades: list[Trade], start: int, end: int) -> None:
        for bucket in aggregate_window(
            self._exchange,
            symbol,
            trades,
            start,
            end,
            source="live",
            fill_empty=True,
        ):
            self._pipeline.add_model(bucket, merge=False)

    async def _seed(self, now: int) -> dict[str, int]:
        """Catch up the recent past in chunks, then return per-symbol cursors."""
        current_minute = now // MINUTE_MS * MINUTE_MS
        floor = current_minute - self._seed_lookback_ms
        cursors: dict[str, int] = {}
        for symbol in self._symbols:
            last = await self._sink.latest_ts("deriv_cvd_1m", self._exchange, symbol)
            cursor = floor if last is None else max(last + MINUTE_MS, floor)
            cursor = (cursor + MINUTE_MS - 1) // MINUTE_MS * MINUTE_MS
            if cursor >= current_minute:
                cursors[symbol] = current_minute
                continue
            for _ in range(MAX_SEED_CHUNKS):
                if cursor >= current_minute:
                    break
                chunk_end = min(now, cursor + SEED_CHUNK_MS)
                try:
                    trades = await self._rest.fetch_recent_trades(symbol, cursor, chunk_end)
                except Exception:
                    log.warning(
                        "cvd rest seed failed %s %s [%d,%d)",
                        self._exchange,
                        symbol,
                        cursor,
                        chunk_end,
                        exc_info=True,
                    )
                    break
                closed_end = chunk_end // MINUTE_MS * MINUTE_MS
                if closed_end <= cursor:
                    break
                if not trades:
                    # A liquid contract usually trades every minute. An empty
                    # page still proves the range was polled, so keep the grid
                    # continuous with zero buckets.
                    self._write(symbol, [], cursor, closed_end)
                    cursor = closed_end
                    continue
                if self._assume_full_range:
                    # The request started exactly at ``cursor``; the absence
                    # of trades before the first tick is real, so the first
                    # (possibly partial-looking) minute is complete too.
                    effective_start = cursor
                    coverage_end = closed_end
                else:
                    first_ts = min(trade.ts for trade in trades)
                    last_ts = max(trade.ts for trade in trades)
                    effective_start = max(
                        cursor,
                        (first_ts + MINUTE_MS - 1) // MINUTE_MS * MINUTE_MS,
                    )
                    coverage_end = min(
                        closed_end,
                        (last_ts // MINUTE_MS + 1) * MINUTE_MS,
                    )
                if coverage_end > effective_start:
                    self._write(symbol, trades, effective_start, coverage_end)
                    cursor = coverage_end
                else:
                    # The page began in the middle of a minute; advance at
                    # least one minute so the loop cannot spin forever.
                    cursor = max(cursor + MINUTE_MS, effective_start)
            cursors[symbol] = cursor
            log.info(
                "cvd rest seed %s %s: cursor=%d target=%d",
                self._exchange,
                symbol,
                cursor,
                current_minute,
            )
        return cursors

    async def _poll_once(self, cursors: dict[str, int], now: int) -> None:
        current_minute = now // MINUTE_MS * MINUTE_MS
        for symbol in self._symbols:
            start = cursors.get(symbol, current_minute)
            if start >= current_minute:
                continue
            try:
                trades = await self._rest.fetch_recent_trades(symbol, start, now)
            except Exception:
                log.warning(
                    "cvd rest poll failed %s %s [%d,%d)",
                    self._exchange,
                    symbol,
                    start,
                    now,
                    exc_info=True,
                )
                continue
            last_ts = max((trade.ts for trade in trades), default=None)
            coverage_end = current_minute
            if last_ts is not None:
                coverage_end = min(current_minute, (last_ts // MINUTE_MS + 1) * MINUTE_MS)
            if coverage_end <= start:
                continue
            self._write(symbol, trades, start, coverage_end)
            cursors[symbol] = coverage_end

    async def run(self, stop: asyncio.Event) -> None:
        cursors = await self._seed(now_ms())
        while not stop.is_set():
            await self._poll_once(cursors, now_ms())
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._poll_sec)


class LiquidationStream:
    def __init__(
        self,
        ws: WsAdapter,
        pipeline: Pipeline,
        exchange: str,
        symbols: Sequence[str],
    ) -> None:
        self._ws = ws
        self._pipeline = pipeline
        self._exchange = exchange
        self._symbols = list(symbols)

    async def run(self, stop: asyncio.Event) -> None:
        def on_liquidation(event: Liquidation) -> None:
            self._pipeline.add_model(event, merge=False)

        await self._ws.stream_liquidations(self._symbols, on_liquidation, stop)
