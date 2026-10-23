from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import replace

from .models import CvdBucket, Trade
from .timeutil import now_ms

log = logging.getLogger(__name__)

MINUTE_MS = 60_000


class CvdAggregator:
    """Exact taker-volume CVD, bucketed by exchange trade time."""

    def __init__(
        self,
        exchange: str,
        *,
        source: str = "live",
        close_delay_ms: int = 5_000,
        dedupe_size: int = 200_000,
    ) -> None:
        self._exchange = exchange
        self._source = source
        self._close_delay_ms = close_delay_ms
        self._dedupe_size = dedupe_size
        self._buckets: dict[tuple[str, int], CvdBucket] = {}
        self._seen: dict[str, OrderedDict[str, None]] = {}

    def add(self, trade: Trade) -> bool:
        if trade.exchange != self._exchange:
            return False
        seen = self._seen.setdefault(trade.symbol, OrderedDict())
        trade_id = trade.trade_id or f"{trade.ts}:{trade.price}:{trade.qty}"
        if trade_id in seen:
            return False
        seen[trade_id] = None
        if len(seen) > self._dedupe_size:
            seen.popitem(last=False)

        key = (trade.symbol, trade.minute)
        bucket = self._buckets.get(key) or CvdBucket(
            exchange=self._exchange,
            symbol=trade.symbol,
            ts=trade.minute,
            source=self._source,
        )
        if trade.side == "buy":
            bucket = replace(
                bucket,
                buy_volume=bucket.buy_volume + trade.qty,
                buy_notional=bucket.buy_notional + trade.notional,
                trades=bucket.trades + 1,
            )
        else:
            bucket = replace(
                bucket,
                sell_volume=bucket.sell_volume + trade.qty,
                sell_notional=bucket.sell_notional + trade.notional,
                trades=bucket.trades + 1,
            )
        self._buckets[key] = bucket
        return True

    def pop_closed(self, now: int | None = None) -> list[CvdBucket]:
        cutoff = ((now if now is not None else now_ms()) - self._close_delay_ms) // MINUTE_MS * MINUTE_MS
        closed = [bucket for (_, ts), bucket in self._buckets.items() if ts < cutoff]
        for bucket in closed:
            self._buckets.pop((bucket.symbol, bucket.ts), None)
        return sorted(closed, key=lambda bucket: (bucket.symbol, bucket.ts))

    def drain(self) -> list[CvdBucket]:
        rows = sorted(self._buckets.values(), key=lambda bucket: (bucket.symbol, bucket.ts))
        self._buckets.clear()
        return rows


def aggregate_iter(
    exchange: str,
    symbol: str,
    trades: Iterable[Trade],
    start_ms: int,
    end_ms: int,
    *,
    source: str = "archive",
    fill_empty: bool = True,
) -> list[CvdBucket]:
    start = start_ms // MINUTE_MS * MINUTE_MS
    end = end_ms // MINUTE_MS * MINUTE_MS
    buckets: dict[int, CvdBucket] = {}
    if fill_empty:
        for ts in range(start, end, MINUTE_MS):
            buckets[ts] = CvdBucket(exchange=exchange, symbol=symbol, ts=ts, source=source)
    for trade in trades:
        if not (start_ms <= trade.ts < end_ms):
            continue
        ts = trade.minute
        bucket = buckets.get(ts)
        if bucket is None:
            bucket = CvdBucket(exchange=exchange, symbol=symbol, ts=ts, source=source)
        if trade.side == "buy":
            bucket = replace(
                bucket,
                buy_volume=bucket.buy_volume + trade.qty,
                buy_notional=bucket.buy_notional + trade.notional,
                trades=bucket.trades + 1,
            )
        else:
            bucket = replace(
                bucket,
                sell_volume=bucket.sell_volume + trade.qty,
                sell_notional=bucket.sell_notional + trade.notional,
                trades=bucket.trades + 1,
            )
        buckets[ts] = bucket
    return [buckets[ts] for ts in sorted(buckets)]


def aggregate_window(
    exchange: str,
    symbol: str,
    trades: list[Trade],
    start_ms: int,
    end_ms: int,
    *,
    source: str = "archive",
    fill_empty: bool = True,
) -> list[CvdBucket]:
    return aggregate_iter(
        exchange,
        symbol,
        trades,
        start_ms,
        end_ms,
        source=source,
        fill_empty=fill_empty,
    )
