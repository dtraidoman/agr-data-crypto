from __future__ import annotations

from collector.aggregation import CvdAggregator, aggregate_iter, aggregate_window
from collector.models import Trade


def make_trade(
    *,
    ts: int = 60_000,
    side: str = "buy",
    qty: float = 2.0,
    price: float = 100.0,
    trade_id: str = "t1",
    exchange: str = "binance",
    symbol: str = "BTCUSDT",
) -> Trade:
    return Trade(
        exchange=exchange,
        symbol=symbol,
        trade_id=trade_id,
        price=price,
        qty=qty,
        ts=ts,
        side=side,
    )


def test_aggregator_uses_taker_side() -> None:
    aggregator = CvdAggregator("binance", close_delay_ms=0)
    assert aggregator.add(make_trade(side="buy", qty=2, price=100, trade_id="buy"))
    assert aggregator.add(make_trade(side="sell", qty=1, price=50, trade_id="sell"))

    rows = aggregator.pop_closed(now=120_000)

    assert len(rows) == 1
    bucket = rows[0]
    assert bucket.buy_volume == 2
    assert bucket.sell_volume == 1
    assert bucket.buy_notional == 200
    assert bucket.sell_notional == 50
    assert bucket.delta == 1
    assert bucket.trades == 2


def test_aggregator_deduplicates_trade_ids_and_ignores_other_exchanges() -> None:
    aggregator = CvdAggregator("binance", close_delay_ms=0)
    assert aggregator.add(make_trade(trade_id="same"))
    assert not aggregator.add(make_trade(trade_id="same"))
    assert not aggregator.add(make_trade(trade_id="other", exchange="bybit"))

    rows = aggregator.pop_closed(now=120_000)

    assert len(rows) == 1
    assert rows[0].trades == 1


def test_pop_closed_only_returns_closed_buckets() -> None:
    aggregator = CvdAggregator("binance", close_delay_ms=0)
    aggregator.add(make_trade(ts=60_000, trade_id="a"))
    aggregator.add(make_trade(ts=120_000, trade_id="b"))

    rows = aggregator.pop_closed(now=120_000)

    assert [row.ts for row in rows] == [60_000]
    assert aggregator.drain()[0].ts == 120_000


def test_aggregate_iter_fills_empty_minutes_and_filters_window() -> None:
    trades = [
        make_trade(ts=60_000, trade_id="in"),
        make_trade(ts=180_000, trade_id="out"),
    ]

    rows = aggregate_iter("binance", "BTCUSDT", trades, 60_000, 180_000)

    assert [row.ts for row in rows] == [60_000, 120_000]
    assert rows[0].trades == 1
    assert rows[1].trades == 0
    assert rows[1].delta == 0


def test_aggregate_window_is_a_thin_wrapper() -> None:
    rows = aggregate_window(
        "bybit",
        "ETHUSDT",
        [make_trade(exchange="bybit", symbol="ETHUSDT", ts=301_000)],
        300_000,
        360_000,
    )

    assert len(rows) == 1
    assert rows[0].ts == 300_000
    assert rows[0].source == "archive"
