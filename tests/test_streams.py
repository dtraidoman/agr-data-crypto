from __future__ import annotations

from typing import Any

from collector.models import Trade
from collector.pipeline import Pipeline
from collector.sink import MemorySink
from collector.streams import CvdStream, RestCvdStream

BASE = 1_700_000_000_000 // 60_000 * 60_000


class FakeRest:
    name = "binance"

    def __init__(self, trades: list[Trade]) -> None:
        self.trades = trades
        self.calls: list[tuple[str, int, int]] = []

    async def fetch_recent_trades(self, symbol: str, start_ms: int, end_ms: int) -> list[Trade]:
        self.calls.append((symbol, start_ms, end_ms))
        return [trade for trade in self.trades if start_ms <= trade.ts < end_ms]

    async def fetch_open_interest(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    async def fetch_long_short(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []


def trade(offset_ms: int, qty: float = 1.0, side: str = "buy") -> Trade:
    return Trade(
        exchange="binance",
        symbol="BTCUSDT",
        trade_id=f"t-{offset_ms}-{qty}-{side}",
        price=100.0,
        qty=qty,
        ts=BASE + offset_ms,
        side=side,
    )


async def test_warmup_persists_only_closed_minutes_and_returns_open_tail() -> None:
    sink = MemorySink()
    pipeline = Pipeline(sink)
    rest = FakeRest(
        [
            trade(-120_000 + 10_000, 2.0, "buy"),
            trade(-60_000 + 5_000, 3.0, "sell"),
            trade(10_000, 4.0, "buy"),
        ]
    )
    stream = CvdStream(
        ws=None,  # type: ignore[arg-type]
        rest=rest,
        sink=sink,
        pipeline=pipeline,
        exchange="binance",
        symbols=["BTCUSDT"],
    )

    tail = await stream._warmup(BASE + 30_000)
    await pipeline.flush()

    rows = sorted(sink.rows("deriv_cvd_1m"), key=lambda row: row["ts"])
    # The first REST trade falls in the middle of BASE-120_000, so that
    # partial bucket must be dropped. BASE-60_000 is fully covered.
    assert [row["ts"] for row in rows] == [BASE - 60_000]
    assert rows[0]["sell_volume"] == 3.0
    assert {row["source"] for row in rows} == {"live"}
    assert [item.ts for item in tail] == [BASE + 10_000]


async def test_warmup_does_not_invent_zero_minutes_after_last_trade() -> None:
    sink = MemorySink()
    pipeline = Pipeline(sink)
    rest = FakeRest([trade(-120_000)])
    stream = CvdStream(
        ws=None,  # type: ignore[arg-type]
        rest=rest,
        sink=sink,
        pipeline=pipeline,
        exchange="binance",
        symbols=["BTCUSDT"],
    )

    tail = await stream._warmup(BASE + 30_000)
    await pipeline.flush()

    rows = sink.rows("deriv_cvd_1m")
    assert len(rows) == 1
    assert rows[0]["ts"] == BASE - 120_000
    assert rows[0]["trades"] == 1
    assert tail == []


async def test_rest_seed_catches_up_closed_minutes() -> None:
    sink = MemorySink()
    pipeline = Pipeline(sink)
    rest = FakeRest(
        [trade(-600_000 + minute * 60_000 + 5_000) for minute in range(10)]
        + [trade(10_000, 4.0, "buy")]
    )
    stream = RestCvdStream(
        rest=rest,
        sink=sink,
        pipeline=pipeline,
        exchange="binance",
        symbols=["BTCUSDT"],
        seed_lookback_min=10,
    )

    cursors = await stream._seed(BASE + 30_000)
    await pipeline.flush()

    rows = sorted(sink.rows("deriv_cvd_1m"), key=lambda row: row["ts"])
    # The request starts exactly at the cursor, so every closed minute is
    # written even when the first trade is a few seconds into a bucket.
    assert [row["ts"] for row in rows] == [
        BASE - 600_000 + minute * 60_000 for minute in range(10)
    ]
    assert all(row["trades"] == 1 for row in rows)
    assert cursors == {"BTCUSDT": BASE}


async def test_rest_poll_writes_only_the_closed_minute() -> None:
    sink = MemorySink()
    pipeline = Pipeline(sink)
    rest = FakeRest([trade(10_000, 2.0, "buy"), trade(70_000, 3.0, "sell")])
    stream = RestCvdStream(
        rest=rest,
        sink=sink,
        pipeline=pipeline,
        exchange="binance",
        symbols=["BTCUSDT"],
    )

    cursors = {"BTCUSDT": BASE}
    await stream._poll_once(cursors, BASE + 90_000)
    await pipeline.flush()

    rows = sink.rows("deriv_cvd_1m")
    assert [row["ts"] for row in rows] == [BASE]
    assert rows[0]["buy_volume"] == 2.0
    assert rows[0]["sell_volume"] == 0.0
    assert cursors == {"BTCUSDT": BASE + 60_000}
