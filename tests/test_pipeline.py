from __future__ import annotations

import pytest

from collector.pipeline import Pipeline
from collector.sink.memory import MemorySink


async def test_duplicate_rows_are_buffered_once(candle_factory):
    sink = MemorySink()
    pipeline = Pipeline(sink, flush_rows=100, flush_interval_sec=60)
    candle = candle_factory()

    pipeline.add([candle, candle, candle])

    assert pipeline.buffered == 1
    assert pipeline.stats["buffered_duplicates"] == 2
    assert pipeline.cursors == {}

    written = await pipeline.flush(force=True)
    assert written == 1
    assert len(sink.rows) == 1
    assert pipeline.cursors[candle.stream] == candle.open_time


async def test_open_candles_never_reach_the_sink(candle_factory):
    sink = MemorySink()
    pipeline = Pipeline(sink, flush_rows=100, flush_interval_sec=60)

    pipeline.add([candle_factory(is_closed=False)])

    assert pipeline.buffered == 0
    assert await pipeline.flush(force=True) == 0
    assert sink.rows == {}


async def test_failed_flush_does_not_advance_cursor(candle_factory):
    class BrokenSink:
        def __init__(self) -> None:
            self.calls = 0

        async def write(self, rows):
            self.calls += 1
            raise RuntimeError("supabase is down")

        async def close(self) -> None:
            return None

    sink = BrokenSink()
    pipeline = Pipeline(sink, flush_rows=100, flush_interval_sec=60)
    candle = candle_factory()
    pipeline.add([candle])

    with pytest.raises(RuntimeError):
        await pipeline.flush(force=True)

    assert pipeline.cursors == {}
    assert pipeline.buffered == 1
    assert pipeline.stats["flush_errors"] == 1

    # The retry succeeds, and only then does the cursor move.
    healthy = MemorySink()
    pipeline._sink = healthy  # noqa: SLF001 - deliberate test seam
    assert await pipeline.flush(force=True) == 1
    assert pipeline.cursors[candle.stream] == candle.open_time


async def test_flush_is_idempotent_across_duplicates(candle_factory):
    sink = MemorySink()
    pipeline = Pipeline(sink, flush_rows=100, flush_interval_sec=60)
    candle = candle_factory()

    pipeline.add([candle])
    await pipeline.flush(force=True)

    pipeline.add([candle])  # replayed by a successor after a crash
    await pipeline.flush(force=True)

    assert len(sink.rows) == 1
    assert sink.duplicates == 1
    assert pipeline.cursors[candle.stream] == candle.open_time


async def test_multiple_streams_track_independent_cursors(candle_factory):
    sink = MemorySink()
    pipeline = Pipeline(sink, flush_rows=100, flush_interval_sec=60)

    btc = candle_factory(symbol="BTCUSDT", open_time=60_000)
    eth = candle_factory(symbol="ETHUSDT", open_time=120_000)
    pipeline.add([btc, eth])
    await pipeline.flush(force=True)

    assert pipeline.cursors == {btc.stream: 60_000, eth.stream: 120_000}
    assert set(sink.rows) == {
        ("binance", "BTCUSDT", "1m", 60_000),
        ("binance", "ETHUSDT", "1m", 120_000),
    }


def test_due_respects_threshold(candle_factory):
    pipeline = Pipeline(MemorySink(), flush_rows=2, flush_interval_sec=3600)
    assert pipeline.due() is False
    pipeline.add([candle_factory(open_time=60_000)])
    assert pipeline.due() is False
    pipeline.add([candle_factory(open_time=120_000)])
    assert pipeline.due() is True
