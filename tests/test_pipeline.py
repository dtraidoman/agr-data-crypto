from __future__ import annotations

from collector.models import OpenInterest
from collector.pipeline import Pipeline
from collector.sink.memory import MemorySink


async def test_dedupe_and_flush() -> None:
    sink = MemorySink()
    pipeline = Pipeline(sink, flush_rows=100, flush_interval_sec=60)
    row = OpenInterest(
        exchange="binance",
        symbol="BTCUSDT",
        ts=1_700_000_000_000,
        open_interest=10.5,
        open_interest_value=1_000_000.0,
    )

    pipeline.add_model(row, merge=True)
    pipeline.add_model(row, merge=True)

    assert pipeline.pending_rows == 1
    await pipeline.flush()
    assert len(sink.rows("deriv_open_interest")) == 1
    assert pipeline.pending_rows == 0


async def test_failed_flush_keeps_rows_and_retries() -> None:
    class BrokenSink:
        def __init__(self) -> None:
            self.calls = 0

        async def write(self, table: str, rows: list[dict], *, merge: bool) -> None:
            self.calls += 1
            raise RuntimeError("supabase is down")

    broken = BrokenSink()
    pipeline = Pipeline(broken, flush_rows=100, flush_interval_sec=60)
    row = OpenInterest(
        exchange="bybit",
        symbol="ETHUSDT",
        ts=1_700_000_000_000,
        open_interest=1.0,
    )
    pipeline.add_model(row, merge=True)

    await pipeline.flush()

    assert broken.calls == 1
    assert pipeline.pending_rows == 1

    healthy = MemorySink()
    pipeline._sink = healthy  # noqa: SLF001 - deliberate test seam
    await pipeline.flush()

    assert pipeline.pending_rows == 0
    assert len(healthy.rows("deriv_open_interest")) == 1


async def test_multiple_table_and_merge_modes_are_independent() -> None:
    sink = MemorySink()
    pipeline = Pipeline(sink, flush_rows=2, flush_interval_sec=60)
    row = OpenInterest(
        exchange="binance",
        symbol="SOLUSDT",
        ts=1_700_000_000_000,
        open_interest=2.0,
    )

    pipeline.add_model(row, merge=True)
    pipeline.add_model(row, merge=False)
    assert pipeline.pending_rows == 2

    await pipeline.flush()

    assert pipeline.pending_rows == 0
    assert sink.merge_flags == [("deriv_open_interest", True), ("deriv_open_interest", False)]
    assert len(sink.rows("deriv_open_interest")) == 1
