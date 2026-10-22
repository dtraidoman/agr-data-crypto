from __future__ import annotations

from typing import Any

from collector.exchanges.bybit_rest import BybitRest
from collector.models import Stream

IV = 60_000
START = 1_699_999_980_000


class FakeBybitRest(BybitRest):
    """Reproduces the endpoint's paging contract: newest first, `start` ignored."""

    def __init__(self, open_times: list[int], max_pages: int = 10) -> None:
        self.open_times = sorted(open_times)
        self._max_pages = max_pages
        self.calls: list[tuple[int, int]] = []

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        start, end = int(params["start"]), int(params["end"])
        window = [t for t in self.open_times if start <= t <= end]
        page = window[-1000:][::-1]  # newest first, hard limit of 1000
        self.calls.append((start, end))
        return {
            "retCode": 0,
            "result": {
                "list": [[str(t), "1", "2", "0.5", "1.5", "10", "15"] for t in page],
            },
        }


async def test_bybit_backfill_covers_window_larger_than_one_page():
    times = [START + index * IV for index in range(2500)]
    rest = FakeBybitRest(times)

    candles = await rest.backfill_klines(Stream("bybit", "BTCUSDT", "kl", "1m"), times[0], times[-1])

    assert [candle.open_time for candle in candles] == times
    assert candles[0].close_time == times[0] + IV - 1
    assert candles[-1].quote_volume == 15.0
    assert len(rest.calls) == 3  # 1000 + 1000 + 500


async def test_bybit_backfill_stops_when_history_is_exhausted():
    times = [START + index * IV for index in range(5)]
    rest = FakeBybitRest(times)

    candles = await rest.backfill_klines(
        Stream("bybit", "BTCUSDT", "kl", "1m"), START - 100 * IV, times[-1]
    )

    assert [candle.open_time for candle in candles] == times
    assert len(rest.calls) == 2  # the only page, then an empty one before listing date


async def test_bybit_backfill_respects_max_pages():
    times = [START + index * IV for index in range(2500)]
    rest = FakeBybitRest(times, max_pages=1)

    candles = await rest.backfill_klines(Stream("bybit", "BTCUSDT", "kl", "1m"), times[0], times[-1])

    assert len(candles) == 1000
    assert candles[-1].open_time == times[-1]
