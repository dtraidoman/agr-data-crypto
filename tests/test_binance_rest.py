from __future__ import annotations

from typing import Any

from collector.exchanges.binance_rest import PERIOD_MS, BinanceRest


class FakeBinanceRest(BinanceRest):
    def __init__(self, timestamps: list[int]) -> None:
        super().__init__(None, None, max_pages=10)  # type: ignore[arg-type]
        self.timestamps = timestamps
        self.calls: list[tuple[int, int]] = []

    async def _get(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        start = int(params["startTime"])
        end = int(params["endTime"])
        self.calls.append((start, end))
        points = [ts for ts in self.timestamps if start <= ts <= end]
        # Binance returns the newest page inside the requested window.
        page = points[-500:]
        return [
            {
                "timestamp": ts,
                "sumOpenInterest": "1",
                "sumOpenInterestValue": "2",
                "longAccount": "0.6",
                "shortAccount": "0.4",
                "longShortRatio": "1.5",
                "buyVol": "3",
                "sellVol": "2",
                "buySellRatio": "1.5",
            }
            for ts in page
        ]


async def test_binance_metrics_page_backwards_from_end() -> None:
    base = 1_700_000_000_000 // PERIOD_MS * PERIOD_MS
    timestamps = [base + index * PERIOD_MS for index in range(1200)]
    end = timestamps[-1]
    rest = FakeBinanceRest(timestamps)

    oi = await rest.fetch_open_interest("BTCUSDT", timestamps[0], end)
    ls = await rest.fetch_long_short("BTCUSDT", timestamps[0], end)

    assert len(oi) == 1200
    assert oi[0].ts == timestamps[0]
    assert oi[-1].ts == timestamps[-1]
    assert len(ls) == 4800
    assert {row.kind for row in ls} == {
        "global_account",
        "top_account",
        "top_position",
        "taker_volume",
    }
    assert len(rest.calls) > 3
