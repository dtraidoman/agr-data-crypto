from __future__ import annotations

import fakeredis
import pytest

from collector.models import Candle


@pytest.fixture
def redis_client():
    client = fakeredis.FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        client.flushall()
        client.close()


@pytest.fixture
def candle_factory():
    def _make(
        exchange: str = "binance",
        symbol: str = "BTCUSDT",
        interval: str = "1m",
        open_time: int = 1_700_000_000_000,
        close_time: int = 1_700_000_059_999,
        is_closed: bool = True,
    ) -> Candle:
        return Candle(
            exchange=exchange,
            symbol=symbol,
            interval=interval,
            open_time=open_time,
            close_time=close_time,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10.0,
            quote_volume=1005.0,
            trades=42,
            is_closed=is_closed,
            received_at=open_time + 60_000,
        )

    return _make
