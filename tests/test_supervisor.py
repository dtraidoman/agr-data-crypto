from __future__ import annotations

import asyncio

from collector.config import Role, Settings
from collector.models import Candle, Stream
from collector.notify import Notifier
from collector.pipeline import Pipeline
from collector.sink.memory import MemorySink
from collector.state.leases import LeaseManager
from collector.state.manager import StateManager
from collector.supervision import Supervisor
from collector.timeutil import now_ms


class FakeAdapter:
    """Stands in for REST + websocket without touching the network."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.backfills: list[tuple[Stream, int, int]] = []
        self.subscribed: list[tuple[str, ...]] = []

    async def backfill_klines(self, stream: Stream, start_ms: int, end_ms: int) -> list[Candle]:
        self.backfills.append((stream, start_ms, end_ms))
        return [
            Candle(
                exchange=stream.exchange,
                symbol=stream.symbol,
                interval=stream.interval,
                open_time=end_ms,
                close_time=end_ms + 59_999,
                open=1.0,
                high=2.0,
                low=0.5,
                close=1.5,
                volume=10.0,
                received_at=now_ms(),
            )
        ]

    async def stream_klines(self, symbols, interval, on_candle, stop) -> None:
        self.subscribed.append(tuple(symbols))
        await stop.wait()


def _settings(**overrides) -> Settings:
    base = dict(
        role=Role.ALPHA,
        run_id="test-run",
        interval="1m",
        soft_deadline_sec=30,
        drain_reserve_sec=1,
        heartbeat_interval_sec=1,
        cursor_flush_interval_sec=1,
        takeover_poll_interval_sec=1,
        takeover_grace_sec=3_600,  # never take over during the test
        standby_max_wait_sec=1,
        degraded_max_sec=60,
        flush_rows=10,
        flush_interval_sec=1,
    )
    base.update(overrides)
    return Settings(**base)


async def test_supervisor_collects_and_releases(redis_client):
    state = StateManager(
        redis_client,
        Role.ALPHA,
        "test-run",
        primary_exchange="binance",
        lease_ttl_ms=30_000,
        heartbeat_ttl_ms=60_000,
    )
    sink = MemorySink()
    pipeline = Pipeline(sink, flush_rows=10, flush_interval_sec=1)
    adapters = {"binance": FakeAdapter("binance"), "bybit": FakeAdapter("bybit")}
    supervisor = Supervisor(_settings(), state, adapters, pipeline, Notifier())

    task = asyncio.create_task(supervisor.run())
    await asyncio.sleep(0.8)
    supervisor.request_stop()
    code = await asyncio.wait_for(task, timeout=10)

    assert code == 0
    assert len(sink.rows) == 3
    assert {key[0] for key in sink.rows} == {"binance"}
    assert adapters["bybit"].backfills == []

    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        assert redis_client.exists(f"lease:kl:binance:{symbol}") == 0
        assert state.cursor(Stream("binance", symbol)) == adapters["binance"].backfills[0][2]

    assert pipeline.stats["written"] == 3


async def test_trade_streams_are_never_leased(redis_client):
    state = StateManager(
        redis_client,
        Role.ALPHA,
        "test-run",
        primary_exchange="binance",
        lease_ttl_ms=30_000,
        heartbeat_ttl_ms=60_000,
    )
    sink = MemorySink()
    pipeline = Pipeline(sink, flush_rows=10, flush_interval_sec=1)
    adapters = {"binance": FakeAdapter("binance"), "bybit": FakeAdapter("bybit")}
    supervisor = Supervisor(_settings(enable_trades=True), state, adapters, pipeline, Notifier())

    task = asyncio.create_task(supervisor.run())
    await asyncio.sleep(0.8)
    supervisor.request_stop()
    code = await asyncio.wait_for(task, timeout=10)

    assert code == 0
    # aggTrades are not implemented: leasing them would only burn REST quota
    # on kline backfills that have nowhere to be written.
    assert redis_client.keys("lease:tr:*") == []
    assert len(sink.rows) == 3


async def test_no_takeover_before_startup_grace(redis_client):
    # The peer's heartbeat is absent, but bravo's cron may simply not have
    # fired yet: until startup_grace_sec passes, its streams are off-limits.
    state = StateManager(
        redis_client,
        Role.ALPHA,
        "test-run",
        primary_exchange="binance",
        lease_ttl_ms=30_000,
        heartbeat_ttl_ms=60_000,
    )
    sink = MemorySink()
    pipeline = Pipeline(sink, flush_rows=10, flush_interval_sec=1)
    adapters = {"binance": FakeAdapter("binance"), "bybit": FakeAdapter("bybit")}
    supervisor = Supervisor(
        _settings(takeover_grace_sec=0, startup_grace_sec=3_600),
        state,
        adapters,
        pipeline,
        Notifier(),
    )

    task = asyncio.create_task(supervisor.run())
    await asyncio.sleep(0.8)
    supervisor.request_stop()
    code = await asyncio.wait_for(task, timeout=10)

    assert code == 0
    assert {key[0] for key in sink.rows} == {"binance"}
    assert redis_client.keys("lease:kl:bybit:*") == []
    assert adapters["bybit"].backfills == []


async def test_supervisor_exits_when_everything_is_leased(redis_client):
    holder = LeaseManager(redis_client, "bravo:other", 60_000)
    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        assert holder.acquire(Stream("binance", symbol)) is not None

    state = StateManager(
        redis_client,
        Role.ALPHA,
        "test-run",
        primary_exchange="binance",
        lease_ttl_ms=30_000,
    )
    sink = MemorySink()
    pipeline = Pipeline(sink)
    adapters = {"binance": FakeAdapter("binance"), "bybit": FakeAdapter("bybit")}

    supervisor = Supervisor(_settings(standby_max_wait_sec=1), state, adapters, pipeline)

    code = await asyncio.wait_for(supervisor.run(), timeout=15)

    assert code == 0
    assert sink.rows == {}
