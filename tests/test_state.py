from __future__ import annotations

import asyncio

import fakeredis.aioredis
from pytest import MonkeyPatch
from redis.exceptions import RedisError

from collector.models import Stream
from collector.state.manager import NullState, StateManager


def make_stream() -> Stream:
    return Stream(exchange="binance", symbol="ALL", kind="cvd")


async def test_lease_is_exclusive_and_releasable() -> None:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    alpha = StateManager(redis, role="alpha", run_id="alpha-1")
    bravo = StateManager(redis, role="bravo", run_id="bravo-1")
    stream = make_stream()

    try:
        assert await alpha.acquire(stream)
        assert not await bravo.acquire(stream)
        assert await alpha.heartbeat([stream]) == []
        await alpha.release(stream)
        assert await bravo.acquire(stream)
    finally:
        await redis.aclose()


async def test_heartbeat_reports_lost_lease() -> None:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    alpha = StateManager(redis, role="alpha", run_id="alpha-1")
    stream = make_stream()

    try:
        assert await alpha.acquire(stream)
        await redis.delete(f"lease:{stream.lease_name}")
        assert await alpha.heartbeat([stream]) == [stream.lease_name]
    finally:
        await redis.aclose()


async def test_waiting_successor_heartbeat_does_not_take_stream_lease() -> None:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    old = StateManager(redis, role="alpha", run_id="alpha-old")
    new = StateManager(redis, role="alpha", run_id="alpha-new")
    stream = make_stream()

    try:
        assert await old.acquire(stream)
        await new.heartbeat([])
        assert not await new.acquire(stream)
        assert await old.heartbeat([stream]) == []
    finally:
        await redis.aclose()


async def test_peer_heartbeat() -> None:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    alpha = StateManager(redis, role="alpha", run_id="alpha-1")
    bravo = StateManager(redis, role="bravo", run_id="bravo-1")

    try:
        assert not await alpha.peer_alive("bravo")
        await bravo.heartbeat([])
        assert await alpha.peer_alive("bravo")
    finally:
        await redis.aclose()


async def test_null_state_never_blocks() -> None:
    state = NullState()
    stream = make_stream()

    assert not state.available
    assert await state.acquire(stream)
    assert await state.heartbeat([stream]) == []
    assert not await state.peer_alive("alpha")
    await state.release(stream)
    await state.close()


async def test_redis_error_degrades_leases(monkeypatch: MonkeyPatch) -> None:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    state = StateManager(redis, role="alpha", run_id="alpha-1")
    stream = make_stream()

    async def fail(*args: object, **kwargs: object) -> None:
        raise RedisError("redis down")

    monkeypatch.setattr(state, "_acquire_script", fail)
    try:
        assert not await state.acquire(stream)
        assert not state.available
    finally:
        await redis.aclose()


async def test_lease_ttl_expires() -> None:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    alpha = StateManager(redis, role="alpha", run_id="alpha-1", lease_ttl_ms=1)
    bravo = StateManager(redis, role="bravo", run_id="bravo-1")
    stream = make_stream()

    try:
        assert await alpha.acquire(stream)
        await asyncio.sleep(0.02)
        assert await bravo.acquire(stream)
    finally:
        await redis.aclose()
