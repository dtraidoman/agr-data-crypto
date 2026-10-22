from __future__ import annotations

from collector.config import Role
from collector.models import Stream
from collector.state.manager import StateManager

BINANCE_BTC = Stream("binance", "BTCUSDT")


def _state(redis_client, role: Role, run_id: str) -> StateManager:
    return StateManager(
        redis_client,
        role,
        run_id,
        primary_exchange="binance" if role is Role.ALPHA else "bybit",
        lease_ttl_ms=1_000,
        heartbeat_ttl_ms=5_000,
    )


async def test_takeover_after_peer_dies(redis_client):
    alpha = _state(redis_client, Role.ALPHA, "a1")
    bravo = _state(redis_client, Role.BRAVO, "b1")
    alpha.start()
    bravo.start()

    assert bravo.peer_alive(Role.ALPHA) is True
    assert alpha.peer_alive(Role.BRAVO) is True

    lease_a = alpha.acquire(BINANCE_BTC)
    assert lease_a is not None
    assert bravo.acquire(BINANCE_BTC) is None

    # The peer process dies: its heartbeat and leases disappear after their TTL.
    redis_client.delete("hb:alpha", lease_a.key)
    assert bravo.peer_alive(Role.ALPHA) is False

    lease_b = bravo.acquire(BINANCE_BTC)
    assert lease_b is not None
    assert lease_b.fence > lease_a.fence

    # The new holder writes the cursor...
    assert bravo.flush_cursors({BINANCE_BTC: (1_000, lease_b.fence)}) == 1
    assert bravo.cursor(BINANCE_BTC) == 1_000

    # ...and the resurrected zombie cannot overwrite it, even with a larger value.
    assert alpha.flush_cursors({BINANCE_BTC: (9_999, lease_a.fence)}) == 0
    assert bravo.cursor(BINANCE_BTC) == 1_000


async def test_same_role_restart_is_fenced(redis_client):
    first = _state(redis_client, Role.ALPHA, "a1")
    lease_first = first.acquire(BINANCE_BTC)
    assert lease_first is not None

    redis_client.delete(lease_first.key)  # TTL expiry

    second = _state(redis_client, Role.ALPHA, "a2")
    lease_second = second.acquire(BINANCE_BTC)
    assert lease_second is not None
    assert lease_second.fence == lease_first.fence + 1

    assert second.flush_cursors({BINANCE_BTC: (500, lease_second.fence)}) == 1
    assert first.flush_cursors({BINANCE_BTC: (900, lease_first.fence)}) == 0
    assert second.cursor(BINANCE_BTC) == 500


def test_degraded_mode_allows_primary_exchange_only():
    state = StateManager(None, Role.ALPHA, "offline", primary_exchange="binance")

    assert state.degraded is True
    assert state.acquire(Stream("binance", "BTCUSDT")) is not None
    # Grabbing the peer's exchange without a lease would double-collect it.
    assert state.acquire(Stream("bybit", "BTCUSDT")) is None
    assert state.cursor(BINANCE_BTC) == 0
    assert state.peer_alive(Role.BRAVO) is False
    assert state.heartbeat([]).ok is False
    assert state.flush_cursors({BINANCE_BTC: (1, 1)}) == 0


async def test_scanning_heartbeat_does_not_touch_leases(redis_client):
    alpha = _state(redis_client, Role.ALPHA, "a1")
    alpha.start()
    beta = alpha.heartbeat([])
    assert beta.ok is True
    assert redis_client.get("hb:alpha") == "a1"
