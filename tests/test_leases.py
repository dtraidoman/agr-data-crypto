from __future__ import annotations

from collector.models import Stream
from collector.state.cursors import CursorStore
from collector.state.leases import Lease, LeaseManager

STREAM = Stream("binance", "BTCUSDT")


def test_acquire_is_exclusive_and_reentrant(redis_client):
    alpha = LeaseManager(redis_client, "alpha:run1", 90_000)
    bravo = LeaseManager(redis_client, "bravo:run1", 90_000)

    first = alpha.acquire(STREAM)
    assert first is not None
    assert first.fence == 1
    assert first.token == "alpha:run1:1"

    assert bravo.acquire(STREAM) is None

    # Re-acquiring by the same holder must not bump the fence: the holder is
    # already collecting and a fence bump would invalidate its cursor writes.
    again = alpha.acquire(STREAM)
    assert again is not None
    assert again.fence == first.fence
    assert again.token == first.token


def test_release_is_compare_and_delete(redis_client):
    alpha = LeaseManager(redis_client, "alpha:run1", 90_000)
    bravo = LeaseManager(redis_client, "bravo:run1", 90_000)

    lease = alpha.acquire(STREAM)
    assert lease is not None

    forged = Lease(stream=STREAM, token="bravo:run1:1", fence=1)
    assert bravo.release(forged) is False
    assert redis_client.exists(lease.key) == 1

    assert alpha.release(lease) is True
    assert redis_client.exists(lease.key) == 0


def test_takeover_bumps_fence(redis_client):
    alpha = LeaseManager(redis_client, "alpha:run1", 90_000)
    bravo = LeaseManager(redis_client, "bravo:run1", 90_000)

    lease_a = alpha.acquire(STREAM)
    assert lease_a is not None

    # Simulate TTL expiry / process death.
    redis_client.delete(lease_a.key)

    lease_b = bravo.acquire(STREAM)
    assert lease_b is not None
    assert lease_b.fence == lease_a.fence + 1
    assert lease_b.token == "bravo:run1:2"


def test_heartbeat_reports_lost_leases(redis_client):
    alpha = LeaseManager(redis_client, "alpha:run1", 90_000)
    lease = alpha.acquire(STREAM)
    assert lease is not None

    renewed, lost = alpha.heartbeat([lease], "hb:alpha", "run1", 300_000)
    assert renewed == 1
    assert lost == []
    assert redis_client.get("hb:alpha") == "run1"

    redis_client.delete(lease.key)
    renewed, lost = alpha.heartbeat([lease], "hb:alpha", "run1", 300_000)
    assert renewed == 0
    assert lost == [lease.key]


def test_cursor_is_monotonic_and_fenced(redis_client):
    cursors = CursorStore(redis_client)

    assert cursors.get(STREAM) == 0

    assert cursors.flush({STREAM: (1_000, 2)}) == 1
    assert cursors.get(STREAM) == 1_000

    # A zombie holder with an older fence cannot move it, in either direction.
    assert cursors.flush({STREAM: (2_000, 1)}) == 0
    assert cursors.get(STREAM) == 1_000

    # The current holder can move it forward, but never backwards.
    assert cursors.flush({STREAM: (1_500, 2)}) == 1
    assert cursors.flush({STREAM: (1_200, 2)}) == 0
    assert cursors.get(STREAM) == 1_500


def test_cursor_batch_flush(redis_client):
    cursors = CursorStore(redis_client)
    streams = [Stream("binance", "BTCUSDT"), Stream("binance", "ETHUSDT"), Stream("bybit", "SOLUSDT")]

    values = {stream: (i * 60_000, 3) for i, stream in enumerate(streams, start=1)}
    assert cursors.flush(values) == 3
    assert cursors.get_many(streams) == {
        streams[0]: 60_000,
        streams[1]: 120_000,
        streams[2]: 180_000,
    }
