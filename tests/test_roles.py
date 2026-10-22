from __future__ import annotations

from collector.config import Role
from collector.roles import (
    PRIMARY_EXCHANGE,
    SECONDARY_EXCHANGE,
    desired_streams,
    primary_streams,
    priority_order,
    takeover_streams,
)


def test_roles_are_mirrored():
    assert PRIMARY_EXCHANGE[Role.ALPHA] == "binance"
    assert SECONDARY_EXCHANGE[Role.ALPHA] == "bybit"
    assert PRIMARY_EXCHANGE[Role.BRAVO] == "bybit"
    assert SECONDARY_EXCHANGE[Role.BRAVO] == "binance"
    assert Role.ALPHA.peer is Role.BRAVO
    assert Role.BRAVO.peer is Role.ALPHA


def test_steady_state_is_primary_exchange_only():
    streams = desired_streams(Role.ALPHA, "1m", allow_takeover=False)
    assert {stream.exchange for stream in streams} == {"binance"}
    assert [stream.symbol for stream in streams] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def test_takeover_adds_peer_exchange_after_primary():
    streams = desired_streams(Role.ALPHA, "1m", allow_takeover=True)
    assert [stream.exchange for stream in streams] == ["binance"] * 3 + ["bybit"] * 3
    assert [stream.symbol for stream in streams] == [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
    ]


def test_trades_are_lowest_priority():
    streams = desired_streams(Role.BRAVO, "1m", enable_trades=True)
    assert [stream.kind for stream in streams] == ["kl", "kl", "kl", "tr", "tr", "tr"]


def test_priority_keeps_btc_first():
    streams = priority_order(takeover_streams(Role.ALPHA, "1m"))
    assert streams[0].symbol == "BTCUSDT"
    assert streams[-1].symbol == "SOLUSDT"


def test_primary_and_takeover_do_not_overlap():
    primary = {stream.lease_name for stream in primary_streams(Role.ALPHA)}
    takeover = {stream.lease_name for stream in takeover_streams(Role.ALPHA)}
    assert primary.isdisjoint(takeover)
