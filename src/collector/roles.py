from __future__ import annotations

from collections.abc import Iterable

from .config import Role
from .models import Stream

SYMBOLS: tuple[str, str, str] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
INTERVALS = ("1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d")

# Steady-state ownership. BTC is first everywhere: it is the series that
# matters most for backtests, so it gets the shortest possible gap.
PRIMARY_EXCHANGE: dict[Role, str] = {Role.ALPHA: "binance", Role.BRAVO: "bybit"}
SECONDARY_EXCHANGE: dict[Role, str] = {Role.ALPHA: "bybit", Role.BRAVO: "binance"}

_SYMBOL_PRIORITY = {symbol: idx for idx, symbol in enumerate(SYMBOLS)}
_KIND_PRIORITY = {"kl": 0, "tr": 1}


def priority_key(stream: Stream) -> tuple[int, int]:
    """1m klines first, then ticks; BTC before ETH before SOL inside a group."""
    return (_KIND_PRIORITY.get(stream.kind, 9), _SYMBOL_PRIORITY.get(stream.symbol, 99))


def priority_order(streams: Iterable[Stream]) -> list[Stream]:
    return sorted(streams, key=priority_key)


def _streams_for(exchange: str, interval: str, enable_trades: bool) -> list[Stream]:
    out = [Stream(exchange, symbol, "kl", interval) for symbol in SYMBOLS]
    if enable_trades:
        out += [Stream(exchange, symbol, "tr", interval) for symbol in SYMBOLS]
    return out


def primary_streams(role: Role, interval: str = "1m", enable_trades: bool = False) -> list[Stream]:
    """What the role always wants, regardless of the peer's health."""
    return _streams_for(PRIMARY_EXCHANGE[role], interval, enable_trades)


def takeover_streams(role: Role, interval: str = "1m", enable_trades: bool = False) -> list[Stream]:
    """What the role may grab only if the peer's heartbeat is gone."""
    return _streams_for(SECONDARY_EXCHANGE[role], interval, enable_trades)


def desired_streams(
    role: Role, interval: str = "1m", enable_trades: bool = False, allow_takeover: bool = False
) -> list[Stream]:
    """Own primary streams first, then the peer's — never interleaved.

    Exchange order must dominate symbol order: taking over Bybit BTC while
    Binance ETH is still unclaimed would be the wrong trade-off.
    """
    streams = priority_order(primary_streams(role, interval, enable_trades))
    if allow_takeover:
        streams += priority_order(takeover_streams(role, interval, enable_trades))
    return streams


def all_streams(interval: str = "1m", enable_trades: bool = False) -> list[Stream]:
    return priority_order(_streams_for("binance", interval, enable_trades) + _streams_for("bybit", interval, enable_trades))
