from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class Stream:
    """One logical data stream: (exchange, symbol, kind).

    The stream is also the unit of locking: exactly one process may hold the
    lease for it, therefore exactly one process writes its rows.
    """

    exchange: str
    symbol: str
    kind: str = "kl"  # "kl" = klines, "tr" = aggTrades
    interval: str = "1m"

    @property
    def lease_name(self) -> str:
        """Redis suffix shared by lease/cursor/fence keys."""
        return f"{self.kind}:{self.exchange}:{self.symbol}"

    @property
    def label(self) -> str:
        return f"{self.exchange}:{self.symbol}:{self.kind}"

    @property
    def is_klines(self) -> bool:
        return self.kind == "kl"


@dataclass(frozen=True, slots=True)
class Candle:
    """A closed OHLCV candle, normalised to exchange-provided timestamps."""

    exchange: str
    symbol: str
    interval: str
    open_time: int  # ms, exchange clock
    close_time: int  # ms, exchange clock
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float | None = None
    trades: int | None = None
    is_closed: bool = True
    received_at: int = 0

    @property
    def pk(self) -> tuple[str, str, str, int]:
        """Natural key used for deduplication at every layer."""
        return (self.exchange, self.symbol, self.interval, self.open_time)

    @property
    def stream(self) -> Stream:
        return Stream(self.exchange, self.symbol, "kl", self.interval)

    def to_row(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "interval": self.interval,
            "open_time": self.open_time,
            "close_time": self.close_time,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "quote_volume": self.quote_volume,
            "trades": self.trades,
            "is_closed": self.is_closed,
        }
        if self.received_at:
            # PostgREST expects an ISO-8601 timestamptz, not epoch ms.
            row["received_at"] = datetime.fromtimestamp(self.received_at / 1000, tz=UTC).isoformat()
        return row


@dataclass(frozen=True, slots=True)
class Trade:
    """Optional phase-2 stream. Not written by default (see ENABLE_TRADES)."""

    exchange: str
    symbol: str
    trade_id: str
    price: float
    qty: float
    ts: int  # ms
    is_buyer_maker: bool | None = None

    @property
    def pk(self) -> tuple[str, str, str]:
        return (self.exchange, self.symbol, self.trade_id)

    def to_row(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "trade_id": self.trade_id,
            "price": self.price,
            "qty": self.qty,
            "ts": self.ts,
            "is_buyer_maker": self.is_buyer_maker,
        }
