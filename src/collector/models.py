from __future__ import annotations

from dataclasses import dataclass
from typing import Any

TABLES = {
    "oi": "deriv_open_interest",
    "ls": "deriv_long_short",
    "cvd": "deriv_cvd_1m",
    "liq": "deriv_liquidations",
}


def normalize_side(value: str) -> str:
    value = value.strip().lower()
    if value in {"buy", "b"}:
        return "buy"
    if value in {"sell", "s"}:
        return "sell"
    raise ValueError(f"unsupported side: {value!r}")


@dataclass(frozen=True, slots=True)
class Stream:
    exchange: str
    symbol: str = "ALL"
    kind: str = "cvd"

    @property
    def lease_name(self) -> str:
        return f"{self.kind}:{self.exchange}:{self.symbol}"

    @property
    def label(self) -> str:
        return self.lease_name


@dataclass(frozen=True, slots=True)
class Trade:
    exchange: str
    symbol: str
    trade_id: str
    price: float
    qty: float
    ts: int
    side: str

    @property
    def notional(self) -> float:
        return self.price * self.qty

    @property
    def minute(self) -> int:
        return self.ts // 60_000 * 60_000


@dataclass(frozen=True, slots=True)
class OpenInterest:
    exchange: str
    symbol: str
    ts: int
    open_interest: float
    open_interest_value: float | None = None

    table = TABLES["oi"]

    @property
    def key(self) -> tuple[str, str, int]:
        return self.exchange, self.symbol, self.ts

    def to_row(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "ts": self.ts,
            "open_interest": self.open_interest,
            "open_interest_value": self.open_interest_value,
        }


@dataclass(frozen=True, slots=True)
class LongShort:
    exchange: str
    symbol: str
    kind: str
    ts: int
    long_value: float
    short_value: float
    ratio: float | None
    unit: str

    table = TABLES["ls"]

    @property
    def key(self) -> tuple[str, str, str, int]:
        return self.exchange, self.symbol, self.kind, self.ts

    def to_row(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "kind": self.kind,
            "ts": self.ts,
            "long_value": self.long_value,
            "short_value": self.short_value,
            "ratio": self.ratio,
            "unit": self.unit,
        }


@dataclass(frozen=True, slots=True)
class CvdBucket:
    exchange: str
    symbol: str
    ts: int
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    buy_notional: float = 0.0
    sell_notional: float = 0.0
    trades: int = 0
    source: str = "live"

    table = TABLES["cvd"]

    @property
    def key(self) -> tuple[str, str, int]:
        return self.exchange, self.symbol, self.ts

    @property
    def delta(self) -> float:
        return self.buy_volume - self.sell_volume

    def to_row(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "ts": self.ts,
            "buy_volume": self.buy_volume,
            "sell_volume": self.sell_volume,
            "buy_notional": self.buy_notional,
            "sell_notional": self.sell_notional,
            "trades": self.trades,
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class Liquidation:
    exchange: str
    symbol: str
    ts: int
    order_side: str
    position_side: str
    price: float
    qty: float
    notional: float
    event_id: str
    source: str = "live"

    table = TABLES["liq"]

    @property
    def key(self) -> tuple[str, str]:
        return self.exchange, self.event_id

    def to_row(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "ts": self.ts,
            "order_side": self.order_side,
            "position_side": self.position_side,
            "price": self.price,
            "qty": self.qty,
            "notional": self.notional,
            "event_id": self.event_id,
            "source": self.source,
        }


def position_side_for(order_side: str) -> str:
    return "long" if normalize_side(order_side) == "sell" else "short"
