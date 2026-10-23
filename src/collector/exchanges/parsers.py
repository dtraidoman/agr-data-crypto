"""Pure exchange payload parsers. No I/O, no clock, easy to unit-test."""

from __future__ import annotations

import hashlib
from typing import Any

from ..models import (
    Liquidation,
    LongShort,
    OpenInterest,
    Trade,
    normalize_side,
    position_side_for,
)


def _f(value: Any) -> float:
    return float(value)


def _i(value: Any) -> int:
    return int(float(value))


def _ratio(long_value: float, short_value: float) -> float | None:
    return long_value / short_value if short_value else None


def make_event_id(*parts: Any) -> str:
    canonical = "|".join(str(part) for part in parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def parse_binance_open_interest(payload: list[dict[str, Any]], symbol: str) -> list[OpenInterest]:
    return [
        OpenInterest(
            exchange="binance",
            symbol=symbol,
            ts=_i(item["timestamp"]),
            open_interest=_f(item["sumOpenInterest"]),
            open_interest_value=_f(item["sumOpenInterestValue"]),
        )
        for item in payload
    ]


def parse_binance_long_short(
    payload: list[dict[str, Any]], symbol: str, kind: str
) -> list[LongShort]:
    out: list[LongShort] = []
    for item in payload:
        if kind == "taker_volume":
            long_value, short_value = _f(item["buyVol"]), _f(item["sellVol"])
            ratio = _f(item.get("buySellRatio") or _ratio(long_value, short_value) or 0)
            unit = "base_volume"
        else:
            long_value, short_value = _f(item["longAccount"]), _f(item["shortAccount"])
            ratio = _f(item.get("longShortRatio") or _ratio(long_value, short_value) or 0)
            unit = "fraction"
        out.append(
            LongShort(
                exchange="binance",
                symbol=symbol,
                kind=kind,
                ts=_i(item["timestamp"]),
                long_value=long_value,
                short_value=short_value,
                ratio=ratio,
                unit=unit,
            )
        )
    return out


def parse_binance_agg_trade(payload: dict[str, Any], symbol: str | None = None) -> Trade:
    raw_symbol = payload.get("s") or symbol
    if not raw_symbol:
        raise ValueError("binance aggTrade payload has no symbol")
    return Trade(
        exchange="binance",
        symbol=str(raw_symbol).upper(),
        trade_id=str(payload["a"]),
        price=_f(payload["p"]),
        qty=_f(payload["q"]),
        ts=_i(payload["T"]),
        side="sell" if bool(payload["m"]) else "buy",
    )


def parse_binance_force_order(payload: dict[str, Any]) -> Liquidation:
    order = payload.get("o", payload)
    symbol = str(order["s"]).upper()
    order_side = normalize_side(str(order["S"]))
    qty = _f(order.get("z") or order.get("q") or 0)
    avg_price = _f(order.get("ap") or 0)
    price = avg_price if avg_price > 0 else _f(order.get("p") or 0)
    ts = _i(order.get("T") or payload.get("E") or 0)
    return Liquidation(
        exchange="binance",
        symbol=symbol,
        ts=ts,
        order_side=order_side,
        position_side=position_side_for(order_side),
        price=price,
        qty=qty,
        notional=price * qty,
        event_id=make_event_id("binance", symbol, ts, order_side, qty, price),
    )


def parse_bybit_open_interest(result: dict[str, Any], symbol: str) -> list[OpenInterest]:
    out: list[OpenInterest] = []
    for item in result.get("list", []):
        value = item.get("openInterestValue")
        out.append(
            OpenInterest(
                exchange="bybit",
                symbol=symbol,
                ts=_i(item["timestamp"]),
                open_interest=_f(item["openInterest"]),
                open_interest_value=_f(value) if value not in (None, "") else None,
            )
        )
    return out


def parse_bybit_long_short(result: dict[str, Any], symbol: str) -> list[LongShort]:
    out: list[LongShort] = []
    for item in result.get("list", []):
        long_value, short_value = _f(item["buyRatio"]), _f(item["sellRatio"])
        out.append(
            LongShort(
                exchange="bybit",
                symbol=symbol,
                kind="account",
                ts=_i(item["timestamp"]),
                long_value=long_value,
                short_value=short_value,
                ratio=_ratio(long_value, short_value),
                unit="fraction",
            )
        )
    return out


def parse_bybit_trade(item: dict[str, Any], symbol: str) -> Trade:
    # WebSocket publicTrade uses short keys; REST recent-trade uses long keys.
    price = item.get("p", item.get("price"))
    qty = item.get("v", item.get("size"))
    ts = item.get("T", item.get("time"))
    side = item.get("S", item.get("side"))
    if price is None or qty is None or ts is None or side is None:
        raise KeyError("bybit trade payload is missing required fields")
    return Trade(
        exchange="bybit",
        symbol=symbol,
        trade_id=str(item.get("i") or item.get("execId") or ""),
        price=_f(price),
        qty=_f(qty),
        ts=_i(ts),
        side=normalize_side(str(side)),
    )


def parse_bybit_liquidation(item: dict[str, Any], symbol: str) -> Liquidation:
    order_side = normalize_side(str(item["S"]))
    price, qty = _f(item["p"]), _f(item["v"])
    ts = _i(item["T"])
    return Liquidation(
        exchange="bybit",
        symbol=symbol,
        ts=ts,
        order_side=order_side,
        position_side=position_side_for(order_side),
        price=price,
        qty=qty,
        notional=price * qty,
        event_id=make_event_id("bybit", symbol, ts, order_side, qty, price),
    )

