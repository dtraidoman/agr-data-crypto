"""Pure parsers: no I/O, no clock, fully unit-testable.

Every parser is responsible for the two classic traps:
  * drop the *in-progress* candle (Binance REST tail, Bybit REST head);
  * only accept a candle when the exchange says it is closed (x / confirm).
"""

from __future__ import annotations

import json
from typing import Any

from ..models import Candle, Trade
from ..timeutil import candle_is_closed, interval_ms


def _f(value: Any) -> float:
    return float(value)


def _i(value: Any) -> int:
    return int(value)


# --------------------------------------------------------------------- Binance
def parse_binance_rest_klines(
    payload: list[list[Any]], symbol: str, interval: str, ts_ms: int
) -> list[Candle]:
    """GET /api/v3/klines -> ascending array of arrays.

    The last element is the currently forming candle; it must never be
    persisted, otherwise it stays partial forever.
    """
    out: list[Candle] = []
    iv = interval_ms(interval)
    for row in payload:
        open_time = _i(row[0])
        close_time = _i(row[6])
        if not candle_is_closed(open_time, iv, ts_ms):
            continue
        out.append(
            Candle(
                exchange="binance",
                symbol=symbol,
                interval=interval,
                open_time=open_time,
                close_time=close_time,
                open=_f(row[1]),
                high=_f(row[2]),
                low=_f(row[3]),
                close=_f(row[4]),
                volume=_f(row[5]),
                quote_volume=_f(row[7]) if len(row) > 7 and row[7] is not None else None,
                trades=_i(row[8]) if len(row) > 8 and row[8] is not None else None,
                is_closed=True,
                received_at=ts_ms,
            )
        )
    return out


def parse_binance_ws_kline(raw: str | bytes, ts_ms: int) -> Candle | None:
    try:
        message = json.loads(raw)
    except (ValueError, TypeError):
        return None
    payload = message.get("data", message)
    if payload.get("e") != "kline":
        return None
    k = payload.get("k") or {}
    if not k.get("x"):  # updates every second; only the final one is a candle
        return None
    interval = str(k.get("i", "1m"))
    return Candle(
        exchange="binance",
        symbol=str(payload.get("s") or k.get("s") or ""),
        interval=interval,
        open_time=_i(k["t"]),
        close_time=_i(k["T"]),
        open=_f(k["o"]),
        high=_f(k["h"]),
        low=_f(k["l"]),
        close=_f(k["c"]),
        volume=_f(k["v"]),
        quote_volume=_f(k["q"]) if k.get("q") is not None else None,
        trades=_i(k["n"]) if k.get("n") is not None else None,
        is_closed=True,
        received_at=ts_ms,
    )


def parse_binance_ws_aggtrade(raw: str | bytes, ts_ms: int) -> Trade | None:
    try:
        message = json.loads(raw)
    except (ValueError, TypeError):
        return None
    payload = message.get("data", message)
    if payload.get("e") != "aggTrade":
        return None
    return Trade(
        exchange="binance",
        symbol=str(payload.get("s", "")),
        trade_id=str(payload.get("a")),
        price=_f(payload["p"]),
        qty=_f(payload["q"]),
        ts=_i(payload["T"]),
        is_buyer_maker=bool(payload.get("m")),
    )


# ----------------------------------------------------------------------- Bybit
def parse_bybit_rest_klines(
    payload: dict[str, Any], symbol: str, interval: str, ts_ms: int
) -> list[Candle]:
    """GET /v5/market/kline -> result.list is *newest first*.

    Each row is an array of strings, not an object:
    ``[start, open, high, low, close, volume, turnover]``.
    ``end`` is not returned, but it is always ``start + interval - 1``.
    The first element is the candle currently in progress.
    """
    result = payload.get("result") or {}
    rows = result.get("list") or []
    iv = interval_ms(interval)
    out: list[Candle] = []
    for row in rows:
        open_time = _i(row[0])
        if not candle_is_closed(open_time, iv, ts_ms):
            continue
        out.append(
            Candle(
                exchange="bybit",
                symbol=symbol,
                interval=interval,
                open_time=open_time,
                close_time=open_time + iv - 1,
                open=_f(row[1]),
                high=_f(row[2]),
                low=_f(row[3]),
                close=_f(row[4]),
                volume=_f(row[5]),
                quote_volume=_f(row[6]) if len(row) > 6 and row[6] not in (None, "") else None,
                trades=None,
                is_closed=True,
                received_at=ts_ms,
            )
        )
    out.sort(key=lambda candle: candle.open_time)
    return out


def parse_bybit_ws_kline(raw: str | bytes, ts_ms: int) -> Candle | None:
    """Bybit spot sends a snapshot with confirm=false on subscribe.

    Persisting that snapshot is the single most common bug in Bybit
    collectors: the later confirm=true update would be ignored by an
    "only closed" filter and the row stays partial forever.
    """
    try:
        message = json.loads(raw)
    except (ValueError, TypeError):
        return None
    topic = str(message.get("topic", ""))
    if not topic.startswith("kline."):
        return None
    parts = topic.split(".")
    if len(parts) < 3:
        return None
    items = message.get("data") or []
    if not items:
        return None
    item = items[-1]
    if not item.get("confirm"):
        return None
    interval = _bybit_interval_to_name(parts[1])
    return Candle(
        exchange="bybit",
        symbol=parts[2],
        interval=interval,
        open_time=_i(item["start"]),
        close_time=_i(item["end"]),
        open=_f(item["open"]),
        high=_f(item["high"]),
        low=_f(item["low"]),
        close=_f(item["close"]),
        volume=_f(item["volume"]),
        quote_volume=_f(item["turnover"]) if item.get("turnover") is not None else None,
        trades=None,
        is_closed=True,
        received_at=ts_ms,
    )


def parse_bybit_ws_trade(raw: str | bytes, ts_ms: int) -> Trade | None:
    try:
        message = json.loads(raw)
    except (ValueError, TypeError):
        return None
    topic = str(message.get("topic", ""))
    if not topic.startswith("publicTrade."):
        return None
    items = message.get("data") or []
    if not items:
        return None
    item = items[-1]
    return Trade(
        exchange="bybit",
        symbol=topic.split(".", 1)[1],
        trade_id=str(item.get("i")),
        price=_f(item["p"]),
        qty=_f(item["v"]),
        ts=_i(item["T"]),
        is_buyer_maker=str(item.get("S", "")).lower() == "sell",
    )


BYBIT_INTERVAL_CODE = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "4h": "240",
    "1d": "D",
}
_BYBIT_CODE_INTERVAL = {code: name for name, code in BYBIT_INTERVAL_CODE.items()}


def bybit_interval_code(interval: str) -> str:
    try:
        return BYBIT_INTERVAL_CODE[interval]
    except KeyError as exc:
        raise ValueError(f"unsupported Bybit interval: {interval!r}") from exc


def _bybit_interval_to_name(code: str) -> str:
    return _BYBIT_CODE_INTERVAL.get(code, code)
