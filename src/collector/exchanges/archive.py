from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator
from dataclasses import replace
from datetime import date
from decimal import Decimal, InvalidOperation

from ..models import Trade, normalize_side


def binance_archive_url(symbol: str, day: date) -> str:
    stamp = day.isoformat()
    return (
        "https://data.binance.vision/data/futures/um/daily/aggTrades/"
        f"{symbol}/{symbol}-aggTrades-{stamp}.zip"
    )


def bybit_archive_url(symbol: str, day: date) -> str:
    return f"https://public.bybit.com/trading/{symbol}/{symbol}{day.isoformat()}.csv.gz"


def parse_binance_archive_row(row: list[str]) -> Trade | None:
    if len(row) < 7 or not row[0].strip().isdigit():
        return None
    agg_id, price, qty, _first, _last, transact_time, buyer_maker = row[:7]
    return Trade(
        exchange="binance",
        symbol="",
        trade_id=agg_id,
        price=float(price),
        qty=float(qty),
        ts=int(float(transact_time)),
        side="sell" if buyer_maker.strip().lower() == "true" else "buy",
    )


def parse_bybit_archive_row(row: list[str]) -> Trade | None:
    if len(row) < 7:
        return None
    stamp, _symbol, side, size, price, _direction, match_id = row[:7]
    try:
        raw_ts = Decimal(stamp)
        ts = int(raw_ts * 1000) if raw_ts < Decimal(10**12) else int(raw_ts)
        trade_side = normalize_side(side)
        trade_qty = float(size)
        trade_price = float(price)
    except (InvalidOperation, ValueError):
        return None
    return Trade(
        exchange="bybit",
        symbol="",
        trade_id=match_id,
        price=trade_price,
        qty=trade_qty,
        ts=ts,
        side=trade_side,
    )


def iter_binance_csv(lines: Iterable[str], symbol: str) -> Iterator[Trade]:
    for row in csv.reader(lines):
        trade = parse_binance_archive_row(row)
        if trade is not None:
            yield replace(trade, symbol=symbol)


def iter_bybit_csv(lines: Iterable[str], symbol: str) -> Iterator[Trade]:
    for row in csv.reader(lines):
        trade = parse_bybit_archive_row(row)
        if trade is not None:
            yield replace(trade, symbol=symbol)
