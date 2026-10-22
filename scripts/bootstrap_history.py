#!/usr/bin/env python3
"""One-off deep-history bootstrap (not part of the live relay).

Binance is pulled from the monthly public archives (fast, no rate limits).
Bybit has no equally stable public archive, so it is walked through the REST
kline endpoint with the same throttling the collector uses.

Usage:
    python scripts/bootstrap_history.py --exchange binance --months 12
    python scripts/bootstrap_history.py --exchange both --symbols BTCUSDT --months 3
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from collector.config import Settings  # noqa: E402
from collector.exchanges.bybit_rest import BybitRest  # noqa: E402
from collector.exchanges.ratelimit import AsyncRateLimiter  # noqa: E402
from collector.models import Candle, Stream  # noqa: E402
from collector.sink.supabase import SupabaseSink  # noqa: E402
from collector.timeutil import interval_ms  # noqa: E402

BINANCE_ARCHIVE = "https://data.binance.vision/data/spot/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year}-{month:02d}.zip"
BYBIT_CHUNK_CANDLES = 6_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exchange", choices=["binance", "bybit", "both"], default="binance")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def month_range(months: int) -> list[tuple[int, int]]:
    today = datetime.now(UTC)
    out: list[tuple[int, int]] = []
    year, month = today.year, today.month
    for _ in range(months):
        month -= 1
        if month == 0:
            month = 12
            year -= 1
        out.append((year, month))
    return list(reversed(out))


def _normalise_ts(value: str) -> int:
    ts = int(value)
    # Some archive revisions ship microseconds instead of milliseconds.
    if ts > 10**14:
        ts //= 1000
    return ts


def parse_binance_archive(content: bytes, symbol: str, interval: str) -> list[Candle]:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        name = next((item for item in archive.namelist() if item.endswith(".csv")), None)
        if name is None:
            return []
        text = archive.read(name).decode("utf-8", errors="replace")

    candles: list[Candle] = []
    for row in csv.reader(io.StringIO(text)):
        if not row or not row[0].strip().isdigit():
            continue  # header
        open_time = _normalise_ts(row[0])
        close_time = _normalise_ts(row[6])
        candles.append(
            Candle(
                exchange="binance",
                symbol=symbol,
                interval=interval,
                open_time=open_time,
                close_time=close_time,
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]),
                quote_volume=float(row[7]) if len(row) > 7 and row[7] else None,
                trades=int(row[8]) if len(row) > 8 and row[8] else None,
                received_at=0,
            )
        )
    return candles


def download_month(symbol: str, interval: str, year: int, month: int) -> list[Candle]:
    url = BINANCE_ARCHIVE.format(symbol=symbol, interval=interval, year=year, month=month)
    response = httpx.get(url, timeout=120.0, follow_redirects=True)
    if response.status_code == 404:
        print(f"  {year}-{month:02d}: archive not published yet, skipping")
        return []
    response.raise_for_status()
    candles = parse_binance_archive(response.content, symbol, interval)
    print(f"  {year}-{month:02d}: {len(candles)} candles")
    return candles


async def bybit_history(symbols: list[str], interval: str, start_ms: int, end_ms: int) -> list[Candle]:
    settings = Settings.from_env()
    iv = interval_ms(interval)
    step = BYBIT_CHUNK_CANDLES * iv
    out: list[Candle] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        rest = BybitRest(client, AsyncRateLimiter(settings.rest_min_interval_sec), max_pages=10)
        for symbol in symbols:
            stream = Stream("bybit", symbol, "kl", interval)
            cursor = start_ms
            while cursor < end_ms:
                chunk_end = min(cursor + step - iv, end_ms)
                candles = await rest.backfill_klines(stream, cursor, chunk_end)
                out.extend(candles)
                print(f"  bybit {symbol} {cursor}..{chunk_end}: {len(candles)} candles")
                cursor = chunk_end + iv
    return out


async def write_batches(sink: SupabaseSink, candles: list[Candle], dry_run: bool) -> int:
    if dry_run:
        print(f"dry run: would write {len(candles)} rows")
        return len(candles)
    total = 0
    for start in range(0, len(candles), 5_000):
        batch = candles[start : start + 5_000]
        total += await sink.write([candle.to_row() for candle in batch])
        print(f"  written {total}/{len(candles)}")
    return total


async def amain() -> int:
    args = parse_args()
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    months = month_range(args.months)

    start = datetime(months[0][0], months[0][1], 1, tzinfo=UTC)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(datetime.now(UTC).timestamp() * 1000)

    settings = Settings.from_env()
    if (not settings.supabase_url or not settings.supabase_key) and not args.dry_run:
        print("COLLECTOR_SUPABASE_URL / COLLECTOR_SUPABASE_KEY are required")
        return 2

    sink = SupabaseSink(settings.supabase_url, settings.supabase_key, settings.supabase_table)
    candles: list[Candle] = []

    if args.exchange in {"binance", "both"}:
        print(f"binance: {len(months)} month(s), symbols={symbols}")
        for symbol in symbols:
            for year, month in months:
                candles.extend(download_month(symbol, args.interval, year, month))

    if args.exchange in {"bybit", "both"}:
        print(f"bybit: REST walk from {start.date()} to now, symbols={symbols}")
        candles.extend(await bybit_history(symbols, args.interval, start_ms, end_ms))

    unique = {(candle.pk): candle for candle in candles}
    print(f"parsed {len(candles)} candles, {len(unique)} unique")

    try:
        written = await write_batches(sink, list(unique.values()), args.dry_run)
    finally:
        await sink.close()
    print(f"done: {written} rows accepted")
    return 0


def main() -> int:
    return asyncio.run(amain())


if __name__ == "__main__":
    raise SystemExit(main())
