from __future__ import annotations

import argparse
import asyncio
import gzip
import io
import logging
import os
import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path

import httpx

from collector.aggregation import aggregate_iter
from collector.config import Settings
from collector.exchanges.archive import (
    binance_archive_url,
    bybit_archive_url,
    iter_binance_csv,
    iter_bybit_csv,
)
from collector.exchanges.binance_rest import BinanceRest
from collector.exchanges.bybit_rest import BybitRest
from collector.exchanges.ratelimit import AsyncRateLimiter
from collector.models import Trade
from collector.sink import MemorySink, SupabaseSink
from collector.timeutil import iso_to_ms, now_ms, utc_dates, utc_day_start_ms

log = logging.getLogger("backfill")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill derivatives metrics")
    parser.add_argument("--exchange", choices=("binance", "bybit", "both"), default="both")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--days", type=int, default=5)
    parser.add_argument("--start", help="ISO-8601 UTC start; overrides --days")
    parser.add_argument("--end", help="ISO-8601 UTC end; defaults to now")
    parser.add_argument("--include", default="oi,ls,cvd")
    parser.add_argument("--replace-cvd", action="store_true", help="overwrite existing CVD rows")
    parser.add_argument("--prune", action="store_true", help="prune rows older than 5 days")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def parse_symbols(value: str) -> list[str]:
    return [part.strip().upper() for part in value.split(",") if part.strip()]


def time_range(args: argparse.Namespace) -> tuple[int, int]:
    end = iso_to_ms(args.end) if args.end else now_ms()
    start = iso_to_ms(args.start) if args.start else end - args.days * 86_400_000
    start = start // 60_000 * 60_000
    end = end // 60_000 * 60_000
    if start >= end:
        raise SystemExit("--start must be before --end")
    return start, end


async def download(client: httpx.AsyncClient, url: str) -> Path | None:
    path: Path | None = None
    try:
        async with client.stream("GET", url, timeout=180) as response:
            if response.status_code == 404:
                log.warning("archive not found: %s", url)
                return None
            response.raise_for_status()
            fd, name = tempfile.mkstemp(prefix="agr-", suffix=Path(url).suffix)
            os.close(fd)
            path = Path(name)
            with path.open("wb") as handle:
                async for chunk in response.aiter_bytes(1024 * 1024):
                    handle.write(chunk)
            return path
    except httpx.HTTPError:
        log.exception("download failed: %s", url)
        if path is not None:
            path.unlink(missing_ok=True)
        return None


def read_binance(path: Path, symbol: str) -> Iterator[Trade]:
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name.endswith(".csv")]
        if not members:
            return
        with archive.open(members[0]) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8")
            yield from iter_binance_csv(text, symbol)


def read_bybit(path: Path, symbol: str) -> Iterator[Trade]:
    with gzip.open(path, "rt", encoding="utf-8") as text:
        yield from iter_bybit_csv(text, symbol)


async def write_rows(sink, table: str, rows: list[dict], *, merge: bool, dry_run: bool) -> int:
    if dry_run or not rows:
        return len(rows)
    for offset in range(0, len(rows), 500):
        await sink.write(table, rows[offset : offset + 500], merge=merge)
    return len(rows)


async def backfill_cvd(
    client: httpx.AsyncClient,
    sink,
    exchange: str,
    symbol: str,
    start: int,
    end: int,
    *,
    replace_cvd: bool,
    dry_run: bool,
) -> int:
    written = 0
    for day in utc_dates(start, end):
        day_start = utc_day_start_ms(day)
        day_end = day_start + 86_400_000
        lo, hi = max(start, day_start), min(end, day_end)
        if lo >= hi:
            continue
        url = binance_archive_url(symbol, day) if exchange == "binance" else bybit_archive_url(symbol, day)
        path = await download(client, url)
        if path is None:
            if day_end <= now_ms():
                raise RuntimeError(f"archive missing for completed UTC day {day}: {url}")
            continue
        try:
            trades = read_binance(path, symbol) if exchange == "binance" else read_bybit(path, symbol)
            buckets = aggregate_iter(exchange, symbol, trades, lo, hi, source="archive", fill_empty=True)
            rows = [bucket.to_row() for bucket in buckets]
            trade_count = sum(bucket.trades for bucket in buckets)
            if day_end <= now_ms() and trade_count == 0:
                raise RuntimeError(f"archive has no trades for completed UTC day {day}: {url}")
            written += await write_rows(sink, "deriv_cvd_1m", rows, merge=replace_cvd, dry_run=dry_run)
            log.info(
                "cvd %s %s %s: %d buckets, %d trades",
                exchange,
                symbol,
                day,
                len(rows),
                trade_count,
            )
        finally:
            path.unlink(missing_ok=True)
    return written


async def backfill_metrics(
    sink,
    rest,
    exchange: str,
    symbol: str,
    start: int,
    end: int,
    includes: set[str],
    *,
    dry_run: bool,
) -> int:
    written = 0
    if "oi" in includes:
        oi = [row for row in await rest.fetch_open_interest(symbol, start, end) if row.ts < end]
        written += await write_rows(
            sink, "deriv_open_interest", [row.to_row() for row in oi], merge=True, dry_run=dry_run
        )
        log.info("oi %s %s: %d", exchange, symbol, len(oi))
    if "ls" in includes:
        ls = [row for row in await rest.fetch_long_short(symbol, start, end) if row.ts < end]
        written += await write_rows(
            sink, "deriv_long_short", [row.to_row() for row in ls], merge=True, dry_run=dry_run
        )
        log.info("ls %s %s: %d", exchange, symbol, len(ls))
    return written


async def amain(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    if not args.dry_run and (not settings.supabase_url or not settings.supabase_key):
        raise SystemExit("Supabase is not configured; use --dry-run or set COLLECTOR_SUPABASE_*")
    start, end = time_range(args)
    selected = parse_symbols(args.symbols)
    includes = {part.strip() for part in args.include.split(",") if part.strip()}
    exchanges = ["binance", "bybit"] if args.exchange == "both" else [args.exchange]
    log.info("backfill %s %d..%d exchanges=%s", selected, start, end, exchanges)

    client = httpx.AsyncClient(timeout=None, limits=httpx.Limits(max_connections=8))
    try:
        sink = MemorySink() if args.dry_run else SupabaseSink(settings.supabase_url, settings.supabase_key, client)
        rests = {
            "binance": BinanceRest(client, AsyncRateLimiter(0.2)),
            "bybit": BybitRest(client, AsyncRateLimiter(0.2)),
        }
        total = 0
        for exchange in exchanges:
            for symbol in selected:
                if "oi" in includes or "ls" in includes:
                    total += await backfill_metrics(
                        sink,
                        rests[exchange],
                        exchange,
                        symbol,
                        start,
                        end,
                        includes,
                        dry_run=args.dry_run,
                    )
                if "cvd" in includes:
                    total += await backfill_cvd(
                        client,
                        sink,
                        exchange,
                        symbol,
                        start,
                        end,
                        replace_cvd=args.replace_cvd,
                        dry_run=args.dry_run,
                    )
        if args.prune and not args.dry_run:
            await sink.rpc("prune_derivatives", {"p_days": 5})
            log.info("pruned rows older than 5 days")
        log.info("backfill done, rows written=%d", total)
        return 0
    finally:
        await client.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(asyncio.run(amain(parse_args())))


if __name__ == "__main__":
    main()
