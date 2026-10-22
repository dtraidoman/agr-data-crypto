#!/usr/bin/env python3
"""Verify that every expected 1m candle is present in the sink.

Runs at the end of every collector job and on demand. Exits 1 when gaps are
found, so a workflow step can flag a broken handoff (it is marked
``continue-on-error`` on purpose: a red collection job is more useful than a
missing one).

Reads Supabase over PostgREST; no database driver is required.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict

import httpx

IV_MS = 60_000
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
EXCHANGES = ("binance", "bybit")
PAGE = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-minutes", type=int, default=260)
    parser.add_argument("--exchange", action="append", choices=EXCHANGES)
    parser.add_argument("--symbol", action="append", choices=SYMBOLS)
    parser.add_argument("--max-gaps", type=int, default=0)
    parser.add_argument("--expect-all", action="store_true", help="require all 6 streams, not just the ones seen")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def now_ms() -> int:
    return int(time.time() * 1000)


def fetch_rows(base_url: str, key: str, start_ms: int, exchanges: list[str], symbols: list[str]) -> list[dict]:
    url = f"{base_url.rstrip('/')}/rest/v1/ohlcv"
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
    params = {
        "select": "exchange,symbol,open_time",
        "interval": "eq.1m",
        "is_closed": "is.true",
        "open_time": f"gte.{start_ms}",
        "order": "exchange.asc,symbol.asc,open_time.asc",
        "limit": str(PAGE),
    }
    if exchanges:
        params["exchange"] = f"in.({','.join(exchanges)})"
    if symbols:
        params["symbol"] = f"in.({','.join(symbols)})"

    rows: list[dict] = []
    offset = 0
    while True:
        response = httpx.get(url, headers=headers, params={**params, "offset": str(offset)}, timeout=30.0)
        if response.status_code >= 400:
            raise SystemExit(f"supabase error {response.status_code}: {response.text[:300]}")
        batch = response.json()
        if not isinstance(batch, list):
            raise SystemExit(f"unexpected response: {str(batch)[:300]}")
        rows.extend(batch)
        if len(batch) < PAGE:
            return rows
        offset += PAGE
        if offset > 2_000_000:  # safety valve
            return rows


def analyze(rows: list[dict], start_ms: int, end_ms: int, expect_all: bool, exchanges, symbols) -> list[dict]:
    seen: dict[tuple[str, str], set[int]] = defaultdict(set)
    for row in rows:
        seen[(row["exchange"], row["symbol"])].add(int(row["open_time"]))

    expected: set[tuple[str, str]] = set(seen)
    if expect_all:
        expected |= {(exchange, symbol) for exchange in (exchanges or EXCHANGES) for symbol in (symbols or SYMBOLS)}

    expected_times = list(range(start_ms, end_ms + 1, IV_MS))
    report = []
    for stream in sorted(expected):
        present = seen.get(stream, set())
        missing = [ts for ts in expected_times if ts not in present]
        newest = max(present) if present else None
        report.append(
            {
                "exchange": stream[0],
                "symbol": stream[1],
                "rows": len(present),
                "missing": len(missing),
                "first_gap": missing[0] if missing else None,
                "last_gap": missing[-1] if missing else None,
                "newest": newest,
                "age_minutes": round((end_ms - newest) / IV_MS, 1) if newest else None,
            }
        )
    return report


def main() -> int:
    args = parse_args()
    base_url = os.getenv("COLLECTOR_SUPABASE_URL", "").strip()
    key = os.getenv("COLLECTOR_SUPABASE_KEY", "").strip()
    if not base_url or not key:
        print("COLLECTOR_SUPABASE_URL / COLLECTOR_SUPABASE_KEY are not set; skipping continuity check")
        return 0

    end_ms = (now_ms() // IV_MS - 1) * IV_MS  # last closed candle
    start_ms = end_ms - (args.window_minutes - 1) * IV_MS

    rows = fetch_rows(base_url, key, start_ms, args.exchange or [], args.symbol or [])
    report = analyze(rows, start_ms, end_ms, args.expect_all, args.exchange or [], args.symbol or [])

    if args.json:
        print(json.dumps({"window_minutes": args.window_minutes, "streams": report}, indent=2))
    else:
        print(f"continuity window: {args.window_minutes} min ({start_ms}..{end_ms})")
        print(f"{'stream':24} {'rows':>6} {'missing':>8} {'age_min':>8}  first_gap")
        for item in report:
            stream = f"{item['exchange']}:{item['symbol']}"
            print(
                f"{stream:24} {item['rows']:>6} {item['missing']:>8} "
                f"{item['age_minutes'] if item['age_minutes'] is not None else '-':>8}  {item['first_gap']}"
            )

    total_missing = sum(item["missing"] for item in report)
    if total_missing > args.max_gaps:
        print(f"FAIL: {total_missing} missing candles (max allowed {args.max_gaps})", file=sys.stderr)
        return 1
    print("OK: no gaps in the window")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
