from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass

import httpx

from collector.config import Settings
from collector.timeutil import iso_to_ms, ms_to_iso, now_ms

log = logging.getLogger("continuity")

STEP_MS = {
    "deriv_open_interest": 5 * 60_000,
    "deriv_long_short": 5 * 60_000,
    "deriv_cvd_1m": 60_000,
}
MAX_AGE_MS = {
    "deriv_open_interest": 15 * 60_000,
    "deriv_long_short": 15 * 60_000,
    "deriv_cvd_1m": 5 * 60_000,
}
LS_KINDS = {
    "binance": (
        "global_account",
        "top_account",
        "top_position",
        "taker_volume",
    ),
    "bybit": ("account",),
}
PAGE_SIZE = 1000


@dataclass(slots=True)
class Check:
    table: str
    exchange: str
    symbol: str
    kind: str | None
    mode: str
    rows: int
    expected: int
    first_ts: int | None
    last_ts: int | None
    newest_age_ms: int | None
    largest_gap_ms: int | None
    ok: bool


async def fetch_window(
    client: httpx.AsyncClient,
    settings: Settings,
    table: str,
    exchange: str,
    symbol: str,
    start_ms: int,
    end_ms: int | None,
    *,
    kind: str | None = None,
) -> list[int]:
    """Read every timestamp in [start, end) using keyset pagination.

    PostgREST caps a response at ``PAGE_SIZE`` rows, which is smaller than a
    full day of one-minute CVD data (1440 rows). Walking the primary key keeps
    the historical reconciliation check exact instead of silently truncating.
    """
    step = STEP_MS[table]
    span = (end_ms - start_ms) if end_ms is not None else 86_400_000
    max_pages = min(1000, max(2, span // step // PAGE_SIZE + 2))
    headers = {"apikey": settings.supabase_key, "Authorization": f"Bearer {settings.supabase_key}"}
    rows: list[int] = []
    cursor = start_ms
    for _ in range(max_pages):
        params: list[tuple[str, str]] = [
            ("select", "ts"),
            ("exchange", f"eq.{exchange}"),
            ("symbol", f"eq.{symbol}"),
            ("order", "ts.asc"),
            ("limit", str(PAGE_SIZE)),
        ]
        if kind is not None:
            params.append(("kind", f"eq.{kind}"))
        if rows:
            params.append(("ts", f"gt.{cursor}"))
        else:
            params.append(("ts", f"gte.{cursor}"))
        if end_ms is not None:
            params.append(("ts", f"lt.{end_ms}"))
        response = await client.get(
            f"{settings.supabase_url}/rest/v1/{table}",
            headers=headers,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        page = sorted(int(row["ts"]) for row in response.json())
        if not page:
            break
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
        next_cursor = page[-1] + 1
        if next_cursor <= cursor:
            raise RuntimeError(f"{table} pagination did not advance past {cursor}")
        cursor = next_cursor
    return sorted(set(rows))


def evaluate(
    table: str,
    exchange: str,
    symbol: str,
    kind: str | None,
    rows: list[int],
    *,
    mode: str,
    now: int,
    window_ms: int | None = None,
    grid_start: int | None = None,
    grid_last: int | None = None,
) -> Check:
    step = STEP_MS[table]
    gaps = [b - a for a, b in zip(rows, rows[1:], strict=False)]
    largest_gap = max(gaps) if gaps else None

    if mode == "history":
        assert grid_start is not None and grid_last is not None
        expected = 0 if grid_last < grid_start else (grid_last - grid_start) // step + 1
        count_ok = len(rows) == expected
        boundary_ok = (expected == 0 and not rows) or (
            len(rows) == expected
            and bool(rows)
            and rows[0] == grid_start
            and rows[-1] == grid_last
        )
        gap_ok = all(gap == step for gap in gaps) if gaps else expected <= 1
        age: int | None = None
        age_ok = True
    else:
        assert window_ms is not None
        expected = max(1, window_ms // step)
        # A recently started collector or a single delayed poll should not fail
        # the live check; age and gap checks below still catch real outages.
        count_ok = len(rows) >= expected - 2
        boundary_ok = True
        gap_ok = largest_gap is None or largest_gap <= step * 2
        age = now - rows[-1] if rows else None
        age_ok = age is not None and age <= MAX_AGE_MS[table]

    return Check(
        table=table,
        exchange=exchange,
        symbol=symbol,
        kind=kind,
        mode=mode,
        rows=len(rows),
        expected=expected,
        first_ts=rows[0] if rows else None,
        last_ts=rows[-1] if rows else None,
        newest_age_ms=age,
        largest_gap_ms=largest_gap,
        ok=count_ok and boundary_ok and age_ok and gap_ok,
    )


def _range_start(steps: dict[str, int], table: str, start_ms: int, end_ms: int) -> tuple[int, int]:
    step = steps[table]
    grid_start = start_ms if start_ms % step == 0 else (start_ms // step + 1) * step
    grid_last = (end_ms - 1) // step * step
    return grid_start, grid_last


async def amain() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-minutes", type=int, default=260)
    parser.add_argument("--start", help="ISO-8601 UTC inclusive; requires --end")
    parser.add_argument("--end", help="ISO-8601 UTC exclusive; requires --start")
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--tables",
        default=",".join(STEP_MS),
        help="Comma-separated tables to check (default: all)",
    )
    args = parser.parse_args()

    tables = tuple(part.strip() for part in args.tables.split(",") if part.strip())
    unknown = sorted(set(tables) - set(STEP_MS))
    if not tables or unknown:
        parser.error(
            f"--tables must be a non-empty subset of {','.join(STEP_MS)}; "
            f"unknown={','.join(unknown) or '-'}"
        )
    if bool(args.start) != bool(args.end):
        parser.error("--start and --end must be used together")
    if args.window_minutes <= 0:
        parser.error("--window-minutes must be positive")

    settings = Settings.from_env()
    if not settings.supabase_url or not settings.supabase_key:
        log.error("Supabase is not configured")
        return 2

    now = now_ms()
    mode = "history" if args.start else "window"
    if mode == "history":
        start_ms = iso_to_ms(args.start)
        end_ms = iso_to_ms(args.end)
        if start_ms >= end_ms:
            parser.error("--start must be before --end")
        if end_ms > now:
            parser.error("--end must be in the past")
        if start_ms % 60_000 or end_ms % 60_000:
            parser.error("--start and --end must align to a minute")
        window_ms = None
        log.info("continuity history range %s..%s", ms_to_iso(start_ms), ms_to_iso(end_ms))
    else:
        window_ms = args.window_minutes * 60_000
        start_ms = now - window_ms
        end_ms = None
        log.info("continuity rolling window=%dmin", args.window_minutes)

    checks: list[Check] = []
    async with httpx.AsyncClient() as client:
        for table in tables:
            if mode == "history":
                assert end_ms is not None
                grid_start, grid_last = _range_start(STEP_MS, table, start_ms, end_ms)
            else:
                grid_start = grid_last = None
            for exchange in ("binance", "bybit"):
                for symbol in settings.symbols:
                    kinds = LS_KINDS[exchange] if table == "deriv_long_short" else (None,)
                    for kind in kinds:
                        try:
                            rows = await fetch_window(
                                client,
                                settings,
                                table,
                                exchange,
                                symbol,
                                start_ms,
                                end_ms,
                                kind=kind,
                            )
                        except Exception as exc:
                            log.error("%s %s %s %s query failed: %s", table, exchange, symbol, kind or "-", exc)
                            checks.append(
                                Check(
                                    table,
                                    exchange,
                                    symbol,
                                    kind,
                                    mode,
                                    0,
                                    0,
                                    None,
                                    None,
                                    None,
                                    None,
                                    False,
                                )
                            )
                            continue
                        check = evaluate(
                            table,
                            exchange,
                            symbol,
                            kind,
                            rows,
                            mode=mode,
                            now=now,
                            window_ms=window_ms,
                            grid_start=grid_start,
                            grid_last=grid_last,
                        )
                        checks.append(check)
                        if check.ok:
                            log.info(
                                "%s %s %s %s ok mode=%s rows=%d/%d first=%s last=%s age=%s gap=%s",
                                table,
                                exchange,
                                symbol,
                                kind or "-",
                                mode,
                                check.rows,
                                check.expected,
                                ms_to_iso(check.first_ts) if check.first_ts else "-",
                                ms_to_iso(check.last_ts) if check.last_ts else "-",
                                f"{(check.newest_age_ms or 0) / 60_000:.1f}min"
                                if check.newest_age_ms is not None
                                else "-",
                                f"{(check.largest_gap_ms or 0) / 60_000:.1f}min"
                                if check.largest_gap_ms is not None
                                else "-",
                            )
                        else:
                            log.error(
                                "%s %s %s %s FAIL mode=%s rows=%d/%d first=%s last=%s age=%s gap=%s",
                                table,
                                exchange,
                                symbol,
                                kind or "-",
                                mode,
                                check.rows,
                                check.expected,
                                ms_to_iso(check.first_ts) if check.first_ts else "missing",
                                ms_to_iso(check.last_ts) if check.last_ts else "missing",
                                f"{(check.newest_age_ms or 0) / 60_000:.1f}min"
                                if check.newest_age_ms is not None
                                else "n/a",
                                f"{(check.largest_gap_ms or 0) / 60_000:.1f}min"
                                if check.largest_gap_ms is not None
                                else "-",
                            )

    failed = any(not check.ok for check in checks)
    if args.json:
        print(
            json.dumps(
                {
                    "mode": mode,
                    "window_minutes": args.window_minutes if mode == "window" else None,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "ok": not failed,
                    "checks": [
                        {
                            "table": check.table,
                            "exchange": check.exchange,
                            "symbol": check.symbol,
                            "kind": check.kind,
                            "mode": check.mode,
                            "rows": check.rows,
                            "expected": check.expected,
                            "first_ts": check.first_ts,
                            "last_ts": check.last_ts,
                            "newest_age_ms": check.newest_age_ms,
                            "largest_gap_ms": check.largest_gap_ms,
                            "ok": check.ok,
                        }
                        for check in checks
                    ],
                },
                indent=2,
            )
        )
    log.info("continuity mode=%s result=%s", mode, "FAIL" if failed else "OK")
    return 1 if failed else 0


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(asyncio.run(amain()))


if __name__ == "__main__":
    main()
