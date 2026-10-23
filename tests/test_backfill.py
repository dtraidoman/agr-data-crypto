from __future__ import annotations

import zipfile
from datetime import date
from pathlib import Path

import pytest

import scripts.backfill_derivatives as backfill
from collector.models import LongShort, OpenInterest
from collector.sink import MemorySink
from collector.timeutil import utc_day_start_ms

BASE = 1_700_000_000_000 // 300_000 * 300_000
DAY = date(2025, 10, 20)


class FakeRest:
    name = "binance"

    def __init__(self, oi: list[OpenInterest], ls: list[LongShort]) -> None:
        self._oi = oi
        self._ls = ls

    async def fetch_open_interest(self, symbol: str, start_ms: int, end_ms: int) -> list[OpenInterest]:
        return list(self._oi)

    async def fetch_long_short(self, symbol: str, start_ms: int, end_ms: int) -> list[LongShort]:
        return list(self._ls)


def oi(ts: int) -> OpenInterest:
    return OpenInterest(exchange="binance", symbol="BTCUSDT", ts=ts, open_interest=1.0)


def ls(ts: int) -> LongShort:
    return LongShort(
        exchange="binance",
        symbol="BTCUSDT",
        kind="global_account",
        ts=ts,
        long_value=1.0,
        short_value=1.0,
        ratio=1.0,
        unit="fraction",
    )


async def test_metrics_backfill_excludes_exclusive_end_boundary() -> None:
    start = BASE
    end = BASE + 600_000
    rest = FakeRest(
        oi=[oi(start), oi(start + 300_000), oi(end)],
        ls=[ls(start), ls(start + 300_000), ls(end)],
    )
    sink = MemorySink()

    written = await backfill.backfill_metrics(
        sink,
        rest,
        "binance",
        "BTCUSDT",
        start,
        end,
        {"oi", "ls"},
        dry_run=False,
    )

    assert written == 4
    assert sorted(row["ts"] for row in sink.rows("deriv_open_interest")) == [start, start + 300_000]
    assert sorted(row["ts"] for row in sink.rows("deriv_long_short")) == [start, start + 300_000]


async def test_missing_completed_archive_fails_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    day_start = utc_day_start_ms(DAY)
    day_end = day_start + 86_400_000

    async def missing(client: object, url: str) -> None:
        return None

    monkeypatch.setattr(backfill, "download", missing)
    monkeypatch.setattr(backfill, "now_ms", lambda: day_end + 1)

    with pytest.raises(RuntimeError, match="archive missing for completed UTC day"):
        await backfill.backfill_cvd(
            None,  # type: ignore[arg-type]
            MemorySink(),
            "binance",
            "BTCUSDT",
            day_start,
            day_end,
            replace_cvd=False,
            dry_run=True,
        )


async def test_empty_completed_archive_fails_loudly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    day_start = utc_day_start_ms(DAY)
    day_end = day_start + 86_400_000
    path = tmp_path / "empty.zip"
    member = "BTCUSDT-aggTrades-2025-10-20.csv"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member, "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n")

    async def empty(client: object, url: str) -> object:
        return path

    monkeypatch.setattr(backfill, "download", empty)
    monkeypatch.setattr(backfill, "now_ms", lambda: day_end + 1)

    with pytest.raises(RuntimeError, match="archive has no trades for completed UTC day"):
        await backfill.backfill_cvd(
            None,  # type: ignore[arg-type]
            MemorySink(),
            "binance",
            "BTCUSDT",
            day_start,
            day_end,
            replace_cvd=False,
            dry_run=True,
        )
