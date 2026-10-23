from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


def now_ms() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)


def floor_ms(ts_ms: int, step_ms: int) -> int:
    return ts_ms // step_ms * step_ms


def utc_day_start_ms(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1000)


def utc_dates(start_ms: int, end_ms: int) -> list[date]:
    start = datetime.fromtimestamp(start_ms / 1000, tz=UTC).date()
    end = datetime.fromtimestamp(end_ms / 1000, tz=UTC).date()
    out: list[date] = []
    current = start
    while current <= end:
        out.append(current)
        current += timedelta(days=1)
    return out


def iso_to_ms(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def ms_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat()
