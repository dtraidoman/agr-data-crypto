from __future__ import annotations

import time

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
    return int(time.time() * 1000)


def interval_ms(interval: str) -> int:
    try:
        return INTERVAL_MS[interval]
    except KeyError as exc:
        raise ValueError(f"unsupported interval: {interval!r}") from exc


def last_closed_open_time(ts_ms: int, iv_ms: int) -> int:
    """open_time of the most recent candle that has already closed."""
    return (ts_ms // iv_ms - 1) * iv_ms


def candle_is_closed(open_time: int, iv_ms: int, ts_ms: int) -> bool:
    return open_time + iv_ms <= ts_ms
