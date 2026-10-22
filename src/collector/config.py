from __future__ import annotations

import os
import socket
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum


class Role(StrEnum):
    """Two symmetric roles. Each one is primary for exactly one exchange."""

    ALPHA = "alpha"
    BRAVO = "bravo"

    @property
    def peer(self) -> Role:
        return Role.BRAVO if self is Role.ALPHA else Role.ALPHA


def _str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return default if raw is None or raw == "" else int(raw)


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return default if raw is None or raw == "" else float(raw)


def _bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class Settings:
    role: Role = Role.ALPHA
    run_id: str = ""
    interval: str = "1m"

    redis_url: str = ""
    supabase_url: str = ""
    supabase_key: str = ""
    supabase_table: str = "ohlcv"

    # 3h50m: the successor starts at T+4h00m, the job is killed at T+4h25m.
    soft_deadline_sec: int = 13_800
    drain_reserve_sec: int = 60
    # Upstash Free allows ~10k commands/day, so coordination is deliberately
    # slow: 2 roles x (60 heartbeats + 30 cursor flushes + 30 peer checks)
    # per hour is ~5.8k/day, leaving room for the watchdog and restarts.
    lease_ttl_ms: int = 180_000
    heartbeat_ttl_ms: int = 300_000
    heartbeat_interval_sec: int = 60
    cursor_flush_interval_sec: int = 120
    takeover_poll_interval_sec: int = 120
    takeover_grace_sec: int = 120
    # The two roles start 10 minutes apart, so "peer heartbeat absent" right
    # after boot means "peer has not started yet", not "peer is dead".
    startup_grace_sec: int = 900
    standby_max_wait_sec: int = 600
    degraded_max_sec: int = 900

    flush_rows: int = 200
    flush_interval_sec: int = 5
    cold_start_candles: int = 60

    rest_min_interval_sec: float = 0.3
    rest_timeout_sec: float = 15.0
    rest_max_pages: int = 10

    ws_planned_reconnect_sec: int = 6 * 3600
    ws_reconnect_max_sec: int = 30
    ws_max_attempts_per_minute: int = 5

    enable_trades: bool = False

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    log_level: str = "INFO"
    dry_run: bool = False
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls, prefix: str = "COLLECTOR_") -> Settings:
        role = Role(_str(f"{prefix}ROLE", "alpha").lower())
        run_id = _str(f"{prefix}RUN_ID") or f"{role}-{int(time.time())}-{uuid.uuid4().hex[:6]}"
        return cls(
            role=role,
            run_id=run_id,
            interval=_str(f"{prefix}INTERVAL", "1m"),
            redis_url=_str(f"{prefix}REDIS_URL"),
            supabase_url=_str(f"{prefix}SUPABASE_URL").rstrip("/"),
            supabase_key=_str(f"{prefix}SUPABASE_KEY"),
            supabase_table=_str(f"{prefix}SUPABASE_TABLE", "ohlcv"),
            soft_deadline_sec=_int(f"{prefix}SOFT_DEADLINE_SEC", 13_800),
            drain_reserve_sec=_int(f"{prefix}DRAIN_RESERVE_SEC", 60),
            lease_ttl_ms=_int(f"{prefix}LEASE_TTL_MS", 180_000),
            heartbeat_ttl_ms=_int(f"{prefix}HEARTBEAT_TTL_MS", 300_000),
            heartbeat_interval_sec=_int(f"{prefix}HEARTBEAT_INTERVAL_SEC", 60),
            cursor_flush_interval_sec=_int(f"{prefix}CURSOR_FLUSH_INTERVAL_SEC", 120),
            takeover_poll_interval_sec=_int(f"{prefix}TAKEOVER_POLL_INTERVAL_SEC", 120),
            takeover_grace_sec=_int(f"{prefix}TAKEOVER_GRACE_SEC", 120),
            startup_grace_sec=_int(f"{prefix}STARTUP_GRACE_SEC", 900),
            standby_max_wait_sec=_int(f"{prefix}STANDBY_MAX_WAIT_SEC", 600),
            degraded_max_sec=_int(f"{prefix}DEGRADED_MAX_SEC", 900),
            flush_rows=_int(f"{prefix}FLUSH_ROWS", 200),
            flush_interval_sec=_int(f"{prefix}FLUSH_INTERVAL_SEC", 5),
            cold_start_candles=_int(f"{prefix}COLD_START_CANDLES", 60),
            rest_min_interval_sec=_float(f"{prefix}REST_MIN_INTERVAL_SEC", 0.3),
            rest_timeout_sec=_float(f"{prefix}REST_TIMEOUT_SEC", 15.0),
            rest_max_pages=_int(f"{prefix}REST_MAX_PAGES", 10),
            ws_planned_reconnect_sec=_int(f"{prefix}WS_PLANNED_RECONNECT_SEC", 6 * 3600),
            ws_reconnect_max_sec=_int(f"{prefix}WS_RECONNECT_MAX_SEC", 30),
            enable_trades=_bool(f"{prefix}ENABLE_TRADES", False),
            telegram_bot_token=_str(f"{prefix}TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=_str(f"{prefix}TELEGRAM_CHAT_ID"),
            log_level=_str(f"{prefix}LOG_LEVEL", "INFO").upper(),
            dry_run=_bool(f"{prefix}DRY_RUN", False),
        )

    @property
    def hostname(self) -> str:
        return socket.gethostname()

    def validate(self) -> list[str]:
        """Return human-readable problems; empty means usable."""
        problems: list[str] = []
        if self.interval not in {"1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"}:
            problems.append(f"unsupported interval {self.interval!r}")
        if self.lease_ttl_ms < self.heartbeat_interval_sec * 2000:
            problems.append("lease TTL must be at least 2 heartbeat intervals")
        if not self.redis_url:
            problems.append("COLLECTOR_REDIS_URL is empty (degraded mode)")
        if not self.supabase_url or not self.supabase_key:
            problems.append("Supabase sink is not configured (dry run only)")
        if self.enable_trades:
            problems.append("COLLECTOR_ENABLE_TRADES is not implemented yet; trade streams are ignored")
        return problems
