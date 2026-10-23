from __future__ import annotations

import os
import socket
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum


class Role(StrEnum):
    ALPHA = "alpha"
    BRAVO = "bravo"

    @property
    def peer(self) -> Role:
        return Role.BRAVO if self is Role.ALPHA else Role.ALPHA


PRIMARY_EXCHANGE = {Role.ALPHA: "binance", Role.BRAVO: "bybit"}
SECONDARY_EXCHANGE = {Role.ALPHA: "bybit", Role.BRAVO: "binance"}


def _str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return default if raw is None else raw.strip()


def _int(name: str, default: int) -> int:
    raw = _str(name)
    return default if raw == "" else int(raw)


def _bool(name: str, default: bool = False) -> bool:
    raw = _str(name).lower()
    return default if raw == "" else raw in {"1", "true", "yes", "on"}


def _csv(name: str, default: str) -> tuple[str, ...]:
    raw = _str(name, default)
    return tuple(part.strip().upper() for part in raw.split(",") if part.strip())


def _csv_lower(name: str, default: str) -> tuple[str, ...]:
    raw = _str(name, default)
    return tuple(part.strip().lower() for part in raw.split(",") if part.strip())


@dataclass(slots=True)
class Settings:
    role: Role = Role.ALPHA
    run_id: str = field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    enable_liquidations: bool = True
    # Venues whose websocket data plane is unreachable: poll trades over REST.
    cvd_rest_exchanges: tuple[str, ...] = ()
    cvd_poll_sec: int = 15
    cvd_seed_lookback_min: int = 10

    redis_url: str = ""
    supabase_url: str = ""
    supabase_key: str = ""

    soft_deadline_sec: int = 16_200
    lease_ttl_ms: int = 180_000
    heartbeat_interval_sec: int = 60
    takeover_poll_sec: int = 60
    startup_grace_sec: int = 120

    metrics_poll_sec: int = 300
    metrics_startup_lookback_sec: int = 86_400
    metrics_overlap_sec: int = 900
    trade_lookback_min: int = 10

    rest_min_interval_sec: float = 0.25
    rest_timeout_sec: float = 20.0
    rest_max_retries: int = 4

    ws_planned_reconnect_sec: int = 6 * 3600
    ws_reconnect_max_sec: int = 30

    flush_rows: int = 500
    flush_interval_sec: int = 5
    max_buffered_rows: int = 50_000

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    log_level: str = "INFO"
    dry_run: bool = False

    @classmethod
    def from_env(cls, prefix: str = "COLLECTOR_") -> Settings:
        role = Role(_str(f"{prefix}ROLE", "alpha").lower())
        run_id = _str(f"{prefix}RUN_ID") or f"{role}-{int(time.time())}-{uuid.uuid4().hex[:6]}"
        return cls(
            role=role,
            run_id=run_id,
            symbols=_csv(f"{prefix}SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"),
            enable_liquidations=_bool(f"{prefix}ENABLE_LIQUIDATIONS", True),
            cvd_rest_exchanges=_csv_lower(f"{prefix}CVD_REST_EXCHANGES", ""),
            cvd_poll_sec=_int(f"{prefix}CVD_POLL_SEC", 15),
            cvd_seed_lookback_min=_int(f"{prefix}CVD_SEED_LOOKBACK_MIN", 10),
            redis_url=_str(f"{prefix}REDIS_URL"),
            supabase_url=_str(f"{prefix}SUPABASE_URL").rstrip("/"),
            supabase_key=_str(f"{prefix}SUPABASE_KEY"),
            soft_deadline_sec=_int(f"{prefix}SOFT_DEADLINE_SEC", 16_200),
            lease_ttl_ms=_int(f"{prefix}LEASE_TTL_MS", 180_000),
            heartbeat_interval_sec=_int(f"{prefix}HEARTBEAT_SEC", 60),
            takeover_poll_sec=_int(f"{prefix}TAKEOVER_POLL_SEC", 60),
            startup_grace_sec=_int(f"{prefix}STARTUP_GRACE_SEC", 120),
            metrics_poll_sec=_int(f"{prefix}METRICS_POLL_SEC", 300),
            metrics_startup_lookback_sec=_int(f"{prefix}METRICS_STARTUP_LOOKBACK_SEC", 86_400),
            metrics_overlap_sec=_int(f"{prefix}METRICS_OVERLAP_SEC", 900),
            trade_lookback_min=_int(f"{prefix}TRADE_LOOKBACK_MIN", 10),
            rest_min_interval_sec=float(_str(f"{prefix}REST_MIN_INTERVAL_SEC") or 0.25),
            rest_timeout_sec=float(_str(f"{prefix}REST_TIMEOUT_SEC") or 20.0),
            rest_max_retries=_int(f"{prefix}REST_MAX_RETRIES", 4),
            ws_planned_reconnect_sec=_int(f"{prefix}WS_PLANNED_RECONNECT_SEC", 21_600),
            ws_reconnect_max_sec=_int(f"{prefix}WS_RECONNECT_MAX_SEC", 30),
            flush_rows=_int(f"{prefix}FLUSH_ROWS", 500),
            flush_interval_sec=_int(f"{prefix}FLUSH_INTERVAL_SEC", 5),
            max_buffered_rows=_int(f"{prefix}MAX_BUFFERED_ROWS", 50_000),
            telegram_bot_token=_str(f"{prefix}TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=_str(f"{prefix}TELEGRAM_CHAT_ID"),
            log_level=_str(f"{prefix}LOG_LEVEL", "INFO").upper(),
            dry_run=_bool(f"{prefix}DRY_RUN", False),
        )

    @property
    def primary_exchange(self) -> str:
        return PRIMARY_EXCHANGE[self.role]

    @property
    def secondary_exchange(self) -> str:
        return SECONDARY_EXCHANGE[self.role]

    def validate(self) -> list[str]:
        problems: list[str] = []
        if self.lease_ttl_ms < self.heartbeat_interval_sec * 2000:
            problems.append("lease TTL must be >= 2 heartbeat intervals")
        if not self.redis_url:
            problems.append("Redis URL is empty; running without A/B leases")
        if not self.supabase_url or not self.supabase_key:
            problems.append("Supabase is not configured; use dry-run/memory sink only")
        if not self.symbols:
            problems.append("COLLECTOR_SYMBOLS is empty")
        return problems
