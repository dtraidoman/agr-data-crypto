from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys

import httpx
import redis

from .config import Settings
from .exchanges import (
    AsyncRateLimiter,
    BinanceRest,
    BinanceWs,
    BybitRest,
    BybitWs,
    CompositeAdapter,
)
from .notify import Notifier
from .pipeline import Pipeline
from .roles import PRIMARY_EXCHANGE
from .sink import build_sink
from .state import StateManager
from .supervision import Supervisor

log = logging.getLogger(__name__)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
        stream=sys.stdout,
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)


def build_redis(settings: Settings):
    if not settings.redis_url:
        return None
    return redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=5.0,
        socket_connect_timeout=5.0,
        health_check_interval=30,
    )


def build_adapters(settings: Settings, http: httpx.AsyncClient) -> dict[str, CompositeAdapter]:
    ws_kwargs = {
        "planned_reconnect_sec": settings.ws_planned_reconnect_sec,
        "max_backoff_sec": settings.ws_reconnect_max_sec,
        "max_attempts_per_minute": settings.ws_max_attempts_per_minute,
    }
    rest_kwargs = {"max_pages": settings.rest_max_pages}
    return {
        "binance": CompositeAdapter(
            "binance",
            BinanceWs(**ws_kwargs),
            BinanceRest(http, AsyncRateLimiter(settings.rest_min_interval_sec), **rest_kwargs),
        ),
        "bybit": CompositeAdapter(
            "bybit",
            BybitWs(**ws_kwargs),
            BybitRest(http, AsyncRateLimiter(settings.rest_min_interval_sec), **rest_kwargs),
        ),
    }


async def run(settings: Settings) -> int:
    client = build_redis(settings)
    state = StateManager(
        client,
        settings.role,
        settings.run_id,
        primary_exchange=PRIMARY_EXCHANGE[settings.role],
        lease_ttl_ms=settings.lease_ttl_ms,
        heartbeat_ttl_ms=settings.heartbeat_ttl_ms,
    )
    sink = build_sink(settings)
    pipeline = Pipeline(sink, flush_rows=settings.flush_rows, flush_interval_sec=settings.flush_interval_sec)
    notifier = Notifier(settings.telegram_bot_token, settings.telegram_chat_id, state=state)

    http = httpx.AsyncClient(
        timeout=settings.rest_timeout_sec,
        headers={"User-Agent": "agr-data-crypto/0.1 (+github actions)"},
    )
    adapters = build_adapters(settings, http)
    supervisor = Supervisor(settings, state, adapters, pipeline, notifier)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, supervisor.request_stop)
        except (NotImplementedError, RuntimeError):
            # Windows: add_signal_handler is unsupported.
            with contextlib.suppress(ValueError):
                signal.signal(sig, lambda *_: supervisor.request_stop())

    try:
        code = await supervisor.run()
    finally:
        await pipeline.close()
        await http.aclose()
        if client is not None:
            with contextlib.suppress(Exception):
                client.close()

    log.info("exit: %s", supervisor.status())
    return code


def main() -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    log.info(
        "start role=%s run_id=%s interval=%s deadline=%ss pid=%s",
        settings.role,
        settings.run_id,
        settings.interval,
        settings.soft_deadline_sec,
        os.getpid(),
    )
    for problem in settings.validate():
        log.warning("config: %s", problem)

    try:
        return asyncio.run(run(settings))
    except KeyboardInterrupt:
        log.warning("interrupted")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
