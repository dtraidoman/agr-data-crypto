from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys

import httpx
from redis.asyncio import Redis

from .config import Settings
from .exchanges.binance_rest import BinanceRest
from .exchanges.binance_ws import BinanceWs
from .exchanges.bybit_rest import BybitRest
from .exchanges.bybit_ws import BybitWs
from .exchanges.ratelimit import AsyncRateLimiter
from .notify import Notifier
from .pipeline import Pipeline
from .sink import MemorySink, SupabaseSink
from .state.manager import NullState, StateManager
from .supervisor import Supervisor
from .timeutil import now_ms

log = logging.getLogger(__name__)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


async def _build_state(settings: Settings) -> StateManager | NullState:
    if not settings.redis_url:
        log.warning("no Redis URL: A/B leases disabled")
        return NullState()
    redis = None
    try:
        redis = Redis.from_url(settings.redis_url, decode_responses=True, socket_timeout=10)
        await redis.ping()
    except Exception as exc:
        # Connection failures can echo the URL; keep credentials out of logs.
        log.warning("Redis unavailable: A/B leases disabled (%s)", type(exc).__name__)
        if redis is not None:
            await redis.aclose()
        return NullState()
    assert redis is not None
    return StateManager(
        redis,
        role=settings.role.value,
        run_id=settings.run_id,
        lease_ttl_ms=settings.lease_ttl_ms,
        heartbeat_ttl_ms=settings.lease_ttl_ms,
    )


async def amain() -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    for problem in settings.validate():
        log.warning("config: %s", problem)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop.set)
    if settings.soft_deadline_sec > 0:
        loop.call_later(settings.soft_deadline_sec, stop.set)
        log.info("soft deadline in %ds", settings.soft_deadline_sec)

    client = httpx.AsyncClient(
        timeout=settings.rest_timeout_sec,
        limits=httpx.Limits(max_connections=20),
    )
    try:
        if settings.dry_run:
            sink = MemorySink()
            log.warning("dry-run: using in-memory sink")
        elif settings.supabase_url and settings.supabase_key:
            sink = SupabaseSink(settings.supabase_url, settings.supabase_key, client)
        else:
            log.error("Supabase is not configured; set COLLECTOR_SUPABASE_URL/KEY")
            return 2

        rests = {
            "binance": BinanceRest(
                client,
                AsyncRateLimiter(settings.rest_min_interval_sec),
                max_retries=settings.rest_max_retries,
            ),
            "bybit": BybitRest(
                client,
                AsyncRateLimiter(settings.rest_min_interval_sec),
                max_retries=settings.rest_max_retries,
            ),
        }
        wss = {
            "binance": BinanceWs(
                planned_reconnect_sec=settings.ws_planned_reconnect_sec,
                max_backoff_sec=settings.ws_reconnect_max_sec,
            ),
            "bybit": BybitWs(
                planned_reconnect_sec=settings.ws_planned_reconnect_sec,
                max_backoff_sec=settings.ws_reconnect_max_sec,
            ),
        }
        pipeline = Pipeline(
            sink,
            flush_rows=settings.flush_rows,
            flush_interval_sec=settings.flush_interval_sec,
            max_buffered_rows=settings.max_buffered_rows,
        )
        state = await _build_state(settings)
        notifier = Notifier(settings.telegram_bot_token, settings.telegram_chat_id, client)
        supervisor = Supervisor(settings, rests, wss, sink, pipeline, state, notifier)
        pipeline_task = asyncio.create_task(pipeline.run(stop), name="pipeline")
        try:
            await supervisor.run(stop)
        finally:
            stop.set()
            await pipeline_task
        log.info("collector finished at %d", now_ms())
        return 0
    finally:
        await client.aclose()


def main() -> None:
    try:
        raise SystemExit(asyncio.run(amain()))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
