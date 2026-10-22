from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping, Sequence
from typing import Any

from .config import Settings
from .exchanges.base import ExchangeAdapter
from .models import Candle, Stream
from .notify import Notifier
from .pipeline import Pipeline
from .roles import desired_streams
from .state.manager import StateManager
from .timeutil import interval_ms, last_closed_open_time, now_ms

log = logging.getLogger(__name__)

LEASE_KEY_PREFIX = "lease:"


class Supervisor:
    """Owns the lifecycle of one collector run.

    There is no "leader": every role independently reconciles the set of
    streams it may hold. A stream is collectable when its lease is held, so
    the same code path handles first acquisition, renewal after a blip and
    takeover from a dead peer.
    """

    def __init__(
        self,
        settings: Settings,
        state: StateManager,
        adapters: Mapping[str, ExchangeAdapter],
        pipeline: Pipeline,
        notifier: Notifier | None = None,
        *,
        clock=time.monotonic,
        wall=now_ms,
    ) -> None:
        self.settings = settings
        self.state = state
        self.adapters = adapters
        self.pipeline = pipeline
        self.notifier = notifier or Notifier()
        self._clock = clock
        self._wall = wall

        self._leases: dict[str, Any] = {}
        self._tasks: dict[tuple[str, str], asyncio.Task] = {}
        self._task_symbols: dict[tuple[str, str], tuple[str, ...]] = {}
        self._stop = asyncio.Event()

        self._started_at = 0.0
        self._deadline = 0.0
        self._stop_at = 0.0
        self._next_heartbeat = 0.0
        self._next_reconcile = 0.0
        self._next_sink_flush = 0.0
        self._next_cursor_flush = 0.0
        self._degraded_since: float | None = None
        self._peer_seen = False
        self._takeovers = 0
        self.exit_code = 0

    # --------------------------------------------------------------- lifecycle
    async def run(self) -> int:
        self._started_at = self._clock()
        self._deadline = self._started_at + self.settings.soft_deadline_sec
        # A deadline shorter than the drain reserve would abort immediately;
        # always allow a few seconds to actually collect something.
        self._stop_at = max(self._deadline - self.settings.drain_reserve_sec, self._started_at + 5.0)
        self.state.start()

        await self._reconcile(initial=True)
        if not self._leases:
            log.warning("no streams acquired: entering standby")
            if not await self._standby():
                log.info("standby expired with nothing to do, exiting")
                await self._shutdown()
                return 0

        await self._loop()
        await self._shutdown()
        return self.exit_code

    def request_stop(self) -> None:
        log.info("stop requested")
        self._stop.set()

    async def _loop(self) -> None:
        while not self._stop.is_set():
            now = self._clock()
            if now >= self._stop_at:
                log.info("soft deadline reached, shutting down")
                break

            if self.state.degraded:
                if self._degraded_since is None:
                    self._degraded_since = now
                elif now - self._degraded_since > self.settings.degraded_max_sec:
                    log.error("state store unavailable for %ss, exiting non-zero", self.settings.degraded_max_sec)
                    self.exit_code = 1
                    break
            else:
                self._degraded_since = None

            if now >= self._next_heartbeat:
                await self._heartbeat()
                self._next_heartbeat = now + self.settings.heartbeat_interval_sec

            if now >= self._next_reconcile:
                await self._reconcile()
                self._next_reconcile = now + self.settings.takeover_poll_interval_sec

            if now >= self._next_sink_flush:
                await self._sink_flush()
                self._next_sink_flush = now + self.settings.flush_interval_sec

            if now >= self._next_cursor_flush:
                await self._cursor_flush()
                self._next_cursor_flush = now + self.settings.cursor_flush_interval_sec

            await asyncio.sleep(0.5)

    async def _shutdown(self) -> None:
        self._stop.set()

        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._task_symbols.clear()

        # Order is critical: data first, then the cursor that points at it.
        try:
            written = await self.pipeline.drain()
            if written:
                log.info("drained %d rows on shutdown", written)
        except Exception:
            log.exception("final drain failed; cursors will not advance past the lost rows")
            self.exit_code = 1

        self._publish_cursors()
        self.state.release_all(list(self._leases.values()))
        self._leases.clear()
        self.state.mark_stopped()
        log.info("shutdown complete: %s", self.pipeline.stats)

    # ------------------------------------------------------------------ leases
    async def _reconcile(self, initial: bool = False) -> None:
        # Never assume the peer is dead two seconds after we started: on a cold
        # start both roles would grab all six streams. Takeover requires that
        # the peer was observed alive at least once, or that enough time has
        # passed for its cron to have fired (`startup_grace_sec`).
        run_age = self._clock() - self._started_at
        peer_alive = self.state.peer_alive(self.settings.role.peer)
        if peer_alive:
            self._peer_seen = True
        allow_takeover = (
            not peer_alive
            and run_age >= self.settings.takeover_grace_sec
            and (self._peer_seen or run_age >= self.settings.startup_grace_sec)
        )
        targets = desired_streams(
            self.settings.role,
            self.settings.interval,
            self.settings.enable_trades,
            allow_takeover=allow_takeover,
        )
        # aggTrades are modelled but not implemented yet. Leasing a stream that
        # has no feed would burn REST quota on pointless kline backfills.
        targets = [stream for stream in targets if stream.is_klines]

        acquired: list[Stream] = []
        for stream in targets:
            if self._stop.is_set():
                return
            if stream.lease_name in self._leases:
                continue
            lease = self.state.acquire(stream)
            if lease is None:
                continue
            self._leases[stream.lease_name] = lease
            acquired.append(stream)
            log.info("acquired %s (fence=%s)", stream.label, lease.fence)

        # Subscribe first, then backfill the gap: the websocket is buffering
        # while the REST catch-up runs, so no candle can slip between them.
        self._sync_tasks()

        for stream in acquired:
            await self._backfill(stream)

        if initial and self._leases:
            log.info("collecting %s", ", ".join(sorted(self._leases)))

    async def _heartbeat(self) -> None:
        result = self.state.heartbeat(list(self._leases.values()))
        if not result.ok:
            return
        if result.lost:
            for key in result.lost:
                name = key.removeprefix(LEASE_KEY_PREFIX)
                lease = self._leases.pop(name, None)
                if lease is not None:
                    log.warning("lease lost: %s", lease.stream.label)
            self._sync_tasks()
            if self.notifier.enabled:
                await self.notifier.send(
                    f"[{self.settings.role}] lost lease(s): {', '.join(result.lost)}",
                    kind="lease_lost",
                    dedupe_ttl_ms=1_800_000,
                )

    async def _standby(self) -> bool:
        until = self._clock() + self.settings.standby_max_wait_sec
        while not self._stop.is_set() and self._clock() < until:
            await asyncio.sleep(min(5.0, max(0.2, until - self._clock())))
            self.state.heartbeat(list(self._leases.values()))
            await self._reconcile()
            if self._leases:
                log.info("standby resolved: streams acquired")
                return True
        return False

    # -------------------------------------------------------------- backfilling
    async def _backfill(self, stream: Stream) -> None:
        adapter = self.adapters.get(stream.exchange)
        if adapter is None:
            log.error("no adapter for exchange %s", stream.exchange)
            return

        iv = interval_ms(stream.interval)
        last_closed = last_closed_open_time(self._wall(), iv)
        cursor = self.state.cursor(stream)

        if cursor and cursor >= last_closed:
            return
        start = (
            cursor + iv
            if cursor
            # Cold start: the cursor is unknown, so fetch a small overlap
            # instead of walking from genesis. Deep history is a separate job.
            else max(0, last_closed - (self.settings.cold_start_candles - 1) * iv)
        )

        try:
            candles = await adapter.backfill_klines(stream, start, last_closed)
        except Exception:
            log.exception("backfill failed for %s", stream.label)
            return

        if not candles:
            return
        self.pipeline.add(candles)
        try:
            await self.pipeline.flush(force=True)
        except Exception:
            log.exception("backfill flush failed for %s", stream.label)
            return
        log.info(
            "backfill %s: %d candles %s..%s",
            stream.label,
            len(candles),
            candles[0].open_time,
            candles[-1].open_time,
        )

    # ------------------------------------------------------------- websockets
    async def _sink_flush(self) -> None:
        try:
            await self.pipeline.flush()
        except Exception:
            log.exception("sink flush failed; %d rows kept in buffer", self.pipeline.buffered)

    async def _cursor_flush(self) -> None:
        try:
            await self.pipeline.flush()
        except Exception:
            log.exception("sink flush failed before cursor advance")
            return
        self._publish_cursors()

    def _publish_cursors(self) -> None:
        values: dict[Stream, tuple[int, int]] = {}
        for lease in self._leases.values():
            cursor = self.pipeline.cursors.get(lease.stream)
            if cursor:
                values[lease.stream] = (cursor, lease.fence)
        if not values:
            return
        advanced = self.state.flush_cursors(values)
        if advanced:
            log.debug("cursors advanced: %d", advanced)

    def _sync_tasks(self) -> None:
        by_exchange: dict[str, list[str]] = {}
        for lease in self._leases.values():
            stream = lease.stream
            if stream.kind == "kl":
                by_exchange.setdefault(stream.exchange, []).append(stream.symbol)

        wanted = {(exchange, "kl") for exchange in by_exchange}
        for key, task in list(self._tasks.items()):
            if key not in wanted and not task.done():
                task.cancel()
                self._tasks.pop(key, None)
                self._task_symbols.pop(key, None)

        for exchange, symbols in by_exchange.items():
            key = (exchange, "kl")
            desired = tuple(sorted(symbols))
            task = self._tasks.get(key)
            if task is not None and not task.done() and self._task_symbols.get(key) == desired:
                continue
            if task is not None and not task.done():
                task.cancel()
            self._task_symbols[key] = desired
            self._tasks[key] = asyncio.create_task(self._ws_loop(exchange, desired))

    async def _ws_loop(self, exchange: str, symbols: Sequence[str]) -> None:
        adapter = self.adapters.get(exchange)
        if adapter is None:
            return
        log.info("%s websocket starting for %s", exchange, ",".join(symbols))
        try:
            await adapter.stream_klines(list(symbols), self.settings.interval, self._on_candle, self._stop)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("%s websocket loop crashed", exchange)

    def _on_candle(self, candle: Candle) -> None:
        if candle.stream.lease_name not in self._leases:
            return
        self.pipeline.add([candle])

    # ------------------------------------------------------------------ status
    def status(self) -> dict[str, Any]:
        return {
            "role": str(self.settings.role),
            "run_id": self.settings.run_id,
            "uptime_sec": int(self._clock() - self._started_at) if self._started_at else 0,
            "streams": sorted(self._leases),
            "degraded": self.state.degraded,
            "exit_code": self.exit_code,
            **{f"pipeline_{key}": value for key, value in self.pipeline.stats.items()},
        }
