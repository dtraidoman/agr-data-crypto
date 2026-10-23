from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from .config import Settings
from .exchanges.base import RestAdapter, WsAdapter
from .models import Stream
from .notify import Notifier
from .pipeline import Pipeline, Sink
from .state.manager import NullState, StateManager
from .streams import CvdStream, LiquidationStream, MetricsPoller, RestCvdStream

log = logging.getLogger(__name__)


class Supervisor:
    def __init__(
        self,
        settings: Settings,
        rests: dict[str, RestAdapter],
        wss: dict[str, WsAdapter],
        sink: Sink,
        pipeline: Pipeline,
        state: StateManager | NullState,
        notifier: Notifier,
    ) -> None:
        self._settings = settings
        self._rests = rests
        self._wss = wss
        self._sink = sink
        self._pipeline = pipeline
        self._state = state
        self._notifier = notifier
        self._held: set[Stream] = set()
        self._stream_stops: dict[Stream, asyncio.Event] = {}
        self._tasks: list[asyncio.Task[None]] = []

    async def run(self, stop: asyncio.Event) -> None:
        settings = self._settings
        for exchange, rest in self._rests.items():
            poller = MetricsPoller(
                rest,
                self._pipeline,
                settings.symbols,
                poll_sec=settings.metrics_poll_sec,
                startup_lookback_sec=settings.metrics_startup_lookback_sec,
                overlap_sec=settings.metrics_overlap_sec,
            )
            self._tasks.append(asyncio.create_task(poller.run(stop), name=f"metrics:{exchange}"))

        self._tasks.append(asyncio.create_task(self._heartbeat_loop(stop), name="heartbeat"))

        for exchange in self._rests:
            primary = exchange == settings.primary_exchange
            for kind in ("cvd", "liq"):
                if kind == "liq" and not settings.enable_liquidations:
                    continue
                stream = Stream(exchange=exchange, symbol="ALL", kind=kind)
                self._tasks.append(
                    asyncio.create_task(
                        self._stream_loop(stream, primary, stop),
                        name=f"stream:{stream.label}",
                    )
                )

        await self._notifier.send(
            f"collector {settings.role} started: primary={settings.primary_exchange} "
            f"redis={'yes' if self._state.available else 'no'}"
        )
        try:
            await stop.wait()
        finally:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            for stream in list(self._held):
                await self._state.release(stream)
            self._held.clear()
            await self._state.close()
            await self._notifier.send(f"collector {settings.role} stopped")

    async def _heartbeat_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            if self._state.available:
                held = list(self._held)
                lost = await self._state.heartbeat(held)
                if lost:
                    log.warning("lost leases: %s", ", ".join(lost))
                    for stream in held:
                        if stream.lease_name in lost:
                            event = self._stream_stops.get(stream)
                            if event is not None:
                                event.set()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=max(5, self._settings.heartbeat_interval_sec))

    async def _stream_loop(self, stream: Stream, primary: bool, stop: asyncio.Event) -> None:
        if not self._state.available and not primary:
            return
        peer = self._settings.role.peer.value
        down_since: float | None = None
        while not stop.is_set():
            if not self._state.available:
                if not primary:
                    return
                await self._run_stream(stream, stop)
                return
            if not primary:
                if await self._state.peer_alive(peer):
                    down_since = None
                    await self._sleep_or_stop(stop, self._settings.takeover_poll_sec)
                    continue
                if down_since is None:
                    down_since = time.monotonic()
                    await self._sleep_or_stop(stop, self._settings.takeover_poll_sec)
                    continue
                if time.monotonic() - down_since < self._settings.startup_grace_sec:
                    await self._sleep_or_stop(stop, self._settings.takeover_poll_sec)
                    continue
            if not await self._state.acquire(stream):
                await self._sleep_or_stop(stop, self._settings.takeover_poll_sec)
                continue

            stream_stop = asyncio.Event()
            self._stream_stops[stream] = stream_stop
            self._held.add(stream)
            forward = asyncio.create_task(self._forward_stop(stop, stream_stop))
            monitor: asyncio.Task[None] | None = None
            if not primary:
                monitor = asyncio.create_task(self._peer_monitor(stream_stop, peer))
            try:
                await self._run_stream(stream, stream_stop)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("stream failed: %s", stream.label)
            finally:
                forward.cancel()
                if monitor is not None:
                    monitor.cancel()
                self._held.discard(stream)
                self._stream_stops.pop(stream, None)
                await self._state.release(stream)
            if not stop.is_set():
                await self._sleep_or_stop(stop, self._settings.takeover_poll_sec)

    async def _run_stream(self, stream: Stream, stop: asyncio.Event) -> None:
        settings = self._settings
        if stream.kind == "cvd":
            if stream.exchange in settings.cvd_rest_exchanges:
                await RestCvdStream(
                    self._rests[stream.exchange],
                    self._sink,
                    self._pipeline,
                    stream.exchange,
                    settings.symbols,
                    poll_sec=settings.cvd_poll_sec,
                    seed_lookback_min=settings.cvd_seed_lookback_min,
                    assume_full_range=stream.exchange == "binance",
                ).run(stop)
                return
            await CvdStream(
                self._wss[stream.exchange],
                self._rests[stream.exchange],
                self._sink,
                self._pipeline,
                stream.exchange,
                settings.symbols,
                trade_lookback_min=settings.trade_lookback_min,
            ).run(stop)
        elif stream.kind == "liq":
            await LiquidationStream(
                self._wss[stream.exchange], self._pipeline, stream.exchange, settings.symbols
            ).run(stop)

    async def _peer_monitor(self, stream_stop: asyncio.Event, peer: str) -> None:
        up_since: float | None = None
        while not stream_stop.is_set():
            await asyncio.sleep(self._settings.heartbeat_interval_sec)
            if await self._state.peer_alive(peer):
                up_since = up_since or time.monotonic()
                if time.monotonic() - up_since > self._settings.heartbeat_interval_sec * 2:
                    log.info("peer returned; releasing secondary stream")
                    stream_stop.set()
                    return
            else:
                up_since = None

    @staticmethod
    async def _forward_stop(global_stop: asyncio.Event, stream_stop: asyncio.Event) -> None:
        await global_stop.wait()
        stream_stop.set()

    @staticmethod
    async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=seconds)
