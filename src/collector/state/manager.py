from __future__ import annotations

import contextlib
import logging
import socket
import time
from dataclasses import dataclass, field
from typing import Any

from redis.exceptions import RedisError

from ..config import Role
from ..models import Stream
from .cursors import CursorStore
from .leases import Lease, LeaseManager

log = logging.getLogger(__name__)

HB_PREFIX = "hb"
RUN_PREFIX = "run"


@dataclass(slots=True)
class HeartbeatResult:
    ok: bool = True
    renewed: int = 0
    lost: list[str] = field(default_factory=list)


class StateManager:
    """Coordination facade.

    Degradation policy (deliberate, see README): losing Redis must not stop
    data collection, because writes are idempotent. It only stops cursor
    advancement, which is what actually protects against data loss.
    """

    def __init__(
        self,
        redis,
        role: Role,
        run_id: str,
        *,
        primary_exchange: str,
        lease_ttl_ms: int = 90_000,
        heartbeat_ttl_ms: int = 300_000,
    ) -> None:
        self._redis = redis
        self.role = role
        self.run_id = run_id
        self.primary_exchange = primary_exchange
        self._prefix = f"{role}:{run_id}"
        self._hb_ttl_ms = heartbeat_ttl_ms
        self._degraded = redis is None
        self._lease_ttl_ms = lease_ttl_ms
        self._leases = LeaseManager(redis, self._prefix, lease_ttl_ms) if redis else None
        self._cursors = CursorStore(redis) if redis else None

    # ------------------------------------------------------------------ state
    @property
    def degraded(self) -> bool:
        return self._degraded

    @property
    def hb_key(self) -> str:
        return f"{HB_PREFIX}:{self.role}"

    @property
    def hb_value(self) -> str:
        return f"{self.run_id}"

    def _enter_degraded(self, exc: BaseException) -> None:
        if not self._degraded:
            log.error("state store unavailable, entering degraded mode: %s", exc)
        self._degraded = True

    def start(self) -> None:
        if not self._redis:
            return
        try:
            self._redis.hset(
                f"{RUN_PREFIX}:{self.role}",
                mapping={
                    "run_id": self.run_id,
                    "started_at": str(time.time()),
                    "host": socket.gethostname(),
                },
            )
            self._redis.expire(f"{RUN_PREFIX}:{self.role}", 86_400)
            self._redis.set(self.hb_key, self.hb_value, ex=self._hb_ttl_ms // 1000)
        except RedisError as exc:
            self._enter_degraded(exc)

    # ----------------------------------------------------------------- leases
    def acquire(self, stream: Stream) -> Lease | None:
        if self._degraded:
            # Safety: only the role's own exchange may be collected without a
            # lease. Grabbing the peer's streams blind would double-collect.
            if stream.exchange == self.primary_exchange:
                return Lease(stream=stream, token="degraded", fence=0)
            return None
        assert self._leases is not None
        try:
            return self._leases.acquire(stream)
        except RedisError as exc:
            self._enter_degraded(exc)
            return Lease(stream=stream, token="degraded", fence=0) if stream.exchange == self.primary_exchange else None

    def heartbeat(self, leases: list[Lease]) -> HeartbeatResult:
        if not self._redis:
            return HeartbeatResult(ok=False)
        try:
            if not leases:
                self._leases.beat_empty(self.hb_key, self.hb_value, self._hb_ttl_ms)  # type: ignore[union-attr]
                return HeartbeatResult()
            renewed, lost = self._leases.heartbeat(  # type: ignore[union-attr]
                leases, self.hb_key, self.hb_value, self._hb_ttl_ms
            )
            return HeartbeatResult(renewed=renewed, lost=lost)
        except RedisError as exc:
            self._enter_degraded(exc)
            return HeartbeatResult(ok=False)

    def release(self, lease: Lease) -> bool:
        if not self._redis or lease.token == "degraded":
            return True
        try:
            return self._leases.release(lease)  # type: ignore[union-attr]
        except RedisError as exc:
            self._enter_degraded(exc)
            return False

    def release_all(self, leases: list[Lease]) -> None:
        for lease in leases:
            self.release(lease)

    # ---------------------------------------------------------------- cursors
    def cursor(self, stream: Stream) -> int:
        if self._degraded or not self._cursors:
            return 0
        try:
            return self._cursors.get(stream)
        except RedisError as exc:
            self._enter_degraded(exc)
            return 0

    def cursors(self, streams: list[Stream]) -> dict[Stream, int]:
        if self._degraded or not self._cursors:
            return {stream: 0 for stream in streams}
        try:
            return self._cursors.get_many(streams)
        except RedisError as exc:
            self._enter_degraded(exc)
            return {stream: 0 for stream in streams}

    def flush_cursors(self, values: dict[Stream, tuple[int, int]]) -> int:
        if self._degraded or not self._cursors:
            return 0
        try:
            return self._cursors.flush(values)
        except RedisError as exc:
            self._enter_degraded(exc)
            return 0

    # ------------------------------------------------------------------ peers
    def peer_alive(self, peer: Role) -> bool:
        if not self._redis:
            return False
        try:
            return bool(self._redis.exists(f"{HB_PREFIX}:{peer}"))
        except RedisError as exc:
            self._enter_degraded(exc)
            return False

    def mark_stopped(self) -> None:
        if not self._redis:
            return
        with contextlib.suppress(RedisError):
            self._redis.set(self.hb_key, f"{self.run_id}|stopped", ex=60)

    def once(self, key: str, ttl_ms: int = 3_600_000) -> bool:
        """Deduplicate alerts. True means "this is the first time"."""
        if not self._redis:
            return True
        try:
            return bool(self._redis.set(key, "1", nx=True, px=ttl_ms))
        except RedisError:
            return True

    def get(self, key: str) -> Any:
        if not self._redis:
            return None
        try:
            return self._redis.get(key)
        except RedisError:
            return None
