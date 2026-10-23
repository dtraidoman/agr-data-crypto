from __future__ import annotations

import logging
import time
from collections.abc import Iterable

from redis.asyncio import Redis
from redis.exceptions import RedisError

from ..models import Stream

log = logging.getLogger(__name__)

ACQUIRE = """
local current = redis.call('GET', KEYS[1])
if current and string.sub(current, 1, string.len(ARGV[1])) ~= ARGV[1] then
  return 0
end
local fence = redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[1], ARGV[1] .. fence, 'PX', ARGV[2])
return fence
"""

RELEASE = """
local current = redis.call('GET', KEYS[1])
if current and string.sub(current, 1, string.len(ARGV[1])) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

BEAT = """
local lost = {}
for i = 2, #KEYS do
  local current = redis.call('GET', KEYS[i])
  if not current or string.sub(current, 1, string.len(ARGV[3])) ~= ARGV[3] then
    table.insert(lost, KEYS[i])
  else
    redis.call('PEXPIRE', KEYS[i], ARGV[4])
  end
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return lost
"""


class StateManager:
    available = True

    def __init__(
        self,
        redis: Redis,
        *,
        role: str,
        run_id: str,
        lease_ttl_ms: int = 90_000,
        heartbeat_ttl_ms: int = 90_000,
    ) -> None:
        self._redis = redis
        self._role = role
        self._run_id = run_id
        self._lease_ttl = lease_ttl_ms
        self._hb_ttl = heartbeat_ttl_ms
        self._peer_cache: dict[str, tuple[float, bool]] = {}
        self._acquire_script = redis.register_script(ACQUIRE)
        self._release_script = redis.register_script(RELEASE)
        self._beat_script = redis.register_script(BEAT)
        self.available = True

    def _degrade(self, operation: str) -> None:
        # A Redis outage mid-run must not stop the primary WebSockets. Keep
        # those streams running without a lease and shut down secondaries;
        # the next process retries Redis from scratch.
        if self.available:
            self.available = False
            log.error(
                "redis %s failed; leases disabled, primary streams continue unleased",
                operation,
            )

    def _lease_key(self, stream: Stream) -> str:
        return f"lease:{stream.lease_name}"

    def _fence_key(self, stream: Stream) -> str:
        return f"fence:{stream.lease_name}"

    def _prefix(self, stream: Stream) -> str:
        return f"{self._run_id}:{stream.lease_name}:"

    async def acquire(self, stream: Stream) -> bool:
        try:
            fence = await self._acquire_script(
                keys=[self._lease_key(stream), self._fence_key(stream)],
                args=[self._prefix(stream), self._lease_ttl],
            )
            return int(fence) > 0
        except RedisError:
            log.warning("redis acquire failed for %s", stream.lease_name, exc_info=True)
            self._degrade("acquire")
            return False

    async def release(self, stream: Stream) -> None:
        try:
            await self._release_script(keys=[self._lease_key(stream)], args=[self._prefix(stream)])
        except RedisError:
            log.warning("redis release failed for %s", stream.lease_name, exc_info=True)
            self._degrade("release")

    async def heartbeat(self, streams: Iterable[Stream]) -> list[str]:
        held = list(streams)
        if not held:
            try:
                await self._redis.set(f"hb:{self._role}", self._run_id, px=self._hb_ttl)
            except RedisError:
                log.warning("redis heartbeat failed", exc_info=True)
                self._degrade("heartbeat")
            return []
        keys = [f"hb:{self._role}"] + [self._lease_key(stream) for stream in held]
        try:
            lost = await self._beat_script(
                keys=keys,
                args=[self._run_id, self._hb_ttl, f"{self._run_id}:", self._lease_ttl],
            )
            lost_set = {str(item) for item in (lost or [])}
            return [stream.lease_name for stream in held if self._lease_key(stream) in lost_set]
        except RedisError:
            log.warning("redis heartbeat failed", exc_info=True)
            self._degrade("heartbeat")
            return []

    async def peer_alive(self, peer: str) -> bool:
        # Both secondary streams poll the same peer. Cache positive results
        # briefly so normal operation uses one Redis command per peer per poll
        # interval instead of one per stream. Negative results are not cached,
        # so a missing peer is noticed immediately.
        now = time.monotonic()
        cached = self._peer_cache.get(peer)
        if cached is not None and now - cached[0] < 45:
            return cached[1]
        try:
            alive = bool(await self._redis.exists(f"hb:{peer}"))
        except RedisError:
            log.warning("redis peer check failed", exc_info=True)
            self._degrade("peer check")
            return False
        if alive:
            self._peer_cache[peer] = (now, alive)
        else:
            self._peer_cache.pop(peer, None)
        return alive

    async def close(self) -> None:
        await self._redis.aclose()


class NullState:
    available = False

    async def acquire(self, stream: Stream) -> bool:
        return True

    async def release(self, stream: Stream) -> None:
        return None

    async def heartbeat(self, streams: Iterable[Stream]) -> list[str]:
        return []

    async def peer_alive(self, peer: str) -> bool:
        return False

    async def close(self) -> None:
        return None
