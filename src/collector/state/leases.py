from __future__ import annotations

from dataclasses import dataclass

from ..models import Stream
from . import scripts


@dataclass(frozen=True, slots=True)
class Lease:
    """Proof of ownership for one stream.

    ``fence`` is monotonically increasing per stream: a holder that was
    replaced can never advance the cursor after the new holder started.
    """

    stream: Stream
    token: str  # exact value stored in Redis: "role:run_id:fence"
    fence: int

    @property
    def key(self) -> str:
        return f"lease:{self.stream.lease_name}"


class LeaseManager:
    def __init__(self, redis, token_prefix: str, ttl_ms: int) -> None:
        self._redis = redis
        self._prefix = token_prefix
        self._ttl_ms = ttl_ms
        self._acquire = redis.register_script(scripts.ACQUIRE)
        self._beat = redis.register_script(scripts.BEAT)
        self._release = redis.register_script(scripts.RELEASE)

    def acquire(self, stream: Stream) -> Lease | None:
        name = stream.lease_name
        result = self._acquire(
            keys=[f"lease:{name}", f"fence:{name}"],
            args=[self._prefix, self._ttl_ms],
        )
        code, fence = int(result[0]), int(result[1])
        if code == 0:
            return None
        return Lease(stream=stream, token=f"{self._prefix}:{fence}", fence=fence)

    def heartbeat(self, leases: list[Lease], hb_key: str, hb_value: str, hb_ttl_ms: int) -> tuple[int, list[str]]:
        keys = [hb_key, *(lease.key for lease in leases)]
        renewed, lost = self._beat(
            keys=keys,
            args=[hb_value, hb_ttl_ms, self._prefix, self._ttl_ms],
        )
        return int(renewed), [str(item) for item in lost]

    def beat_empty(self, hb_key: str, hb_value: str, hb_ttl_ms: int) -> None:
        self._redis.set(hb_key, hb_value, px=hb_ttl_ms)

    def release(self, lease: Lease) -> bool:
        return bool(self._release(keys=[lease.key], args=[lease.token]))
