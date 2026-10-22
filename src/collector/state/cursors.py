from __future__ import annotations

from collections.abc import Iterable

from ..models import Stream
from . import scripts


class CursorStore:
    """Last *closed and persisted* candle per stream.

    Separate keys per exchange and symbol are mandatory: Binance and Bybit
    deliver the closed candle at different moments, so a shared cursor would
    either lag behind or invent gaps.
    """

    def __init__(self, redis) -> None:
        self._redis = redis
        self._flush = redis.register_script(scripts.CURSOR_BATCH)

    @staticmethod
    def key_for(stream: Stream) -> str:
        return f"cursor:{stream.lease_name}"

    @staticmethod
    def fence_key_for(stream: Stream) -> str:
        return f"cursor_fence:{stream.lease_name}"

    def get(self, stream: Stream) -> int:
        raw = self._redis.get(self.key_for(stream))
        return int(raw) if raw else 0

    def get_many(self, streams: Iterable[Stream]) -> dict[Stream, int]:
        streams = list(streams)
        if not streams:
            return {}
        values = self._redis.mget([self.key_for(s) for s in streams])
        return {stream: int(value) if value else 0 for stream, value in zip(streams, values, strict=True)}

    def flush(self, values: dict[Stream, tuple[int, int]]) -> int:
        """``values`` maps stream -> (last_closed_open_time, holder_fence)."""
        if not values:
            return 0
        keys: list[str] = []
        args: list[int] = []
        for stream, (value, fence) in values.items():
            keys.extend([self.key_for(stream), self.fence_key_for(stream)])
            args.extend([int(value), int(fence)])
        return int(self._flush(keys=keys, args=args))
