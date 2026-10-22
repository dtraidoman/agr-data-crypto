from __future__ import annotations

import asyncio
import time


class AsyncRateLimiter:
    """Serialises calls so that consecutive requests are >= interval apart.

    Bybit answers a burst with 403 "access too frequent" and bans the IP for
    10 minutes, so catch-up at startup must be paced, not parallel.
    """

    def __init__(self, min_interval_sec: float) -> None:
        self._interval = max(0.0, float(min_interval_sec))
        self._next_at = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        if self._interval <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            delay = self._next_at - now
            if delay > 0:
                await asyncio.sleep(delay)
            self._next_at = time.monotonic() + self._interval
