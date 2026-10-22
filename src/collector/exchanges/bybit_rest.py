from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from ..models import Candle, Stream
from ..timeutil import now_ms
from .parsers import bybit_interval_code, parse_bybit_rest_klines
from .ratelimit import AsyncRateLimiter

log = logging.getLogger(__name__)

# Bybit: 403 "access too frequent" is followed by a 10 minute IP ban.
# Retrying immediately turns a short hiccup into a 10 minute outage.
RATE_LIMITED_CODES = {10006, 10018}


class BybitRest:
    name = "bybit"
    BASE = "https://api.bybit.com"

    def __init__(
        self,
        client: httpx.AsyncClient,
        limiter: AsyncRateLimiter,
        *,
        max_pages: int = 10,
        max_retries: int = 3,
    ) -> None:
        self._client = client
        self._limiter = limiter
        self._max_pages = max_pages
        self._max_retries = max_retries

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.BASE}{path}"
        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            await self._limiter.wait()
            try:
                response = await self._client.get(url, params=params)
            except httpx.HTTPError as exc:
                last_error = exc
                await asyncio.sleep(min(30.0, 2.0 ** attempt))
                continue

            if response.status_code in (403, 429):
                delay = _retry_after(response, attempt)
                log.warning("bybit throttled (%s), sleeping %.1fs", response.status_code, delay)
                await asyncio.sleep(delay)
                continue
            if response.status_code >= 500:
                last_error = httpx.HTTPStatusError("server error", request=response.request, response=response)
                await asyncio.sleep(min(30.0, 2.0 ** attempt))
                continue

            response.raise_for_status()
            payload = response.json()
            if int(payload.get("retCode", 0)) != 0:
                code = int(payload.get("retCode", -1))
                last_error = RuntimeError(f"bybit retCode={code}: {payload.get('retMsg')}")
                if code in RATE_LIMITED_CODES:
                    await asyncio.sleep(min(60.0, 2.0 ** attempt))
                    continue
                raise last_error
            return payload

        raise RuntimeError(f"bybit request failed after {self._max_retries} attempts: {last_error}")

    async def backfill_klines(self, stream: Stream, start_ms: int, end_ms: int) -> list[Candle]:
        """Walk a window backwards.

        Bybit ignores ``start`` as a paging cursor: whatever the window is, it
        returns the *newest* ``limit`` candles inside it, descending. Forward
        paging with ``start = last + iv`` would therefore skip everything but
        the tail of each window. Paging backwards via ``end = oldest - 1`` is
        the only way to cover the whole range.
        """
        collected: list[Candle] = []
        end = end_ms
        for _page in range(self._max_pages):
            payload = await self._get(
                "/v5/market/kline",
                {
                    "category": "spot",
                    "symbol": stream.symbol,
                    "interval": bybit_interval_code(stream.interval),
                    "start": start_ms,
                    "end": end,
                    "limit": 1000,
                },
            )
            ts = now_ms()
            candles = parse_bybit_rest_klines(payload, stream.symbol, stream.interval, ts)
            candles = [c for c in candles if start_ms <= c.open_time <= end]
            if not candles:
                break
            collected.extend(candles)
            first_open = candles[0].open_time
            if first_open <= start_ms:
                break
            end = first_open - 1

        deduped = {candle.pk: candle for candle in collected}
        return sorted(deduped.values(), key=lambda candle: candle.open_time)


def _retry_after(response: httpx.Response, attempt: int) -> float:
    raw = response.headers.get("Retry-After")
    try:
        value = float(raw) if raw is not None else 2.0 ** attempt
    except ValueError:
        value = 2.0 ** attempt
    return max(0.5, min(60.0, value))
