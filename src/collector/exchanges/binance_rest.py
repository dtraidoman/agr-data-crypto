from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from ..models import Candle, Stream
from ..timeutil import interval_ms, now_ms
from .parsers import parse_binance_rest_klines
from .ratelimit import AsyncRateLimiter

log = logging.getLogger(__name__)


class BinanceRest:
    name = "binance"
    BASE = "https://api.binance.com"

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

    async def _get(self, path: str, params: dict[str, Any]) -> Any:
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

            if response.status_code in (418, 429):
                retry_after = _retry_after(response, attempt)
                log.warning("binance rate limited (%s), sleeping %.1fs", response.status_code, retry_after)
                await asyncio.sleep(retry_after)
                continue
            if response.status_code >= 500:
                last_error = httpx.HTTPStatusError("server error", request=response.request, response=response)
                await asyncio.sleep(min(30.0, 2.0 ** attempt))
                continue

            response.raise_for_status()
            return response.json()

        raise RuntimeError(f"binance request failed after {self._max_retries} attempts: {last_error}")

    async def backfill_klines(self, stream: Stream, start_ms: int, end_ms: int) -> list[Candle]:
        iv = interval_ms(stream.interval)
        cursor = start_ms
        collected: list[Candle] = []
        for _page in range(self._max_pages):
            payload = await self._get(
                "/api/v3/klines",
                {
                    "symbol": stream.symbol,
                    "interval": stream.interval,
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1000,
                },
            )
            ts = now_ms()
            candles = parse_binance_rest_klines(payload, stream.symbol, stream.interval, ts)
            candles = [c for c in candles if start_ms <= c.open_time <= end_ms]
            if not candles:
                break
            collected.extend(candles)
            last_open = candles[-1].open_time
            if last_open >= end_ms or len(payload) < 1000:
                break
            cursor = last_open + iv
        return collected


def _retry_after(response: httpx.Response, attempt: int) -> float:
    raw = response.headers.get("Retry-After")
    try:
        value = float(raw) if raw is not None else 2.0 ** attempt
    except ValueError:
        value = 2.0 ** attempt
    return max(0.5, min(60.0, value))
