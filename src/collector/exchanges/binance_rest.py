from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from ..models import LongShort, OpenInterest, Trade
from ..timeutil import now_ms
from .parsers import (
    parse_binance_agg_trade,
    parse_binance_long_short,
    parse_binance_open_interest,
)
from .ratelimit import AsyncRateLimiter

log = logging.getLogger(__name__)

PERIOD_MS = 300_000
LS_ENDPOINTS = {
    "global_account": "globalLongShortAccountRatio",
    "top_account": "topLongShortAccountRatio",
    "top_position": "topLongShortPositionRatio",
    "taker_volume": "takerlongshortRatio",
}


class BinanceRest:
    name = "binance"
    BASE = "https://fapi.binance.com"

    def __init__(
        self,
        client: httpx.AsyncClient,
        limiter: AsyncRateLimiter,
        *,
        max_retries: int = 4,
        max_pages: int = 30,
    ) -> None:
        self._client = client
        self._limiter = limiter
        self._max_retries = max_retries
        self._max_pages = max_pages
        # aggTrades carries weight 20; 0.6s between pages keeps catch-up
        # under the 2400 weight/minute IP budget.
        self._trade_limiter = AsyncRateLimiter(0.6)

    async def _get(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self.BASE}{path}"
        last: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            await self._limiter.wait()
            try:
                response = await self._client.get(url, params=params)
                if response.status_code == 429 or response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"binance transient {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last = exc
                if attempt == self._max_retries:
                    break
                await asyncio.sleep(min(8.0, 2 ** (attempt - 1)))
        assert last is not None
        raise last

    @staticmethod
    def _closed(end_ms: int) -> int:
        return min(end_ms, now_ms() - PERIOD_MS)

    async def fetch_open_interest(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> list[OpenInterest]:
        out: list[OpenInterest] = []
        end = self._closed(end_ms)
        # Binance returns the newest page within [startTime, endTime], not the
        # oldest. Walk backwards or a five-day request silently stops after the
        # latest 500 points.
        cursor = end
        for _ in range(self._max_pages):
            if cursor < start_ms:
                break
            payload = await self._get(
                "/futures/data/openInterestHist",
                {
                    "symbol": symbol,
                    "period": "5m",
                    "limit": 500,
                    "startTime": start_ms,
                    "endTime": cursor,
                },
            )
            rows = parse_binance_open_interest(payload, symbol)
            rows = [row for row in rows if start_ms <= row.ts <= end]
            out.extend(rows)
            if len(payload) < 500 or not rows:
                break
            oldest = min(row.ts for row in rows)
            if oldest <= start_ms:
                break
            next_cursor = oldest - 1
            if next_cursor >= cursor:
                break
            cursor = next_cursor
        return sorted({row.ts: row for row in out}.values(), key=lambda row: row.ts)

    async def fetch_long_short(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> list[LongShort]:
        out: list[LongShort] = []
        end = self._closed(end_ms)
        for kind, endpoint in LS_ENDPOINTS.items():
            cursor = end
            for _ in range(self._max_pages):
                if cursor < start_ms:
                    break
                payload = await self._get(
                    f"/futures/data/{endpoint}",
                    {
                        "symbol": symbol,
                        "period": "5m",
                        "limit": 500,
                        "startTime": start_ms,
                        "endTime": cursor,
                    },
                )
                rows = parse_binance_long_short(payload, symbol, kind)
                rows = [row for row in rows if start_ms <= row.ts <= end]
                out.extend(rows)
                if len(payload) < 500 or not rows:
                    break
                oldest = min(row.ts for row in rows)
                if oldest <= start_ms:
                    break
                next_cursor = oldest - 1
                if next_cursor >= cursor:
                    break
                cursor = next_cursor
        return sorted({row.key: row for row in out}.values(), key=lambda row: row.key)

    async def fetch_recent_trades(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> list[Trade]:
        out: list[Trade] = []
        end = min(end_ms, now_ms())
        from_id: int | None = None
        for _ in range(self._max_pages):
            await self._trade_limiter.wait()
            params: dict[str, Any] = {"symbol": symbol, "limit": 1000}
            if from_id is None:
                params["startTime"] = start_ms
                params["endTime"] = end
            else:
                params["fromId"] = from_id
            payload = await self._get("/fapi/v1/aggTrades", params)
            if not payload:
                break
            past_end = False
            for item in payload:
                trade = parse_binance_agg_trade(item, symbol)
                if trade.ts > end:
                    past_end = True
                    break
                if start_ms <= trade.ts:
                    out.append(trade)
            last_id = int(payload[-1]["a"])
            if past_end or len(payload) < 1000 or last_id <= (from_id or 0):
                break
            from_id = last_id + 1
        return sorted({trade.trade_id: trade for trade in out}.values(), key=lambda trade: trade.ts)
