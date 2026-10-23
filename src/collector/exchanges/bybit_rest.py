from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from ..models import LongShort, OpenInterest, Trade
from ..timeutil import now_ms
from .parsers import (
    parse_bybit_long_short,
    parse_bybit_open_interest,
    parse_bybit_trade,
)
from .ratelimit import AsyncRateLimiter

log = logging.getLogger(__name__)

PERIOD_MS = 300_000
RATE_LIMIT_CODES = {10006, 10018}


class BybitRest:
    name = "bybit"
    BASE = "https://api.bybit.com"

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

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.BASE}{path}"
        last: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            await self._limiter.wait()
            try:
                response = await self._client.get(url, params=params)
                if response.status_code == 429 or response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"bybit transient {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                body = response.json()
                code = int(body.get("retCode", 0))
                if code in RATE_LIMIT_CODES:
                    raise httpx.HTTPStatusError(
                        f"bybit rate limit {code}",
                        request=response.request,
                        response=response,
                    )
                if code != 0:
                    raise ValueError(f"bybit retCode={code}: {body.get('retMsg')}")
                return body.get("result", {})
            except (httpx.HTTPError, ValueError) as exc:
                last = exc
                if attempt == self._max_retries:
                    break
                await asyncio.sleep(min(15.0, 3 ** (attempt - 1)))
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
        cursor = end
        for _ in range(self._max_pages):
            result = await self._get(
                "/v5/market/open-interest",
                {
                    "category": "linear",
                    "symbol": symbol,
                    "intervalTime": "5min",
                    "startTime": start_ms,
                    "endTime": cursor,
                    "limit": 200,
                },
            )
            rows = parse_bybit_open_interest(result, symbol)
            rows = [row for row in rows if start_ms <= row.ts <= end]
            out.extend(rows)
            all_rows = parse_bybit_open_interest(result, symbol)
            if len(all_rows) < 200 or not all_rows:
                break
            oldest = min(row.ts for row in all_rows)
            if oldest <= start_ms:
                break
            cursor = oldest - 1
        return sorted({row.ts: row for row in out}.values(), key=lambda row: row.ts)

    async def fetch_long_short(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> list[LongShort]:
        out: list[LongShort] = []
        end = self._closed(end_ms)
        cursor = end
        for _ in range(self._max_pages):
            result = await self._get(
                "/v5/market/account-ratio",
                {
                    "category": "linear",
                    "symbol": symbol,
                    "period": "5min",
                    "startTime": start_ms,
                    "endTime": cursor,
                    "limit": 500,
                },
            )
            rows = parse_bybit_long_short(result, symbol)
            rows = [row for row in rows if start_ms <= row.ts <= end]
            out.extend(rows)
            all_rows = parse_bybit_long_short(result, symbol)
            if len(all_rows) < 500 or not all_rows:
                break
            oldest = min(row.ts for row in all_rows)
            if oldest <= start_ms:
                break
            cursor = oldest - 1
        return sorted({row.key: row for row in out}.values(), key=lambda row: row.key)

    async def fetch_recent_trades(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> list[Trade]:
        result = await self._get(
            "/v5/market/recent-trade",
            {"category": "linear", "symbol": symbol, "limit": 1000},
        )
        out: list[Trade] = []
        for item in result.get("list", []):
            trade = parse_bybit_trade(item, symbol)
            if start_ms <= trade.ts <= min(end_ms, now_ms()):
                out.append(trade)
        return sorted({trade.trade_id: trade for trade in out}.values(), key=lambda trade: trade.ts)
