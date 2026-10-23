from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

log = logging.getLogger(__name__)


class SupabaseSink:
    def __init__(
        self,
        url: str,
        key: str,
        client: httpx.AsyncClient,
        *,
        max_retries: int = 5,
    ) -> None:
        self._url = url.rstrip("/")
        self._key = key
        self._client = client
        self._max_retries = max_retries

    def _headers(self, merge: bool) -> dict[str, str]:
        resolution = "merge-duplicates" if merge else "ignore-duplicates"
        return {
            "apikey": self._key,
            "Authorization": f"Bearer {self._key}",
            "Content-Type": "application/json",
            "Prefer": f"resolution={resolution},return=minimal",
        }

    async def write(self, table: str, rows: list[dict[str, Any]], *, merge: bool) -> None:
        if not rows:
            return
        last: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = await self._client.post(
                    f"{self._url}/rest/v1/{table}",
                    headers=self._headers(merge),
                    json=rows,
                    timeout=60,
                )
                if response.status_code == 429 or response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"supabase transient {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return
            except (httpx.HTTPError, ValueError) as exc:
                last = exc
                if attempt == self._max_retries:
                    break
                await asyncio.sleep(min(30.0, 2 ** (attempt - 1)))
        assert last is not None
        raise last

    async def latest_ts(self, table: str, exchange: str, symbol: str) -> int | None:
        response = await self._client.get(
            f"{self._url}/rest/v1/{table}",
            headers={
                "apikey": self._key,
                "Authorization": f"Bearer {self._key}",
            },
            params={
                "select": "ts",
                "exchange": f"eq.{exchange}",
                "symbol": f"eq.{symbol}",
                "order": "ts.desc",
                "limit": "1",
            },
            timeout=20,
        )
        response.raise_for_status()
        rows = response.json()
        return int(rows[0]["ts"]) if rows else None

    async def rpc(self, name: str, payload: dict[str, Any]) -> Any:
        response = await self._client.post(
            f"{self._url}/rest/v1/rpc/{name}",
            headers={
                "apikey": self._key,
                "Authorization": f"Bearer {self._key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=120,
        )
        response.raise_for_status()
        return response.json() if response.content else None
