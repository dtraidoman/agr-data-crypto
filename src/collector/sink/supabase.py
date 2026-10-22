from __future__ import annotations

import asyncio
import logging

import httpx

from .base import Sink

log = logging.getLogger(__name__)

CHUNK = 500


class SupabaseSink(Sink):
    """Batched PostgREST upsert with ON CONFLICT DO NOTHING.

    ``Prefer: resolution=ignore-duplicates`` is PostgREST's way of saying
    "ON CONFLICT DO NOTHING": the row is inserted once and silently skipped
    afterwards. That is the only deduplication guarantee in the system that
    actually holds under concurrency.
    """

    def __init__(
        self,
        url: str,
        key: str,
        table: str = "ohlcv",
        *,
        client: httpx.AsyncClient | None = None,
        max_retries: int = 3,
        timeout: float = 20.0,
    ) -> None:
        self._url = f"{url.rstrip('/')}/rest/v1/{table}"
        self._headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=ignore-duplicates,return=minimal",
        }
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._owns_client = client is None
        self._max_retries = max_retries

    async def write(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        accepted = 0
        for start in range(0, len(rows), CHUNK):
            chunk = rows[start : start + CHUNK]
            await self._post(chunk)
            accepted += len(chunk)
        return accepted

    async def _post(self, chunk: list[dict]) -> None:
        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = await self._client.post(self._url, headers=self._headers, json=chunk)
            except httpx.HTTPError as exc:
                last_error = exc
                await asyncio.sleep(min(10.0, 2.0 ** attempt))
                continue

            if response.status_code in (200, 201, 204):
                return
            if response.status_code in (429, 500, 502, 503, 504):
                delay = min(10.0, 2.0 ** attempt)
                log.warning("supabase %s, retrying in %.1fs", response.status_code, delay)
                last_error = httpx.HTTPStatusError(
                    f"status {response.status_code}: {response.text[:200]}",
                    request=response.request,
                    response=response,
                )
                await asyncio.sleep(delay)
                continue

            # 400/409/422 mean a schema or payload problem: retrying will not help.
            raise RuntimeError(f"supabase rejected batch ({response.status_code}): {response.text[:300]}")

        raise RuntimeError(f"supabase write failed after {self._max_retries} attempts: {last_error}")

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()
