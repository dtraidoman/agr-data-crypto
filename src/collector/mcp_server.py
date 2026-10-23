"""MCP (Model Context Protocol) server exposing the AGR-DATA-CRYPTO dataset.

Run with::

    python -m collector.mcp_server

or via the installed console script ``agr-crypto-mcp``.

Configuration (environment variables, checked in this order):
    SUPABASE_URL / COLLECTOR_SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY / COLLECTOR_SUPABASE_KEY

The server is read-only: it queries the Supabase PostgREST API and returns
JSON-serialised rows. No exchange keys are required.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

import httpx
from mcp.server.mcpserver import MCPServer

log = logging.getLogger("agr-crypto-mcp")

TABLES = {
    "oi": "deriv_open_interest",
    "ls": "deriv_long_short",
    "cvd": "deriv_cvd_1m",
    "liq": "deriv_liquidations",
}

EXCHANGES = ("binance", "bybit")
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")


def _env(name: str, alt: str = "") -> str:
    for key in (name, alt):
        if not key:
            continue
        value = os.getenv(key)
        if value:
            return value.strip()
    return ""


def _to_ms(value: Any) -> int | None:
    """Accept ms epoch (int/float/str) or an ISO-8601 timestamp."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("boolean is not a valid timestamp")
    if isinstance(value, (int, float)):
        v = int(value)
        # Heuristic: seconds -> ms
        if v < 10_000_000_000:
            v *= 1000
        return v
    text = str(value).strip()
    try:
        v = int(text)
        if v < 10_000_000_000:
            v *= 1000
        return v
    except ValueError:
        pass
    # ISO-8601
    txt = text.replace("Z", "+00:00")
    dt = datetime.fromisoformat(txt)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def _ms_to_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


class SupabaseReader:
    def __init__(self) -> None:
        self.url = _env("SUPABASE_URL", "COLLECTOR_SUPABASE_URL").rstrip("/")
        self.key = _env("SUPABASE_SERVICE_ROLE_KEY", "COLLECTOR_SUPABASE_KEY")
        if not self.url or not self.key:
            raise RuntimeError(
                "Supabase is not configured. Set SUPABASE_URL and "
                "SUPABASE_SERVICE_ROLE_KEY (or COLLECTOR_SUPABASE_URL / "
                "COLLECTOR_SUPABASE_KEY)."
            )
        self._client = httpx.Client(
            base_url=self.url,
            headers={
                "apikey": self.key,
                "Authorization": f"Bearer {self.key}",
            },
            timeout=30.0,
        )

    def _get(self, table: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        response = self._client.get(f"/rest/v1/{table}", params=params)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []

    def latest_ts(self, table: str, exchange: str, symbol: str | None = None) -> int | None:
        params: dict[str, Any] = {
            "select": "ts",
            "exchange": f"eq.{exchange}",
            "order": "ts.desc",
            "limit": "1",
        }
        if symbol:
            params["symbol"] = f"eq.{symbol}"
        rows = self._get(table, params)
        return int(rows[0]["ts"]) if rows else None

    def query(
        self,
        table: str,
        *,
        exchange: str | None = None,
        symbol: str | None = None,
        kind: str | None = None,
        start_ms: int | None = None,
        end_ms: int | None = None,
        columns: str = "*",
        order: str = "ts.desc",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 1000))
        params: dict[str, Any] = {
            "select": columns,
            "order": order,
            "limit": str(limit),
        }
        if exchange:
            params["exchange"] = f"eq.{exchange}"
        if symbol:
            params["symbol"] = f"eq.{symbol}"
        if kind:
            params["kind"] = f"eq.{kind}"
        filters: list[tuple[str, str]] = []
        if start_ms is not None:
            filters.append(("ts", f"gte.{start_ms}"))
        if end_ms is not None:
            filters.append(("ts", f"lte.{end_ms}"))
        items: list[tuple[str, str]] = [(k, v) for k, v in params.items()]
        for col, val in filters:
            items.append((col, val))
        response = self._client.get(f"/rest/v1/{table}", params=items)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []

    def close(self) -> None:
        self._client.close()


_READER: SupabaseReader | None = None


def reader() -> SupabaseReader:
    global _READER
    if _READER is None:
        _READER = SupabaseReader()
    return _READER


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str, indent=2)


mcp = MCPServer(
    name="agr-data-crypto",
    title="AGR-DATA-CRYPTO derivatives data",
    description=(
        "Read-only access to the AGR-DATA-CRYPTO Supabase dataset: open interest, "
        "long/short ratios, 1-minute CVD and liquidations for Binance/Bybit "
        "USDT perpetual futures (BTCUSDT, ETHUSDT, SOLUSDT)."
    ),
    instructions=(
        "Timestamps are milliseconds since epoch (UTC). Use the provided tools to "
        "fetch rows; results are JSON arrays. Keep `limit` reasonable (<=1000)."
    ),
)


@mcp.tool()
def health() -> str:
    """Report server configuration and latest available timestamp per table."""
    try:
        r = reader()
    except RuntimeError as exc:
        return _json({"configured": False, "error": str(exc)})
    out: dict[str, Any] = {
        "configured": True,
        "supabase_url": r.url,
        "exchanges": list(EXCHANGES),
        "symbols": list(SYMBOLS),
        "tables": {k: v for k, v in TABLES.items()},
        "latest": {},
    }
    for key, table in TABLES.items():
        out["latest"][key] = {}
        for ex in EXCHANGES:
            for sym in SYMBOLS:
                ts = r.latest_ts(table, ex, sym)
                out["latest"][key].setdefault(ex, {})[sym] = {
                    "ts_ms": ts,
                    "ts_iso": _ms_to_iso(ts),
                }
    return _json(out)


@mcp.tool()
def get_open_interest(
    exchange: str,
    symbol: str = "BTCUSDT",
    start: Any = None,
    end: Any = None,
    limit: int = 200,
) -> str:
    """Fetch open-interest rows for an exchange/symbol.

    exchange: binance | bybit
    symbol: BTCUSDT | ETHUSDT | SOLUSDT
    start/end: ms epoch or ISO-8601 (optional, inclusive)
    """
    rows = reader().query(
        TABLES["oi"],
        exchange=exchange,
        symbol=symbol,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        order="ts.asc",
        limit=limit,
    )
    return _json({"table": TABLES["oi"], "count": len(rows), "rows": rows})


@mcp.tool()
def get_long_short(
    exchange: str,
    symbol: str = "BTCUSDT",
    kind: str | None = None,
    start: Any = None,
    end: Any = None,
    limit: int = 200,
) -> str:
    """Fetch long/short ratio rows.

    kind (optional): binance: global_account, top_account, top_position,
    taker_volume; bybit: account.
    """
    rows = reader().query(
        TABLES["ls"],
        exchange=exchange,
        symbol=symbol,
        kind=kind,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        order="ts.asc",
        limit=limit,
    )
    return _json({"table": TABLES["ls"], "count": len(rows), "rows": rows})


@mcp.tool()
def get_cvd(
    exchange: str,
    symbol: str = "BTCUSDT",
    start: Any = None,
    end: Any = None,
    limit: int = 500,
) -> str:
    """Fetch 1-minute CVD buckets (buy/sell volume, notional, trades, delta)."""
    rows = reader().query(
        TABLES["cvd"],
        exchange=exchange,
        symbol=symbol,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        order="ts.asc",
        limit=limit,
    )
    return _json({"table": TABLES["cvd"], "count": len(rows), "rows": rows})


@mcp.tool()
def get_liquidations(
    exchange: str,
    symbol: str | None = None,
    start: Any = None,
    end: Any = None,
    limit: int = 200,
) -> str:
    """Fetch liquidation events. symbol is optional (all symbols if omitted)."""
    rows = reader().query(
        TABLES["liq"],
        exchange=exchange,
        symbol=symbol,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        order="ts.desc",
        limit=limit,
    )
    return _json({"table": TABLES["liq"], "count": len(rows), "rows": rows})


@mcp.tool()
def get_cvd_cumulative(
    exchange: str,
    symbol: str = "BTCUSDT",
    start: Any = None,
    end: Any = None,
    limit: int = 100,
) -> str:
    """Fetch running CVD (cumulative delta) rows from deriv_cvd_cumulative view."""
    rows = reader().query(
        "deriv_cvd_cumulative",
        exchange=exchange,
        symbol=symbol,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        columns="*",
        order="ts.asc",
        limit=limit,
    )
    return _json({"view": "deriv_cvd_cumulative", "count": len(rows), "rows": rows})


@mcp.tool()
def get_cvd_5m(
    exchange: str,
    symbol: str = "BTCUSDT",
    start: Any = None,
    end: Any = None,
    limit: int = 200,
) -> str:
    """Fetch 5-minute aggregated CVD from deriv_cvd_5m view."""
    rows = reader().query(
        "deriv_cvd_5m",
        exchange=exchange,
        symbol=symbol,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        columns="*",
        order="ts.asc",
        limit=limit,
    )
    return _json({"view": "deriv_cvd_5m", "count": len(rows), "rows": rows})


@mcp.tool()
def get_liquidation_1m(
    exchange: str,
    symbol: str = "BTCUSDT",
    start: Any = None,
    end: Any = None,
    limit: int = 200,
) -> str:
    """Fetch 1-minute aggregated liquidations from deriv_liquidation_1m view."""
    rows = reader().query(
        "deriv_liquidation_1m",
        exchange=exchange,
        symbol=symbol,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        columns="*",
        order="ts.asc",
        limit=limit,
    )
    return _json({"view": "deriv_liquidation_1m", "count": len(rows), "rows": rows})


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    mcp.run()


if __name__ == "__main__":
    main()
