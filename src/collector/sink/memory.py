from __future__ import annotations

from collections import defaultdict
from typing import Any


class MemorySink:
    def __init__(self) -> None:
        self.tables: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = defaultdict(dict)
        self.merge_flags: list[tuple[str, bool]] = []

    async def write(self, table: str, rows: list[dict[str, Any]], *, merge: bool) -> None:
        self.merge_flags.append((table, merge))
        for row in rows:
            key = self._key(table, row)
            if merge or key not in self.tables[table]:
                self.tables[table][key] = row

    @staticmethod
    def _key(table: str, row: dict[str, Any]) -> tuple[Any, ...]:
        if table == "deriv_liquidations":
            return row["exchange"], row["event_id"]
        if table == "deriv_long_short":
            return row["exchange"], row["symbol"], row["kind"], row["ts"]
        return row["exchange"], row["symbol"], row["ts"]

    def rows(self, table: str) -> list[dict[str, Any]]:
        return list(self.tables.get(table, {}).values())

    async def latest_ts(self, table: str, exchange: str, symbol: str) -> int | None:
        values = [
            row["ts"]
            for row in self.rows(table)
            if row.get("exchange") == exchange and row.get("symbol") == symbol
        ]
        return max(values) if values else None

    async def rpc(self, name: str, payload: dict[str, Any]) -> Any:
        return {"name": name, "payload": payload}
