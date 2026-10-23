from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "verify_continuity.py"
SPEC = importlib.util.spec_from_file_location("verify_continuity", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
verify = importlib.util.module_from_spec(SPEC)
sys.modules["verify_continuity"] = verify
SPEC.loader.exec_module(verify)

DAY_START = 1_792_540_800_000


def test_history_evaluate_requires_exact_grid() -> None:
    end = DAY_START + 86_400_000
    rows = [DAY_START + index * 300_000 for index in range(288)]
    check = verify.evaluate(
        "deriv_open_interest",
        "binance",
        "BTCUSDT",
        None,
        rows,
        mode="history",
        now=end,
        grid_start=DAY_START,
        grid_last=end - 300_000,
    )
    assert check.ok
    assert check.rows == 288

    check = verify.evaluate(
        "deriv_open_interest",
        "binance",
        "BTCUSDT",
        None,
        rows[:-1],
        mode="history",
        now=end,
        grid_start=DAY_START,
        grid_last=end - 300_000,
    )
    assert not check.ok


class FakeResponse:
    def __init__(self, timestamps: list[int]) -> None:
        self._timestamps = timestamps

    def raise_for_status(self) -> None:
        return None

    def json(self) -> list[dict[str, int]]:
        return [{"ts": ts} for ts in self._timestamps]


class FakeClient:
    def __init__(self, timestamps: list[int], page_size: int) -> None:
        self._timestamps = sorted(timestamps)
        self._page_size = page_size
        self.requests: list[dict[str, list[str]]] = []

    async def get(self, url: str, headers: dict[str, str], params: list[tuple[str, str]], timeout: int) -> FakeResponse:
        collected: dict[str, list[str]] = {}
        for key, value in params:
            collected.setdefault(key, []).append(value)
        self.requests.append(collected)
        rows = self._timestamps
        for value in collected["ts"]:
            if value.startswith("gte."):
                bound = int(value.split(".", 1)[1])
                rows = [ts for ts in rows if ts >= bound]
            elif value.startswith("gt."):
                bound = int(value.split(".", 1)[1])
                rows = [ts for ts in rows if ts > bound]
            elif value.startswith("lt."):
                bound = int(value.split(".", 1)[1])
                rows = [ts for ts in rows if ts < bound]
        return FakeResponse(rows[: self._page_size])


async def test_fetch_window_paginates_and_excludes_end(monkeypatch: Any) -> None:
    timestamps = [DAY_START + index * 60_000 for index in range(10)]
    client = FakeClient(timestamps, page_size=3)
    monkeypatch.setattr(verify, "PAGE_SIZE", 3)
    settings = SimpleNamespace(supabase_url="https://example.supabase.co", supabase_key="key")

    rows = await verify.fetch_window(
        client,  # type: ignore[arg-type]
        settings,  # type: ignore[arg-type]
        "deriv_cvd_1m",
        "binance",
        "BTCUSDT",
        DAY_START,
        DAY_START + 7 * 60_000,
    )

    assert rows == timestamps[:7]
    assert len(client.requests) > 1
    assert all(request["order"] == ["ts.asc"] for request in client.requests)
