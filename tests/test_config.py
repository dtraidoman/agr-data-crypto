from __future__ import annotations

import pytest

from collector.config import Role, Settings


def test_role_peer_and_exchange_mapping() -> None:
    assert Role.ALPHA.peer is Role.BRAVO
    assert Role.BRAVO.peer is Role.ALPHA
    assert Settings(role=Role.ALPHA).primary_exchange == "binance"
    assert Settings(role=Role.ALPHA).secondary_exchange == "bybit"
    assert Settings(role=Role.BRAVO).primary_exchange == "bybit"


def test_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COLLECTOR_ROLE", "bravo")
    monkeypatch.setenv("COLLECTOR_SYMBOLS", "btcusdt, ethusdt")
    monkeypatch.setenv("COLLECTOR_ENABLE_LIQUIDATIONS", "false")
    monkeypatch.setenv("COLLECTOR_SOFT_DEADLINE_SEC", "100")
    monkeypatch.setenv("COLLECTOR_SUPABASE_URL", "https://example.supabase.co/")
    monkeypatch.setenv("COLLECTOR_REDIS_URL", "redis://localhost:6379/0")

    settings = Settings.from_env()

    assert settings.role is Role.BRAVO
    assert settings.symbols == ("BTCUSDT", "ETHUSDT")
    assert settings.enable_liquidations is False
    assert settings.soft_deadline_sec == 100
    assert settings.supabase_url == "https://example.supabase.co"
    assert settings.primary_exchange == "bybit"


def test_default_soft_deadline_overlaps_successor() -> None:
    assert Settings().soft_deadline_sec > 4 * 3600


def test_validate_reports_missing_configuration() -> None:
    problems = Settings(lease_ttl_ms=1, heartbeat_interval_sec=30).validate()

    assert any("lease TTL" in problem for problem in problems)
    assert any("Redis" in problem for problem in problems)
    assert any("Supabase" in problem for problem in problems)


def test_validate_accepts_complete_configuration() -> None:
    settings = Settings(
        redis_url="redis://localhost",
        supabase_url="https://example.supabase.co",
        supabase_key="key",
    )

    assert settings.validate() == []


def test_invalid_role_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COLLECTOR_ROLE", "charlie")

    with pytest.raises(ValueError):
        Settings.from_env()


def test_from_env_cvd_rest_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COLLECTOR_CVD_REST_EXCHANGES", "BINANCE, bybit")
    monkeypatch.setenv("COLLECTOR_CVD_POLL_SEC", "20")
    monkeypatch.setenv("COLLECTOR_CVD_SEED_LOOKBACK_MIN", "60")

    settings = Settings.from_env()

    assert settings.cvd_rest_exchanges == ("binance", "bybit")
    assert settings.cvd_poll_sec == 20
    assert settings.cvd_seed_lookback_min == 60
