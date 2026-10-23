from __future__ import annotations

from datetime import date

from collector.exchanges.archive import (
    binance_archive_url,
    bybit_archive_url,
    iter_binance_csv,
    iter_bybit_csv,
    parse_binance_archive_row,
    parse_bybit_archive_row,
)


def test_archive_urls() -> None:
    day = date(2026, 10, 20)
    assert binance_archive_url("BTCUSDT", day).endswith(
        "/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-20.zip"
    )
    assert bybit_archive_url("ETHUSDT", day).endswith("/trading/ETHUSDT/ETHUSDT2026-10-20.csv.gz")


def test_binance_archive_row_side_and_header() -> None:
    assert parse_binance_archive_row(["agg_trade_id", "price", "quantity"]) is None
    trade = parse_binance_archive_row(
        ["123", "100.5", "2.5", "1", "2", "1700000000123", "true"]
    )
    assert trade is not None
    assert trade.exchange == "binance"
    assert trade.trade_id == "123"
    assert trade.ts == 1_700_000_000_123
    assert trade.side == "sell"
    assert trade.notional == 251.25

    buyer = parse_binance_archive_row(
        ["124", "100", "1", "1", "1", "1700000000124", "false"]
    )
    assert buyer is not None
    assert buyer.side == "buy"


def test_bybit_archive_row_seconds_and_header() -> None:
    assert parse_bybit_archive_row(["timestamp", "symbol", "side"]) is None
    trade = parse_bybit_archive_row(
        ["1700000000.123", "BTCUSDT", "Buy", "0.5", "100", "PlusTick", "match-1"]
    )
    assert trade is not None
    assert trade.exchange == "bybit"
    assert trade.trade_id == "match-1"
    assert trade.ts == 1_700_000_000_123
    assert trade.side == "buy"

    sell = parse_bybit_archive_row(
        ["1700000001", "BTCUSDT", "Sell", "1", "100", "MinusTick", "match-2"]
    )
    assert sell is not None
    assert sell.side == "sell"


def test_iterators_assign_symbol_and_skip_headers() -> None:
    binance = list(
        iter_binance_csv(
            [
                "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n",
                "1,100,2,1,1,1700000000000,false\n",
            ],
            "SOLUSDT",
        )
    )
    assert len(binance) == 1
    assert binance[0].symbol == "SOLUSDT"

    bybit = list(
        iter_bybit_csv(
            [
                "timestamp,symbol,side,size,price,tickDirection,tradeId\n",
                "1700000000.5,BTCUSDT,Buy,1,100,PlusTick,abc\n",
            ],
            "BTCUSDT",
        )
    )
    assert len(bybit) == 1
    assert bybit[0].trade_id == "abc"
    assert bybit[0].symbol == "BTCUSDT"
