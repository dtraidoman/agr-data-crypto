from __future__ import annotations

import json

from collector.timeutil import last_closed_open_time

OPEN = 1_699_999_980_000
IV = 60_000
CLOSED_NOW = OPEN + 2 * IV + 30_000


def _binance_row(open_time: int, close_time: int, price: str = "100.0") -> list:
    return [
        open_time,
        price,
        "101.0",
        "99.0",
        "100.5",
        "10.0",
        close_time,
        "1005.0",
        42,
        "5.0",
        "500.0",
        "0",
    ]


def test_binance_rest_drops_in_progress_candle():
    from collector.exchanges.parsers import parse_binance_rest_klines

    payload = [
        _binance_row(OPEN, OPEN + IV - 1),
        _binance_row(OPEN + IV, OPEN + 2 * IV - 1, "101.0"),
        # still forming: close_time is in the future relative to CLOSED_NOW
        _binance_row(OPEN + 2 * IV, OPEN + 3 * IV - 1, "102.0"),
    ]
    candles = parse_binance_rest_klines(payload, "BTCUSDT", "1m", CLOSED_NOW)

    assert [c.open_time for c in candles] == [OPEN, OPEN + IV]
    assert candles[0].close == 100.5
    assert candles[0].quote_volume == 1005.0
    assert candles[0].trades == 42
    assert all(c.is_closed for c in candles)


def test_binance_ws_ignores_partial_updates():
    from collector.exchanges.parsers import parse_binance_ws_kline

    def message(closed: bool) -> str:
        return json.dumps(
            {
                "stream": "btcusdt@kline_1m",
                "data": {
                    "e": "kline",
                    "s": "BTCUSDT",
                    "k": {
                        "t": OPEN,
                        "T": OPEN + IV - 1,
                        "i": "1m",
                        "o": "100.0",
                        "h": "101.0",
                        "l": "99.0",
                        "c": "100.5",
                        "v": "10.0",
                        "q": "1005.0",
                        "n": 42,
                        "x": closed,
                    },
                },
            }
        )

    assert parse_binance_ws_kline(message(False), CLOSED_NOW) is None
    candle = parse_binance_ws_kline(message(True), CLOSED_NOW)
    assert candle is not None
    assert candle.symbol == "BTCUSDT"
    assert candle.interval == "1m"
    assert candle.open_time == OPEN


def test_binance_ws_ignores_non_kline_frames():
    from collector.exchanges.parsers import parse_binance_ws_kline

    assert parse_binance_ws_kline(json.dumps({"result": None, "id": 1}), CLOSED_NOW) is None
    assert parse_binance_ws_kline(b"not json", CLOSED_NOW) is None


def test_bybit_rest_returns_ascending_and_drops_open_head():
    from collector.exchanges.parsers import parse_bybit_rest_klines

    # The real endpoint returns arrays of strings, newest first:
    # [start, open, high, low, close, volume, turnover].
    def row(start: int, close: str) -> list[str]:
        return [str(start), "100.0", "101.0", "99.0", close, "10.0", "1005.0"]

    payload = {
        "retCode": 0,
        "result": {
            "symbol": "BTCUSDT",
            "category": "spot",
            "list": [
                row(OPEN + 2 * IV, "102.0"),
                row(OPEN + IV, "101.0"),
                row(OPEN, "100.5"),
            ],
        },
    }

    candles = parse_bybit_rest_klines(payload, "BTCUSDT", "1m", CLOSED_NOW)

    assert [c.open_time for c in candles] == [OPEN, OPEN + IV]
    assert [c.close for c in candles] == [100.5, 101.0]
    assert candles[0].close_time == OPEN + IV - 1
    assert candles[0].quote_volume == 1005.0


def test_bybit_ws_requires_confirm():
    from collector.exchanges.parsers import parse_bybit_ws_kline

    def frame(confirm: bool) -> str:
        return json.dumps(
            {
                "topic": "kline.1.BTCUSDT",
                "data": [
                    {
                        "start": str(OPEN),
                        "end": str(OPEN + IV - 1),
                        "interval": "1",
                        "open": "100.0",
                        "close": "100.5",
                        "high": "101.0",
                        "low": "99.0",
                        "volume": "10.0",
                        "turnover": "1005.0",
                        "confirm": confirm,
                    }
                ],
            }
        )

    assert parse_bybit_ws_kline(frame(False), CLOSED_NOW) is None
    candle = parse_bybit_ws_kline(frame(True), CLOSED_NOW)
    assert candle is not None
    assert candle.exchange == "bybit"
    assert candle.interval == "1m"


def test_bybit_ws_pong_is_ignored():
    from collector.exchanges.parsers import parse_bybit_ws_kline

    assert parse_bybit_ws_kline(json.dumps({"op": "pong"}), CLOSED_NOW) is None
    assert parse_bybit_ws_kline(json.dumps({"success": True, "op": "subscribe"}), CLOSED_NOW) is None


def test_last_closed_open_time():
    assert last_closed_open_time(OPEN + 30_000, IV) == OPEN - IV
    assert last_closed_open_time(OPEN, IV) == OPEN - IV
    assert last_closed_open_time(OPEN + IV, IV) == OPEN
