from collector.exchanges.parsers import (
    parse_binance_agg_trade,
    parse_binance_force_order,
    parse_binance_long_short,
    parse_binance_open_interest,
    parse_bybit_liquidation,
    parse_bybit_long_short,
    parse_bybit_open_interest,
    parse_bybit_trade,
)


def test_binance_open_interest():
    rows = parse_binance_open_interest(
        [
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "123.5",
                "sumOpenInterestValue": "1000000.5",
                "timestamp": 1700000000000,
            }
        ],
        "BTCUSDT",
    )
    assert rows[0].open_interest == 123.5
    assert rows[0].open_interest_value == 1000000.5
    assert rows[0].ts == 1700000000000


def test_binance_long_short_variants():
    payload = [
        {
            "longAccount": "0.6",
            "shortAccount": "0.4",
            "longShortRatio": "1.5",
            "timestamp": 1700000000000,
        }
    ]
    rows = parse_binance_long_short(payload, "BTCUSDT", "global_account")
    assert rows[0].long_value == 0.6
    assert rows[0].ratio == 1.5
    assert rows[0].unit == "fraction"


def test_binance_taker_volume():
    payload = [
        {
            "buyVol": "10",
            "sellVol": "5",
            "buySellRatio": "2",
            "timestamp": 1700000000000,
        }
    ]
    rows = parse_binance_long_short(payload, "BTCUSDT", "taker_volume")
    assert rows[0].unit == "base_volume"
    assert rows[0].long_value == 10
    assert rows[0].short_value == 5


def test_binance_agg_trade_side():
    trade = parse_binance_agg_trade(
        {"e": "aggTrade", "s": "BTCUSDT", "a": 7, "p": "100", "q": "0.5", "T": 1700000000000, "m": True}
    )
    assert trade.side == "sell"
    assert trade.notional == 50


def test_binance_agg_trade_rest_payload_has_no_symbol():
    trade = parse_binance_agg_trade(
        {"a": 8, "p": "100", "q": "1", "T": 1700000000000, "m": False},
        "ETHUSDT",
    )
    assert trade.symbol == "ETHUSDT"
    assert trade.side == "buy"


def test_binance_force_order():
    event = parse_binance_force_order(
        {
            "e": "forceOrder",
            "E": 1700000000000,
            "o": {
                "s": "BTCUSDT",
                "S": "SELL",
                "q": "1",
                "p": "100",
                "ap": "99",
                "z": "1",
                "T": 1700000000000,
            },
        }
    )
    assert event.position_side == "long"
    assert event.notional == 99
    assert event.event_id


def test_bybit_open_interest_and_ratio():
    oi = parse_bybit_open_interest(
        {"list": [{"openInterest": "10", "timestamp": "1700000000000"}]}, "ETHUSDT"
    )
    assert oi[0].open_interest == 10
    ratio = parse_bybit_long_short(
        {"list": [{"buyRatio": "0.7", "sellRatio": "0.3", "timestamp": "1700000000000"}]},
        "ETHUSDT",
    )
    assert ratio[0].long_value == 0.7
    assert ratio[0].ratio == 0.7 / 0.3


def test_bybit_trade_and_liquidation():
    trade = parse_bybit_trade(
        {"i": "abc", "p": "100", "v": "2", "T": 1700000000000, "S": "Buy"}, "SOLUSDT"
    )
    assert trade.side == "buy"
    assert trade.trade_id == "abc"
    rest_trade = parse_bybit_trade(
        {"execId": "rest-1", "price": "101", "size": "3", "time": "1700000000000", "side": "Sell"},
        "SOLUSDT",
    )
    assert rest_trade.trade_id == "rest-1"
    assert rest_trade.side == "sell"
    assert rest_trade.notional == 303
    event = parse_bybit_liquidation(
        {"T": 1700000000000, "s": "SOLUSDT", "S": "Sell", "v": "2", "p": "100"},
        "SOLUSDT",
    )
    assert event.position_side == "long"
    assert event.notional == 200
