"""Юнит-тесты для парсинга данных из API."""

from datetime import datetime, timezone

from tinvest_flow.ingestion.rest_loader import _candles_to_records
from tinvest_flow.ingestion.trades_producer import _serialize_trade


class FakeQuotation:
    def __init__(self, units: int, nano: int) -> None:
        self.units = units
        self.nano = nano


class FakeCandle:
    def __init__(self) -> None:
        self.time = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
        self.open = FakeQuotation(100, 500_000_000)
        self.high = FakeQuotation(101, 0)
        self.low = FakeQuotation(99, 0)
        self.close = FakeQuotation(100, 250_000_000)
        self.volume = 1000
        self.is_complete = True


class FakeTrade:
    def __init__(self) -> None:
        self.trade_id = "trade-1"
        self.order_id = "order-1"
        self.account_id = "acc-1"
        self.instrument_uid = "uid-1"
        self.figi = "BBG004730N88"
        self.direction = "BUY"
        self.price = FakeQuotation(200, 750_000_000)
        self.quantity = 5
        self.date_time = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)


def test_candles_to_records() -> None:
    """Проверяет корректную сериализацию protobuf-свечи."""
    records = _candles_to_records([FakeCandle()], figi="BBG004730N88", interval_name="5min")

    assert len(records) == 1
    record = records[0]
    assert record["figi"] == "BBG004730N88"
    assert record["interval"] == "5min"
    assert record["open"] == 100.5
    assert record["close"] == 100.25
    assert record["volume"] == 1000


def test_serialize_trade() -> None:
    """Проверяет корректную сериализацию protobuf-сделки в JSON."""
    payload = _serialize_trade(FakeTrade())

    assert payload["trade_id"] == "trade-1"
    assert payload["direction"] == "BUY"
    assert payload["price_units"] == 200
    assert payload["price_nano"] == 750_000_000
    assert payload["quantity"] == 5
