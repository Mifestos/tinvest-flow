"""Юнит-тесты для слоя Ingestion."""

from datetime import datetime, timezone

from tinvest_flow.ingestion.rest_loader import _quotation_to_float
from tinvest_flow.ingestion.trades_producer import _serialize_trade


class FakeQuotation:
    def __init__(self, units: int, nano: int) -> None:
        self.units = units
        self.nano = nano


def test_quotation_to_float() -> None:
    q = FakeQuotation(100, 500_000_000)
    assert _quotation_to_float(q) == 100.5


def test_serialize_trade() -> None:
    """Проверяет корректную сериализацию protobuf-сделки в JSON."""

    class FakeTrade:
        def __init__(self):
            self.trade_id = "trade-1"
            self.order_id = "order-1"
            self.account_id = "acc-1"
            self.instrument_uid = "uid-1"
            self.figi = "BBG004730N88"
            self.direction = "BUY"
            self.price = FakeQuotation(200, 750_000_000)
            self.quantity = 5
            self.date_time = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)

    payload = _serialize_trade(FakeTrade())
    assert payload["trade_id"] == "trade-1"
    assert payload["direction"] == "BUY"
    assert payload["price_units"] == 200
    assert payload["price_nano"] == 750_000_000
    assert payload["quantity"] == 5
