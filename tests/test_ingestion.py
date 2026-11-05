"""Unit-тесты для слоя Ingestion (парсинг и пагинация)."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from t_tech.invest import CandleInterval

from tinvest_flow.ingestion.rest_loader import (
    _candles_to_records,
    _quotation_to_float,
    fetch_candles,
)
from tinvest_flow.ingestion.trades_producer import _serialize_trade


class FakeQuotation:
    def __init__(self, units: int, nano: int) -> None:
        self.units = units
        self.nano = nano


class FakeCandle:
    def __init__(self) -> None:
        self.time = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)
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
        self.date_time = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)


def test_quotation_to_float() -> None:
    assert _quotation_to_float(FakeQuotation(100, 500_000_000)) == 100.5


def test_candles_to_records() -> None:
    records = _candles_to_records([FakeCandle()], figi="BBG004730N88", interval_name="5min")

    assert len(records) == 1
    record = records[0]
    assert record["figi"] == "BBG004730N88"
    assert record["interval"] == "5min"
    assert record["open"] == 100.5
    assert record["close"] == 100.25
    assert record["volume"] == 1000


def test_serialize_trade() -> None:
    payload = _serialize_trade(FakeTrade())

    assert payload["trade_id"] == "trade-1"
    assert payload["direction"] == "BUY"
    assert payload["price_units"] == 200
    assert payload["price_nano"] == 750_000_000
    assert payload["quantity"] == 5
    assert "trade_datetime" in payload
    assert payload["trade_datetime"] == "2026-10-29T12:00:00+00:00"


@patch("tinvest_flow.ingestion.rest_loader.Client")
def test_fetch_candles_pagination(mock_client_class: MagicMock) -> None:
    """Проверяет, что fetch_candles разбивает длинный запрос на короткие окна."""
    mock_client = mock_client_class.return_value.__enter__.return_value

    fake_response = MagicMock()
    fake_candle = MagicMock()
    fake_candle.time.isoformat.return_value = "2026-01-01T10:00:00"
    fake_response.candles = [fake_candle]
    mock_client.market_data.get_candles.return_value = fake_response

    from_date = datetime(2026, 1, 1, tzinfo=UTC)
    to_date = datetime(2026, 1, 15, tzinfo=UTC)

    interval = CandleInterval.CANDLE_INTERVAL_5_MIN

    fetch_candles("TEST_FIGI", interval, from_date, to_date)

    assert mock_client.market_data.get_candles.call_count == 2
