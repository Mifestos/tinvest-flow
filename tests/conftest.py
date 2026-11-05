"""Общие фикстуры и моки для всех тестов."""

import os
from datetime import UTC, datetime

import pytest

os.environ["TINKOFF_SANDBOX_TOKEN"] = "test_token"
os.environ["TINKOFF_SANDBOX_ACCOUNT_ID"] = "test_account_id"


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


@pytest.fixture
def fake_candle() -> FakeCandle:
    return FakeCandle()


@pytest.fixture
def fake_trade() -> FakeTrade:
    return FakeTrade()
