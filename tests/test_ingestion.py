"""Юнит-тесты для слоя Ingestion."""
from tinvest_flow.ingestion.rest_loader import _quotation_to_float


class FakeQuotation:
    def __init__(self, units: int, nano: int) -> None:
        self.units = units
        self.nano = nano


def test_quotation_to_float() -> None:
    q = FakeQuotation(100, 500_000_000)
    assert _quotation_to_float(q) == 100.5