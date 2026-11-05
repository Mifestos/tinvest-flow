"""Тесты для конфигурации."""

from tinvest_flow.config import settings


def test_postgres_jdbc_url_format() -> None:
    url = settings.postgres_jdbc_url
    assert url.startswith("jdbc:postgresql://")
    assert ":5432/" in url
    assert "dwh" in url
