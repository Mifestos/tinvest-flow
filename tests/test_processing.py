"""Юнит-тесты для слоя Processing."""
from tinvest_flow.config import settings


def test_postgres_jdbc_url_format() -> None:
    """Проверяет, что строка подключения для Spark JDBC сформирована верно."""
    url = settings.postgres_jdbc_url
    assert url.startswith("jdbc:postgresql://")
    assert "dwh" in url