"""Unit-тесты для Spark Streaming (UPSERT)."""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def upsert_to_postgres():
    """Фикстура для пропуска тестов без pyspark и ленивого импорта функции."""
    pytest.importorskip("pyspark")

    from tinvest_flow.processing.stream_trades_to_silver import _upsert_to_postgres

    return _upsert_to_postgres


@patch("tinvest_flow.processing.stream_trades_to_silver.psycopg2.connect")
def test_upsert_to_postgres_empty_batch(mock_pg: MagicMock, upsert_to_postgres) -> None:
    """Если батч пустой, подключение к БД не должно открываться."""
    batch_df = MagicMock()
    batch_df.isEmpty.return_value = True

    upsert_to_postgres(batch_df, batch_id=1)
    mock_pg.assert_not_called()


@patch("tinvest_flow.processing.stream_trades_to_silver.F")
@patch("tinvest_flow.processing.stream_trades_to_silver.psycopg2.connect")
def test_upsert_to_postgres_success(
    mock_pg_connect: MagicMock, mock_f: MagicMock, upsert_to_postgres
) -> None:
    """Проверяет, что данные пишутся в staging-таблицу и выполняется UPSERT."""
    batch_df = MagicMock()
    batch_df.isEmpty.return_value = False

    mock_df = MagicMock()
    batch_df.withColumn.return_value = mock_df
    mock_df.drop.return_value = mock_df
    mock_df.dropDuplicates.return_value = mock_df

    mock_writer = MagicMock()
    mock_df.write.format.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer

    mock_conn = mock_pg_connect.return_value
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value

    upsert_to_postgres(batch_df, batch_id=1)

    mock_df.write.format.assert_called_once_with("jdbc")
    mock_writer.save.assert_called_once()

    assert mock_cursor.execute.call_count == 2
    sql_queries = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("INSERT INTO dds.dds_trades" in q for q in sql_queries)
    assert any("DROP TABLE" in q for q in sql_queries)

    mock_conn.commit.assert_called_once()
