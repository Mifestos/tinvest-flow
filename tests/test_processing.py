"""Unit-тесты для слоя Processing (ETL)."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd

from tinvest_flow.processing.bronze_to_silver import load_candles_to_postgres
from tinvest_flow.processing.silver_to_gold import build_market_activity_mart
from tinvest_flow.processing.trades_to_gold import build_trade_activity_mart


@patch("tinvest_flow.processing.bronze_to_silver.get_s3_client")
@patch("tinvest_flow.processing.bronze_to_silver.psycopg2.connect")
@patch("tinvest_flow.processing.bronze_to_silver.execute_values")
def test_load_candles_to_postgres(
    mock_execute_values: MagicMock, mock_connect: MagicMock, mock_s3_client: MagicMock
) -> None:
    fake_data = [
        {
            "figi": "SBER",
            "interval": "5min",
            "time": "2026-10-01T10:00:00",
            "open": 100,
            "high": 100,
            "low": 100,
            "close": 100,
            "volume": 10,
            "is_complete": True,
        }
    ]

    mock_s3 = mock_s3_client.return_value
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps(fake_data).encode("utf-8"))
    }

    load_candles_to_postgres(ds="2026-10-01", figi="SBER", interval="5min")

    mock_s3.get_object.assert_called_once()
    mock_connect.assert_called_once()
    mock_execute_values.assert_called_once()


@patch("tinvest_flow.processing.silver_to_gold.clickhouse_connect.get_client")
@patch("tinvest_flow.processing.silver_to_gold.psycopg2.connect")
def test_build_market_activity_mart(mock_pg_connect: MagicMock, mock_ch_client: MagicMock) -> None:
    mock_pg_conn = mock_pg_connect.return_value
    mock_cursor = mock_pg_conn.cursor.return_value.__enter__.return_value

    mock_cursor.fetchmany.side_effect = [
        [
            {
                "figi": "SBER",
                "day": "2026-10-01",
                "open_price": 100,
                "high_price": 105,
                "low_price": 95,
                "close_price": 102,
                "volume": 1000,
                "candle_count": 10,
                "avg_trade_size": 100,
            }
        ],
        [],
    ]
    mock_ch = mock_ch_client.return_value

    build_market_activity_mart()

    mock_ch.command.assert_called_once_with("TRUNCATE TABLE gold.mart_market_activity")
    mock_ch.insert_df.assert_called_once()

    args, _ = mock_ch.insert_df.call_args
    assert args[0] == "gold.mart_market_activity"
    assert isinstance(args[1], pd.DataFrame)
    assert len(args[1]) == 1


@patch("tinvest_flow.processing.trades_to_gold.clickhouse_connect.get_client")
@patch("tinvest_flow.processing.trades_to_gold.create_engine")
@patch("tinvest_flow.processing.trades_to_gold.pd.read_sql_query")
def test_build_trade_activity_mart(
    mock_read_sql: MagicMock, mock_create_engine: MagicMock, mock_ch_client: MagicMock
) -> None:
    mock_read_sql.return_value = pd.DataFrame(
        {
            "figi": ["SBER"],
            "trade_date": ["2026-10-01"],
            "total_volume": [100],
            "total_trades": [5],
            "buy_volume": [60],
            "sell_volume": [40],
            "vwap": [100.5],
            "max_price": [101.0],
            "min_price": [100.0],
        }
    )
    mock_ch = mock_ch_client.return_value

    build_trade_activity_mart(days_back=1)

    mock_read_sql.assert_called_once()
    mock_ch.insert_df.assert_called_once()
    args, _ = mock_ch.insert_df.call_args
    assert args[0] == "gold.mart_trade_activity"
    assert len(args[1]) == 1
