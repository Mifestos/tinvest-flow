"""Unit-тесты для продюсера Kafka."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from tinvest_flow.ingestion.trades_producer import run_producer


async def empty_async_gen(*args, **kwargs):
    """Пустой асинхронный генератор для имитации стрима без данных."""
    if False:
        yield


@patch("tinvest_flow.ingestion.trades_producer.asyncio.Event")
@patch("tinvest_flow.ingestion.trades_producer.signal.signal")
@patch("tinvest_flow.ingestion.trades_producer.AsyncClient")
@patch("tinvest_flow.ingestion.trades_producer.AIOKafkaProducer")
def test_run_producer_stops_correctly(
    mock_producer_class: MagicMock,
    mock_async_client: MagicMock,
    mock_signal: MagicMock,
    mock_event_class: MagicMock,
) -> None:
    """Проверяет корректный запуск и остановку цикла продюсера."""

    mock_event = MagicMock()
    mock_event.is_set.side_effect = [False, True]
    mock_event_class.return_value = mock_event

    mock_producer = mock_producer_class.return_value
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()

    mock_client = mock_async_client.return_value.__aenter__.return_value
    mock_client.orders_stream.trades_stream = empty_async_gen

    asyncio.run(run_producer())

    mock_producer.start.assert_called_once()
    mock_producer.stop.assert_called_once()
