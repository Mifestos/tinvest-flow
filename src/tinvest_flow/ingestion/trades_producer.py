"""Потоковая передача сделок из T-Invest API в Kafka в реальном времени."""
import asyncio
import json
import logging
import signal

from aiokafka import AIOKafkaProducer
from t_tech.invest import AsyncClient
from t_tech.invest.constants import INVEST_GRPC_API_SANDBOX

from tinvest_flow.config import settings

logger = logging.getLogger(__name__)


def _serialize_trade(trade: object) -> dict:
    """Сериализует объект сделки в словарь для JSON."""
    price = getattr(trade, "price", None)
    return {
        "trade_id": getattr(trade, "trade_id", None),
        "order_id": getattr(trade, "order_id", None),
        "account_id": getattr(trade, "account_id", None),
        "instrument_uid": getattr(trade, "instrument_uid", None),
        "figi": getattr(trade, "figi", None),
        "direction": str(getattr(trade, "direction", "")),
        "price_units": getattr(price, "units", 0) if price else 0,
        "price_nano": getattr(price, "nano", 0) if price else 0,
        "quantity": getattr(trade, "quantity", 0),
        "date_time": trade.date_time.isoformat() if hasattr(trade, "date_time") else None,
    }


async def run_producer() -> None:
    """Запускает асинхронный Kafka-продюсер для прослушивания стрима сделок."""
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        enable_idempotence=True,
    )
    await producer.start()
    stop_event = asyncio.Event()

    def _shutdown(*args: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    backoff = 1
    while not stop_event.is_set():
        try:
            async with AsyncClient(
                token=settings.tinkoff_sandbox_token.get_secret_value(),
                target=INVEST_GRPC_API_SANDBOX,
            ) as client:
                async for event in client.market_data_stream.trades_stream(
                    accounts=[settings.tinkoff_sandbox_account_id]
                ):
                    if stop_event.is_set():
                        break
                    
                    trades = getattr(event, "trades", [])
                    for trade in trades:
                        payload = _serialize_trade(trade)
                        await producer.send_and_wait(settings.kafka_topic_trades, payload)
                        logger.info(f"Сделка {payload['trade_id']} отправлена в Kafka")
                        
            backoff = 1
        except Exception as exc:
            logger.warning(f"Ошибка стрима: {exc}. Переподключение через {backoff} сек.")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    await producer.stop()
    logger.info("Продюсер корректно остановлен")


if __name__ == "__main__":
    from tinvest_flow.common.logging import setup_logging
    setup_logging()
    asyncio.run(run_producer())