"""Утилита для E2E-тестирования потокового контура (Kafka -> Spark -> Postgres)."""

import asyncio
import json
import logging
import os
import random
import uuid
from datetime import UTC, datetime

from aiokafka import AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TradeSimulator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC_TRADES", "raw_trades")

INSTRUMENTS = [
    {"figi": "BBG004730N88", "ticker": "SBER", "base_price": 291.50},
    {"figi": "BBG004730RP0", "ticker": "GAZP", "base_price": 115.20},
    {"figi": "BBG004731032", "ticker": "LKOH", "base_price": 4500.00},
]


async def generate_stream(total_events: int = 5):
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    logger.info(f"Starting simulation of {total_events} events...")

    try:
        for i in range(1, total_events + 1):
            inst = random.choice(INSTRUMENTS)
            price = inst["base_price"] + random.uniform(-1.0, 1.0)

            payload = {
                "trade_id": str(uuid.uuid4()),
                "order_id": str(uuid.uuid4()),
                "account_id": "demo-sandbox-account",
                "instrument_uid": "demo-instrument-uid",
                "figi": inst["figi"],
                "direction": random.choice(["BUY", "SELL"]),
                "price_units": int(price),
                "price_nano": int((price % 1) * 1e9),
                "quantity": random.randint(1, 100),
                "date_time": datetime.now(UTC).isoformat(),
            }

            await producer.send_and_wait(TOPIC, payload)
            logger.info(
                f"[{i}/{total_events}] Emitted: {inst['ticker']} | "
                f"{payload['direction']} | {price:.2f} RUB"
            )
            await asyncio.sleep(0.2)  # Эмуляция I/O задержки

    except Exception as e:
        logger.error(f"Simulation failed: {e}")
    finally:
        await producer.stop()
        logger.info("Simulation finished. Producer closed.")


if __name__ == "__main__":
    asyncio.run(generate_stream())
