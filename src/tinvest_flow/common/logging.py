"""Настройка структурированного логирования."""
import logging
import sys

from pythonjsonlogger import jsonlogger


def setup_logging(level: str = "INFO") -> None:
    """Настраивает вывод логов в формате JSON."""
    logger = logging.getLogger()
    logger.setLevel(level)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(  # type: ignore[attr-defined]
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        timestamp=True,
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("aiokafka").setLevel(logging.WARNING)