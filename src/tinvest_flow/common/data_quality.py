"""Data Quality checks через Great Expectations."""

import logging

import great_expectations as gx

from tinvest_flow.config import settings

logger = logging.getLogger(__name__)


def _get_ephemeral_context() -> gx.DataContext:
    """Создает in-memory контекст GX для одноразовой проверки."""
    return gx.get_context(mode="ephemeral")


def run_trades_dq_check() -> None:
    """Прогоняет DQ-проверки для dds_trades."""
    context = _get_ephemeral_context()

    datasource = context.sources.add_postgres(
        name="tinvest_silver",
        connection_string=settings.postgres_dsn,
    )
    asset = datasource.add_table_asset(
        name="dds_trades",
        table_name="dds_trades",
        schema_name="dds",
    )

    validator = context.get_validator(
        batch_request=asset.build_batch_request(),
    )

    validator.expect_column_values_to_not_be_null(column="figi")
    validator.expect_column_values_to_be_between(column="price_rub", min_value=0.01)
    validator.expect_column_values_to_be_between(column="quantity", min_value=1)
    validator.expect_column_values_to_be_unique(column="trade_id")

    results = validator.validate()

    if not results.success:
        failed = [r.expectation_config.expectation_type for r in results.results if not r.success]
        raise ValueError(f"DQ failed for dds_trades: {failed}")

    logger.info("All DQ checks PASSED for dds_trades")


def run_candles_dq_check() -> None:
    """Прогоняет DQ-проверки для dds_candles."""
    context = _get_ephemeral_context()
    datasource = context.sources.add_postgres(
        name="tinvest_silver_candles",
        connection_string=settings.postgres_dsn,
    )
    asset = datasource.add_table_asset(
        name="dds_candles",
        table_name="dds_candles",
        schema_name="dds",
    )

    validator = context.get_validator(
        batch_request=asset.build_batch_request(),
    )

    validator.expect_column_values_to_not_be_null(column="figi")
    validator.expect_column_values_to_be_between(column="close", min_value=0.01)
    validator.expect_column_values_to_be_between(column="volume", min_value=0)

    results = validator.validate()

    if not results.success:
        failed = [r.expectation_config.expectation_type for r in results.results if not r.success]
        raise ValueError(f"DQ failed for dds_candles: {failed}")

    logger.info("All DQ checks PASSED for dds_candles")
