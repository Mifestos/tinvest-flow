-- migrations/clickhouse/V1__init_gold_schema.sql

CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.mart_market_activity (
    figi            String,
    day             DateTime,
    open_price      Decimal(18, 9),
    high_price      Decimal(18, 9),
    low_price       Decimal(18, 9),
    close_price     Decimal(18, 9),
    volume          Int64,
    candle_count    Int64,
    avg_trade_size  Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (figi, day);

CREATE TABLE IF NOT EXISTS gold.mart_trade_activity (
    figi            String,
    trade_date      Date,
    total_volume    Int64,
    total_trades    Int64,
    buy_volume      Int64,
    sell_volume     Int64,
    vwap            Float64,      
    max_price       Decimal(18, 9),
    min_price       Decimal(18, 9),
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (figi, trade_date);