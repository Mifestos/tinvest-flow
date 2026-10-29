CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.trades_raw (
    trade_id        String,
    figi            String,
    price_rub       Decimal(18, 9),
    quantity        Int64,
    trade_datetime  DateTime64(3, 'UTC'),
    loaded_at       DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(trade_datetime)
ORDER BY (figi, trade_datetime, trade_id);

CREATE TABLE IF NOT EXISTS gold.mart_market_activity (
    figi            String,
    minute          DateTime,
    open_price      Decimal(18, 9),
    high_price      Decimal(18, 9),
    low_price       Decimal(18, 9),
    close_price     Decimal(18, 9),
    volume          Int64,
    trade_count     Int64,
    avg_trade_size  Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (figi, minute);