-- =============================================================================
-- SILVER LAYER (DDS)
-- Партиционирование по дате (RANGE) для оптимизации запросов и UPSERT.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS dds;

-- -----------------------------------------------------------------------------
-- Таблица сделок (партиционирована по дням)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.dds_trades (
    trade_id        VARCHAR(64) NOT NULL,
    order_id        VARCHAR(64),
    account_id      VARCHAR(64),
    instrument_uid  VARCHAR(64),
    figi            VARCHAR(32),
    direction       VARCHAR(8),
    price_rub       NUMERIC(18, 9) NOT NULL CHECK (price_rub > 0),
    quantity        BIGINT NOT NULL CHECK (quantity > 0),
    trade_datetime  TIMESTAMPTZ NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_id, trade_datetime) 
) PARTITION BY RANGE (trade_datetime);

CREATE TABLE IF NOT EXISTS dds.dds_trades_2026_10 PARTITION OF dds.dds_trades
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE IF NOT EXISTS dds.dds_trades_2026_11 PARTITION OF dds.dds_trades
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE IF NOT EXISTS dds.dds_trades_default PARTITION OF dds.dds_trades DEFAULT;

CREATE INDEX IF NOT EXISTS idx_dds_trades_figi_time ON dds.dds_trades (figi, trade_datetime DESC);


-- -----------------------------------------------------------------------------
-- Таблица свечей (партиционирована по месяцам)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.dds_candles (
    figi         VARCHAR(32) NOT NULL,
    interval     VARCHAR(8) NOT NULL,
    candle_time  TIMESTAMPTZ NOT NULL,
    open         NUMERIC(18, 9),
    high         NUMERIC(18, 9),
    low          NUMERIC(18, 9),
    close        NUMERIC(18, 9),
    volume       BIGINT,
    is_complete  BOOLEAN,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (figi, interval, candle_time)
) PARTITION BY RANGE (candle_time);

CREATE TABLE IF NOT EXISTS dds.dds_candles_2026_10 PARTITION OF dds.dds_candles
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE IF NOT EXISTS dds.dds_candles_2026_11 PARTITION OF dds.dds_candles
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE IF NOT EXISTS dds.dds_candles_default PARTITION OF dds.dds_candles DEFAULT;

CREATE INDEX IF NOT EXISTS idx_dds_candles_time ON dds.dds_candles (candle_time DESC);