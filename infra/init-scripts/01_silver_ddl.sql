CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.dds_trades (
    trade_id        VARCHAR(64) PRIMARY KEY,
    order_id        VARCHAR(64),
    account_id      VARCHAR(64),
    instrument_uid  VARCHAR(64),
    figi            VARCHAR(32),
    direction       VARCHAR(8),
    price_rub       NUMERIC(18, 9) NOT NULL CHECK (price_rub > 0),
    quantity        BIGINT NOT NULL CHECK (quantity > 0),
    trade_datetime  TIMESTAMPTZ NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dds_trades_figi_time
    ON dds.dds_trades (figi, trade_datetime DESC);

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
);

CREATE INDEX IF NOT EXISTS idx_dds_candles_time
    ON dds.dds_candles (candle_time DESC);