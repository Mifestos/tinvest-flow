"""Инициализация схемы Silver и партиций."""

from alembic import op

revision = "20251106_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS dds")

    op.execute("""
        CREATE TABLE dds.dds_trades (
            trade_id        VARCHAR(64) NOT NULL,
            order_id        VARCHAR(64),
            account_id      VARCHAR(64),
            instrument_uid  VARCHAR(64),
            figi            VARCHAR(32),
            direction       VARCHAR(32),
            price_rub       NUMERIC(18, 9) NOT NULL CHECK (price_rub > 0),
            quantity        BIGINT NOT NULL CHECK (quantity > 0),
            trade_datetime  TIMESTAMPTZ NOT NULL,
            loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (trade_id, trade_datetime)
        ) PARTITION BY RANGE (trade_datetime)
    """)

    op.execute(
        "CREATE TABLE dds.dds_trades_2026_10 PARTITION OF dds.dds_trades "
        "FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')"
    )
    op.execute(
        "CREATE TABLE dds.dds_trades_2026_11 PARTITION OF dds.dds_trades "
        "FOR VALUES FROM ('2026-11-01') TO ('2026-12-01')"
    )
    op.execute(
        "CREATE INDEX idx_dds_trades_figi_time ON dds.dds_trades (figi, trade_datetime DESC)"
    )

    op.execute("""
        CREATE TABLE dds.dds_candles (
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
        ) PARTITION BY RANGE (candle_time)
    """)

    op.execute(
        "CREATE TABLE dds.dds_candles_2026_10 PARTITION OF dds.dds_candles "
        "FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')"
    )
    op.execute(
        "CREATE TABLE dds.dds_candles_2026_11 PARTITION OF dds.dds_candles "
        "FOR VALUES FROM ('2026-11-01') TO ('2026-12-01')"
    )
    op.execute("CREATE INDEX idx_dds_candles_time ON dds.dds_candles (candle_time DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dds.dds_candles CASCADE")
    op.execute("DROP TABLE IF EXISTS dds.dds_trades CASCADE")
    op.execute("DROP SCHEMA IF EXISTS dds CASCADE")
