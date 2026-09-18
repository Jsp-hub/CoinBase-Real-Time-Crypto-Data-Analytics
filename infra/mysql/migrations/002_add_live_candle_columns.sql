SET @add_candle_status = (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'market_candles_1m'
              AND column_name = 'candle_status'
        ),
        'SELECT 1',
        "ALTER TABLE market_candles_1m ADD COLUMN candle_status ENUM('FORMING', 'FINAL') NOT NULL DEFAULT 'FINAL' AFTER emitted_at"
    )
);
PREPARE add_candle_status_statement FROM @add_candle_status;
EXECUTE add_candle_status_statement;
DEALLOCATE PREPARE add_candle_status_statement;

SET @add_last_updated_at = (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'market_candles_1m'
              AND column_name = 'last_updated_at'
        ),
        'SELECT 1',
        'ALTER TABLE market_candles_1m ADD COLUMN last_updated_at DATETIME(3) NULL AFTER candle_status'
    )
);
PREPARE add_last_updated_at_statement FROM @add_last_updated_at;
EXECUTE add_last_updated_at_statement;
DEALLOCATE PREPARE add_last_updated_at_statement;

UPDATE market_candles_1m
SET last_updated_at = emitted_at
WHERE last_updated_at IS NULL;

ALTER TABLE market_candles_1m
    MODIFY COLUMN last_updated_at DATETIME(3) NOT NULL;
