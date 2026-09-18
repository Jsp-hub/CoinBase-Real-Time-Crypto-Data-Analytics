USE coinbase_analytics;

CREATE TABLE IF NOT EXISTS market_candles_1m (
    product_id VARCHAR(20) NOT NULL,
    window_start DATETIME(3) NOT NULL,
    window_end DATETIME(3) NOT NULL,
    open_price DECIMAL(30, 12) NOT NULL,
    high_price DECIMAL(30, 12) NOT NULL,
    low_price DECIMAL(30, 12) NOT NULL,
    close_price DECIMAL(30, 12) NOT NULL,
    trade_count BIGINT UNSIGNED NOT NULL,
    base_volume DECIMAL(30, 12) NOT NULL,
    notional_volume_usd DECIMAL(30, 12) NOT NULL,
    vwap DECIMAL(30, 12) NOT NULL,
    emitted_at DATETIME(3) NOT NULL,
    candle_status ENUM('FORMING', 'FINAL') NOT NULL DEFAULT 'FINAL',
    last_updated_at DATETIME(3) NOT NULL,
    PRIMARY KEY (product_id, window_start),
    INDEX idx_market_candles_1m_window_start (window_start)
);
