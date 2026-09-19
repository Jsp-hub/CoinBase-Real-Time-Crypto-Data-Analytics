# Grafana dashboard query reference

These original summary queries return one wide row. They are useful for
inspection or for a table panel before any Grafana transformations are applied.
They expect the dashboard product variable to be named `product`.

## Current forming candle

```sql
SELECT
  open_price AS `Open`,
  high_price AS `High`,
  low_price AS `Low`,
  close_price AS `Close`,
  vwap AS `VWAP`,
  notional_volume_usd AS `USD Volume`,
  trade_count AS `Trades`,
  candle_status AS `Status`,
  last_updated_at AS `Updated At`
FROM market_candles_1m
WHERE product_id = '$product'
  AND candle_status = 'FORMING'
ORDER BY window_start DESC
LIMIT 1;
```

## Previous final candle

```sql
SELECT
  window_start AS `Minute`,
  open_price AS `Open`,
  high_price AS `High`,
  low_price AS `Low`,
  close_price AS `Close`,
  vwap AS `VWAP`,
  notional_volume_usd AS `USD Volume`,
  trade_count AS `Trades`
FROM market_candles_1m
WHERE product_id = '$product'
  AND candle_status = 'FINAL'
ORDER BY window_start DESC
LIMIT 1;
```
