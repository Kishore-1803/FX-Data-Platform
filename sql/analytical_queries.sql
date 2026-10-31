-- Query 1: Rolling 24-Hour Moving Average & Return (Window Function)
-- Demonstrates SQL analytical window processing over time series
SELECT 
    pair,
    timestamp_utc,
    close,
    ROUND(AVG(close) OVER (
        PARTITION BY pair 
        ORDER BY timestamp_utc 
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    )::numeric, 5) AS rolling_ma_24h,
    ROUND(((close - LAG(close, 24) OVER (PARTITION BY pair ORDER BY timestamp_utc)) / 
        LAG(close, 24) OVER (PARTITION BY pair ORDER BY timestamp_utc) * 100.0)::numeric, 3) AS ret_24h_pct
FROM curated_rates
WHERE timestamp_utc >= '2024-12-30'
ORDER BY pair, timestamp_utc DESC
LIMIT 10;

-- Query 2: Daily Volatility and Trading Range per Pair
SELECT 
    pair,
    DATE(timestamp_utc) AS trade_date,
    COUNT(*) AS bars_recorded,
    ROUND(MIN(low)::numeric, 5) AS daily_low,
    ROUND(MAX(high)::numeric, 5) AS daily_high,
    ROUND((MAX(high) - MIN(low))::numeric, 5) AS daily_range,
    ROUND(STDDEV(close)::numeric, 6) AS hourly_close_volatility
FROM curated_rates
GROUP BY pair, DATE(timestamp_utc)
HAVING COUNT(*) >= 20
ORDER BY trade_date DESC, pair
LIMIT 12;

-- Query 3: Forward-Fill Audit & Data Quality Summary
SELECT 
    pair,
    COUNT(*) AS total_bars,
    SUM(CASE WHEN is_filled THEN 1 ELSE 0 END) AS filled_bars_count,
    ROUND((SUM(CASE WHEN is_filled THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 3) AS filled_bars_pct,
    MAX(gap_length) AS max_gap_hours_filled
FROM curated_rates
GROUP BY pair
ORDER BY filled_bars_count DESC;
