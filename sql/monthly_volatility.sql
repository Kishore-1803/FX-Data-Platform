-- Query 1: Monthly Volatility and Average Price
-- Calculates monthly average close price, standard deviation of close (volatility),
-- and average percentage change per currency pair and month.

SELECT
    Pair,
    Year,
    MONTH(Timestamp_UTC) AS Month,
    COUNT(*) AS Total_Bars,
    ROUND(AVG(Close), 5) AS Avg_Close,
    ROUND(STDDEV(Close), 6) AS Monthly_Volatility,
    ROUND(AVG(ChangePct), 4) AS Avg_Change_Pct
FROM
    forex_cleaned
GROUP BY
    Pair,
    Year,
    MONTH(Timestamp_UTC)
ORDER BY
    Pair,
    Year,
    Month;
