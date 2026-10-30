-- Query 3: Volume Aggregates
-- Computes annual and pair-level trading volume metrics: total traded volume,
-- mean volume per period, and peak bar volume.

SELECT
    Pair,
    Year,
    Timeframe,
    COUNT(*) AS Bar_Count,
    SUM(Volume) AS Total_Traded_Volume,
    ROUND(AVG(Volume), 1) AS Avg_Bar_Volume,
    MAX(Volume) AS Peak_Bar_Volume
FROM
    forex_cleaned
GROUP BY
    Pair,
    Year,
    Timeframe
ORDER BY
    Pair,
    Year,
    Timeframe;
