-- Query 4: Row Count Reconciliation
-- Reconciles record counts across currency pairs and timeframes, verifying minimum
-- and maximum observed timestamps and checking for unexpected record drops or temporal gaps.

SELECT
    Pair,
    Timeframe,
    Year,
    COUNT(*) AS Actual_Row_Count,
    MIN(Timestamp_UTC) AS Earliest_Timestamp,
    MAX(Timestamp_UTC) AS Latest_Timestamp,
    COUNT(DISTINCT DATE(Timestamp_UTC)) AS Distinct_Trading_Days
FROM
    forex_cleaned
GROUP BY
    Pair,
    Timeframe,
    Year
ORDER BY
    Pair,
    Timeframe,
    Year;
