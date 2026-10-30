-- Query 2: Top Volatility Days
-- Identifies the top 10 highest-volatility trading periods per currency pair
-- ranked by bar price range (High - Low) and relative percentage range.

WITH RankedDays AS (
    SELECT
        Pair,
        Date,
        Close,
        Range,
        ChangePct,
        ROUND((Range / Low) * 100, 3) AS Range_Pct,
        ROW_NUMBER() OVER (PARTITION BY Pair ORDER BY Range DESC) AS Volatility_Rank
    FROM
        forex_cleaned
)
SELECT
    Volatility_Rank,
    Pair,
    Date,
    Close,
    Range,
    Range_Pct,
    ChangePct
FROM
    RankedDays
WHERE
    Volatility_Rank <= 10
ORDER BY
    Pair,
    Volatility_Rank;
