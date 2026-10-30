import pytest
from pyspark.sql.functions import col
from src.clean import clean_forex_data
from src.features import build_features

def test_clean_deduplication(tiny_raw_data):
    """Verify that duplicates on (Pair, Date) are eliminated."""
    initial_count = tiny_raw_data.count()
    cleaned = clean_forex_data(tiny_raw_data)
    final_count = cleaned.count()

    # 7 input rows with 1 duplicate -> exactly 6 rows after dedup & fill
    assert initial_count == 7
    assert final_count == 6

    # Verify no duplicate (Pair, Date) remains
    dup_check = cleaned.groupBy("Pair", "Date").count().filter(col("count") > 1).count()
    assert dup_check == 0

def test_clean_forward_fill(tiny_raw_data):
    """Verify that null Close values are forward-filled from previous row within the same Pair."""
    cleaned = clean_forex_data(tiny_raw_data)
    rows = cleaned.filter(col("Pair") == "EURUSD").orderBy("Timestamp_UTC").collect()

    # At index 2 (2024-01-01 02:00:00), Close was None in raw data; should now equal 1.1060 (from 01:00:00)
    filled_row = rows[2]
    assert filled_row["Date"] == "2024-01-01 02:00:00"
    assert filled_row["Close"] == pytest.approx(1.1060, rel=1e-4)

def test_features_arithmetic(tiny_raw_data):
    """Verify Range, Change, and ChangePct match exact expected values."""
    cleaned = clean_forex_data(tiny_raw_data)
    features = build_features(cleaned)
    first_row = features.filter(col("Pair") == "EURUSD").orderBy("Date").first()

    expected_range = 1.1050 - 1.0950
    expected_change = 1.1020 - 1.1000
    expected_change_pct = (expected_change / 1.1000) * 100.0

    assert first_row["Range"] == pytest.approx(expected_range, rel=1e-4)
    assert first_row["Change"] == pytest.approx(expected_change, rel=1e-4)
    assert first_row["ChangePct"] == pytest.approx(expected_change_pct, rel=1e-4)

def test_features_partition_isolation(tiny_raw_data):
    """Verify window functions do not bleed data across distinct currency pairs."""
    cleaned = clean_forex_data(tiny_raw_data)
    features = build_features(cleaned)

    # First row of GBPUSD must have Close_lag1 = None, NOT the last row of EURUSD!
    gbp_first = features.filter(col("Pair") == "GBPUSD").orderBy("Date").first()
    assert gbp_first["Close_lag1"] is None
    assert gbp_first["Change_lag1"] is None

    # First row of GBPUSD MA5 must equal its own Close (1.2520), not an average with EURUSD
    assert gbp_first["MA5"] == pytest.approx(1.2520, rel=1e-4)
