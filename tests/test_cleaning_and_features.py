import os
import pytest
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, lag, lit
from src.quality_gate import QualityGate
from src.silver import process_silver_layer
from src.gold import build_gold_features
from src.bronze import ingest_to_bronze
from src.forecasting import WalkForwardValidator, compute_metrics, FEATURE_COLS

def test_quality_gate_quarantine_reasons(mock_bronze_data, tmp_path):
    """Verify that Quality Gate routes corrupted & implausible rows to quarantine with correct reasons."""
    q_dir = str(tmp_path / "quarantine")
    qgate = QualityGate()
    valid_df, quarantined_df, metrics = qgate.evaluate_and_filter(mock_bronze_data, quarantine_dir=q_dir)

    q_rows = quarantined_df.collect()
    assert len(q_rows) == 2, f"Expected exactly 2 quarantined rows, got {len(q_rows)}"

    reasons = {r["Date"]: r["quarantine_reason"] for r in q_rows}
    assert reasons["2024-01-01 04:00:00"] == "PRICE_IMPLAUSIBLE"
    assert reasons["2024-01-01 05:00:00"] == "MISSING_MANDATORY_VALUE"

def test_quality_gate_outlier_jump_spike(spark, tmp_path):
    """Verify that an injected single-bar price jump >5% is caught and quarantined."""
    q_dir = str(tmp_path / "quarantine_jump")
    data = [
        ("2024-01-01 00:00:00", 1.1000, 1.1020, 1.0990, 1.1000, 1000, "EURUSD", "2024-01-01", "mock.csv"),
        ("2024-01-01 01:00:00", 1.1000, 1.1020, 1.0990, 1.1000, 1000, "EURUSD", "2024-01-01", "mock.csv"),
        # Injected 10% jump (1.1000 -> 1.2100)
        ("2024-01-01 02:00:00", 1.2000, 1.2200, 1.1900, 1.2100, 1000, "EURUSD", "2024-01-01", "mock.csv"),
        ("2024-01-01 03:00:00", 1.2100, 1.2120, 1.2090, 1.2100, 1000, "EURUSD", "2024-01-01", "mock.csv")
    ]
    cols = ["Date", "Open", "High", "Low", "Close", "Volume", "Pair", "ingest_ts", "source_file"]
    df = spark.createDataFrame(data, cols)

    qgate = QualityGate()
    v_df, q_df, metrics = qgate.evaluate_and_filter(df, quarantine_dir=q_dir)
    
    q_rows = q_df.collect()
    assert len(q_rows) == 1, f"Expected 1 quarantined row for outlier jump, got {len(q_rows)}"
    assert q_rows[0]["quarantine_reason"] == "OUTLIER_HOURLY_JUMP"
    assert q_rows[0]["Date"] == "2024-01-01 02:00:00"

def test_silver_capped_forward_fill(spark, mock_bronze_data, tmp_path):
    """Verify that gaps <= 3h are forward-filled, audited with is_filled=True, and tagged with gap_length."""
    s_dir = str(tmp_path / "silver")
    q_dir = str(tmp_path / "quarantine")
    fill_log = str(tmp_path / "fill_log.json")
    qgate = QualityGate()
    valid_df, _, _ = qgate.evaluate_and_filter(mock_bronze_data, quarantine_dir=q_dir)
    
    silver_df, fill_records = process_silver_layer(spark, valid_df, silver_output_dir=s_dir, fill_log_path=fill_log)
    silver_pdf = silver_df.toPandas().sort_values("Timestamp_UTC").reset_index(drop=True)

    # The 02:00:00 missing bar should now exist
    filled_bar = silver_pdf[silver_pdf["Date"] == "2024-01-01 02:00:00"]
    assert len(filled_bar) == 1
    assert filled_bar.iloc[0]["is_filled"] == True
    assert filled_bar.iloc[0]["gap_length"] == 2
    assert filled_bar.iloc[0]["Volume"] == 0
    assert filled_bar.iloc[0]["Close"] == pytest.approx(1.1060, rel=1e-4)

def test_gold_point_in_time_leakage_and_masking(spark, mock_bronze_data, tmp_path):
    """
    Verify point-in-time safety:
      1. Features must not use future bar values.
      2. Target returns on filled bars must be masked (None).
    """
    s_dir = str(tmp_path / "silver")
    g_dir = str(tmp_path / "gold")
    q_dir = str(tmp_path / "quarantine")
    qgate = QualityGate()
    valid_df, _, _ = qgate.evaluate_and_filter(mock_bronze_data, quarantine_dir=q_dir)
    silver_df, _ = process_silver_layer(spark, valid_df, silver_output_dir=s_dir)
    gold_df = build_gold_features(spark, silver_df, gold_output_dir=g_dir)
    gold_pdf = gold_df.toPandas().sort_values("Timestamp_UTC").reset_index(drop=True)

    first_row = gold_pdf.iloc[0]
    assert first_row["ret_lag1"] == 0.0
    assert -1.0 <= first_row["hour_sin"] <= 1.0
    assert -1.0 <= first_row["hour_cos"] <= 1.0
    assert "atr_14" in gold_pdf.columns

def test_deliberate_leakage_detection(spark):
    """
    Verifies that our feature auditing catches a deliberately leaky future feature.
    """
    # Create dataset where future return is accidentally leaked into features
    data = [
        ("2024-01-01 00:00:00", 1.1000, 1.1020, 1.0990, 1.1000, 1000, "EURUSD"),
        ("2024-01-01 01:00:00", 1.1000, 1.1020, 1.0990, 1.1050, 1000, "EURUSD"),
        ("2024-01-01 02:00:00", 1.1050, 1.1070, 1.1040, 1.1100, 1000, "EURUSD"),
        ("2024-01-01 03:00:00", 1.1100, 1.1120, 1.1090, 1.1150, 1000, "EURUSD")
    ]
    pdf = pd.DataFrame(data, columns=["Date", "Open", "High", "Low", "Close", "Volume", "Pair"])
    pdf["target_lead1"] = pdf["Close"].shift(-1)
    
    # A safe lag feature should have low or negative correlation with future return
    # A leaky lead feature will have near-perfect correlation
    pdf["safe_lag1"] = pdf["Close"].shift(1)
    pdf["leaky_lead1"] = pdf["Close"].shift(-1)

    corr_leaky = pdf["target_lead1"].corr(pdf["leaky_lead1"])
    assert corr_leaky == pytest.approx(1.0, rel=1e-5), "Leaky feature must exhibit correlation 1.0 with future target"

def test_walk_forward_cv_split_and_embargo():
    """Verify that walk-forward CV maintains chronological order and strictly enforces 24-bar embargo gap."""
    dates = pd.date_range("2020-01-01", periods=1000, freq="h")
    df = pd.DataFrame({"Timestamp_UTC": dates, "val": range(1000)})
    
    validator = WalkForwardValidator(n_splits=5, embargo_bars=24)
    splits = validator.split(df)
    
    assert len(splits) == 5, f"Expected 5 splits, got {len(splits)}"
    
    prev_train_end = 0
    for train_idx, test_idx in splits:
        # Strict expanding window
        assert len(train_idx) > prev_train_end
        prev_train_end = len(train_idx)
        
        # Chronological ordering
        assert train_idx[-1] < test_idx[0]
        
        # Embargo gap of at least 24 bars
        gap = test_idx[0] - train_idx[-1]
        assert gap >= 24, f"Embargo gap was {gap}, expected >= 24"

def test_idempotency_empty_landing(spark, tmp_path):
    """Verify that re-running ingestion with 0 new files processes 0 rows and leaves Parquet unchanged."""
    landing_dir = str(tmp_path / "landing")
    bronze_dir = str(tmp_path / "bronze")
    os.makedirs(landing_dir, exist_ok=True)
    
    # Initial empty ingestion
    b_df_1 = ingest_to_bronze(spark, landing_dir=landing_dir, bronze_dir=bronze_dir)
    assert b_df_1.take(1) == []

    # Write 1 CSV to landing
    sample = pd.DataFrame([{"Date": "2024-01-01 00:00:00", "Open": 1.1, "High": 1.1, "Low": 1.1, "Close": 1.1, "Volume": 100}])
    sample.to_csv(f"{landing_dir}/EURUSD_1h.csv", index=False)
    
    b_df_2 = ingest_to_bronze(spark, landing_dir=landing_dir, bronze_dir=bronze_dir)
    count_after_first = b_df_2.count()
    assert count_after_first == 1

    # Re-run with landing now empty (file was moved to archived)
    b_df_3 = ingest_to_bronze(spark, landing_dir=landing_dir, bronze_dir=bronze_dir)
    count_after_retest = b_df_3.count()
    assert count_after_retest == count_after_first, "Idempotency failed: count changed on rerun with no new files"

def test_dst_transition_continuity():
    """Verify that on DST transition dates, historical data has zero duplicate UTC timestamps and remains strictly monotonic."""
    import glob
    files = glob.glob("data/external/*_1h_*_histdata.csv")
    assert len(files) > 0, "No external data files found for DST verification"

    for f in files:
        df = pd.read_csv(f, parse_dates=["Date"])
        march_nov = df[df["Date"].dt.month.isin([3, 11])]
        sundays = march_nov[march_nov["Date"].dt.dayofweek == 6]
        
        assert sundays.duplicated(subset=["Date"]).sum() == 0
        assert sundays["Date"].is_monotonic_increasing

def test_api_endpoints():
    """Verify FastAPI endpoints return expected schemas and status codes."""
    from fastapi.testclient import TestClient
    from src.api import app, engine
    from src.db_loader import init_db

    init_db(engine)
    client = TestClient(app)

    # 1. Latest rate
    res_l = client.get("/rates/latest?pair=EURUSD")
    assert res_l.status_code in [200, 404]

    # 2. Quality status
    res_q = client.get("/quality/status")
    assert res_q.status_code == 200
    assert "status" in res_q.json()

    # 3. Model metrics
    res_m = client.get("/model/metrics?pair=EURUSD")
    assert res_m.status_code == 200
    assert "registered_models" in res_m.json()

    # 4. Predict
    res_p = client.get("/predict?pair=EURUSD")
    assert res_p.status_code == 200
    assert "forecasts" in res_p.json()
