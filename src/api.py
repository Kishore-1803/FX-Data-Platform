"""
FastAPI Forex Platform Service:
Endpoints:
  - GET /rates/latest?pair=EURUSD
  - GET /rates/history?pair=EURUSD&limit=100
  - GET /quality/status
  - GET /predict?pair=EURUSD
  - GET /model/metrics?pair=EURUSD
"""

from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Query
import pandas as pd
from sqlalchemy import text
from src.db_loader import get_db_engine

app = FastAPI(
    title="Forex Data Platform API",
    description="REST API serving curated historical rates, data quality audits, and forecasts (Replay simulation)",
    version="1.0.0"
)

engine = get_db_engine()

@app.get("/rates/latest")
def get_latest_rate(pair: str = Query("EURUSD", description="Currency pair (e.g. EURUSD, GBPUSD)")):
    """Returns the most recent hourly rate for a specified currency pair."""
    query = text("""
        SELECT pair, timestamp_utc, open, high, low, close, volume, is_filled
        FROM curated_rates
        WHERE UPPER(pair) = UPPER(:pair)
        ORDER BY timestamp_utc DESC
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(query, {"pair": pair}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail=f"No data found for currency pair '{pair}'")
    return dict(row)

@app.get("/rates/history")
def get_historical_rates(
    pair: str = Query("EURUSD", description="Currency pair"),
    limit: int = Query(100, ge=1, le=1000, description="Number of bars to return")
):
    """Returns historical hourly bars for a pair."""
    query = text("""
        SELECT pair, timestamp_utc, open, high, low, close, volume, is_filled
        FROM curated_rates
        WHERE UPPER(pair) = UPPER(:pair)
        ORDER BY timestamp_utc DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"pair": pair, "limit": limit}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No history found for '{pair}'")
    return [dict(r) for r in rows]

@app.get("/quality/status")
def get_quality_status():
    """Returns recent quality gate run audit summary and quarantine counts."""
    with engine.connect() as conn:
        q_metrics = conn.execute(text("SELECT * FROM quality_results ORDER BY id DESC LIMIT 5")).mappings().all()
        quarantine_cnt = conn.execute(text("SELECT COUNT(*) FROM quarantine")).scalar() or 0
        fill_cnt = conn.execute(text("SELECT COUNT(*) FROM curated_rates WHERE is_filled = TRUE")).scalar() or 0
    return {
        "status": "HEALTHY",
        "recent_runs": [dict(m) for m in q_metrics],
        "total_quarantined_records": quarantine_cnt,
        "total_forward_filled_bars": fill_cnt
    }

@app.get("/predict")
def get_latest_prediction(pair: str = Query("EURUSD", description="Currency pair")):
    """Returns the latest 24-step multi-horizon forecast for the pair."""
    query = text("""
        SELECT pair, forecast_origin, target_horizon, target_timestamp,
               predicted_log_return, predicted_implied_price, predicted_direction, model_version
        FROM predictions
        WHERE UPPER(pair) = UPPER(:pair)
        ORDER BY forecast_origin DESC, target_horizon ASC
        LIMIT 24
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"pair": pair}).mappings().all()
    if not rows:
        return {
            "pair": pair,
            "message": "No model forecasts generated yet. Train models and run batch scoring.",
            "forecasts": []
        }
    return {
        "pair": pair,
        "forecast_origin": str(rows[0]["forecast_origin"]),
        "horizons_count": len(rows),
        "forecasts": [dict(r) for r in rows]
    }

@app.get("/model/metrics")
def get_model_metrics(pair: str = Query("EURUSD", description="Currency pair")):
    """Returns model registry evaluation metrics across horizons."""
    query = text("""
        SELECT model_version, pair, model_type, horizon_hours, mae, rmse,
               directional_accuracy, pct_improvement_over_naive, data_range_start, data_range_end
        FROM model_registry
        WHERE UPPER(pair) = UPPER(:pair)
        ORDER BY horizon_hours ASC
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"pair": pair}).mappings().all()
    return {
        "pair": pair,
        "registered_models": [dict(r) for r in rows]
    }

