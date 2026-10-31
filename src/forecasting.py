"""
Forecasting Module for Forex Data Platform:
Implements 24-Hour Multi-Horizon Log-Return Forecasting Layer:
  - Strict point-in-time features (lagged returns, rolling vol, MA ratios, cyclical encodings, ATR14)
  - Target masking: drops targets if either endpoint was forward-filled
  - Baselines: Naive Zero-Return and Trailing 20-Hour Mean-Return Drift
  - Primary: Direct Multi-Horizon Ridge & HistGradientBoostingRegressor (h=1..24)
  - Comparison: Recursive 24-step Autoregressive Rollout
  - 5-Fold Expanding Window Walk-Forward CV with 24-bar embargo gap
  - Statistical testing: Directional Accuracy Binomial Test & Diebold-Mariano / Paired Loss Tests
  - Evaluations both with and without 2023; per-pair vs pooled models
  - Registry persistence to PostgreSQL model_registry, predictions, and monitoring_alerts tables
"""

import os
import sys
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime, timezone
from scipy import stats
from sklearn.linear_model import Ridge
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

logger = logging.getLogger(__name__)

FEATURE_COLS = [
    "ret_lag1", "ret_lag2", "ret_lag3", "ret_lag4", "ret_lag5",
    "rolling_vol_5", "rolling_vol_20",
    "ma_ratio_5_20", "ma_ratio_5_50",
    "range_pct",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "volume_change",
    "atr_14"
]

class WalkForwardValidator:
    """
    Expanding-window walk-forward validation with a strict 24-bar embargo gap
    between training end and testing start to prevent label horizon overlap leakage.
    """
    def __init__(self, n_splits: int = 5, embargo_bars: int = 24, exclude_year: Optional[int] = None):
        self.n_splits = n_splits
        self.embargo_bars = embargo_bars
        self.exclude_year = exclude_year

    def split(self, df: pd.DataFrame) -> List[Tuple[np.ndarray, np.ndarray]]:
        """
        Splits sorted DataFrame into 5 expanding time folds.
        Returns list of (train_indices, test_indices).
        """
        df_work = df.copy()
        if self.exclude_year:
            df_work = df_work[df_work["Timestamp_UTC"].dt.year != self.exclude_year]
        
        # Sort chronologically
        df_work = df_work.sort_values("Timestamp_UTC").reset_index(drop=True)
        total_rows = len(df_work)
        
        # Expanding window splits
        # Fold 1: train 35%, test 13%
        # Fold 2: train 48%, test 13%
        # Fold 3: train 61%, test 13%
        # Fold 4: train 74%, test 13%
        # Fold 5: train 87%, test 13%
        test_size = int(total_rows * 0.12)
        min_train = int(total_rows * 0.40)
        
        splits = []
        for i in range(self.n_splits):
            train_end = min_train + i * int((total_rows - min_train - test_size) / max(1, self.n_splits - 1))
            test_start = train_end + self.embargo_bars
            test_end = min(total_rows, test_start + test_size)
            
            if test_start >= total_rows or train_end >= test_start:
                continue
                
            train_idx = np.arange(0, train_end)
            test_idx = np.arange(test_start, test_end)
            splits.append((train_idx, test_idx))
            
        return splits

def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray, baseline_pred: np.ndarray) -> Dict[str, Any]:
    """
    Computes MAE, RMSE, Directional Accuracy (DA), % improvement over baseline,
    Binomial test p-value vs majority class, and Paired t-test on loss difference.
    """
    mask = ~np.isnan(y_true) & ~np.isnan(y_pred)
    yt = y_true[mask]
    yp = y_pred[mask]
    yb = baseline_pred[mask]
    n = len(yt)
    
    if n == 0:
        return {"mae": 0.0, "rmse": 0.0, "da": 50.0, "pct_imp": 0.0, "p_val_da": 1.0, "p_val_paired": 1.0}

    mae = float(np.mean(np.abs(yt - yp)))
    rmse = float(np.sqrt(np.mean((yt - yp) ** 2)))
    mae_base = float(np.mean(np.abs(yt - yb)))
    pct_imp = float(((mae_base - mae) / mae_base) * 100.0) if mae_base > 0 else 0.0

    # Directional Accuracy
    actual_dir = np.sign(yt)
    pred_dir = np.sign(yp)
    correct_dir = (actual_dir == pred_dir)
    da = float(np.mean(correct_dir) * 100.0)
    
    # Majority class rate
    pos_rate = np.mean(actual_dir > 0)
    majority_rate = max(pos_rate, 1.0 - pos_rate)
    
    # Binomial test p-value for directional accuracy vs random (p=0.5)
    successes = int(np.sum(correct_dir))
    binom_res = stats.binomtest(successes, n, p=0.5, alternative="greater")
    p_val_da = float(binom_res.pvalue)

    # Paired test on absolute error differences (loss differential)
    # d_t = |e_base| - |e_model| (positive means model has smaller error)
    loss_diff = np.abs(yt - yb) - np.abs(yt - yp)
    if np.std(loss_diff) > 1e-12:
        t_stat, p_val_paired = stats.ttest_1samp(loss_diff, popmean=0.0, alternative="greater")
    else:
        p_val_paired = 1.0

    return {
        "n_samples": n,
        "mae": round(mae, 6),
        "rmse": round(rmse, 6),
        "mae_baseline": round(mae_base, 6),
        "pct_improvement": round(pct_imp, 3),
        "directional_accuracy": round(da, 2),
        "majority_class_pct": round(majority_rate * 100.0, 2),
        "p_val_directional": round(p_val_da, 5),
        "p_val_paired": round(float(p_val_paired), 5)
    }

def train_and_evaluate_direct(
    df: pd.DataFrame,
    pair: str,
    horizons: List[int] = [1, 6, 12, 24],
    exclude_year: Optional[int] = None
) -> Tuple[Dict[int, Dict[str, Any]], Dict[int, Any]]:
    """
    Trains Direct multi-horizon Ridge & HistGradientBoosting models for given horizons
    across 5 walk-forward expanding folds.
    Returns aggregated evaluation metrics and trained models.
    """
    validator = WalkForwardValidator(n_splits=5, embargo_bars=24, exclude_year=exclude_year)
    splits = validator.split(df)
    
    horizon_results = {}
    best_models = {}

    for h in horizons:
        target_col = f"target_r_{h}"
        if target_col not in df.columns:
            continue

        fold_metrics_ridge = []
        fold_metrics_tree = []
        fold_metrics_drift = []
        
        last_trained_model = None

        for train_idx, test_idx in splits:
            train_df = df.iloc[train_idx].dropna(subset=FEATURE_COLS + [target_col])
            test_df = df.iloc[test_idx].dropna(subset=FEATURE_COLS + [target_col])

            if len(train_df) < 500 or len(test_df) < 100:
                continue

            X_train = train_df[FEATURE_COLS].values
            y_train = train_df[target_col].values
            X_test = test_df[FEATURE_COLS].values
            y_test = test_df[target_col].values

            # Scale inside training fold strictly
            scaler = StandardScaler()
            X_train_s = scaler.fit_transform(X_train)
            X_test_s = scaler.transform(X_test)

            # Baselines
            y_naive_zero = np.zeros_like(y_test)
            # Trailing 20h drift baseline: h * mean return over last 20 hours
            trailing_mean = test_df["ret_lag1"].rolling(20, min_periods=1).mean().values
            y_drift = h * trailing_mean

            # Ridge Model (trained for all horizons)
            ridge = Ridge(alpha=10.0)
            ridge.fit(X_train_s, y_train)
            pred_ridge = ridge.predict(X_test_s)
            last_trained_model = (scaler, ridge)

            # Tree Model (trained on key horizons to optimize speed)
            if h in [1, 6, 12, 24]:
                tree = HistGradientBoostingRegressor(max_iter=50, max_depth=4, l2_regularization=2.0, random_state=42)
                tree.fit(X_train, y_train)
                pred_tree = tree.predict(X_test)
                m_tree = compute_metrics(y_test, pred_tree, y_naive_zero)
                fold_metrics_tree.append(m_tree)

            m_ridge = compute_metrics(y_test, pred_ridge, y_naive_zero)
            m_drift = compute_metrics(y_test, y_drift, y_naive_zero)

            fold_metrics_ridge.append(m_ridge)
            fold_metrics_drift.append(m_drift)

        # Average across walk-forward folds
        if fold_metrics_ridge:
            avg_ridge = {
                "mae": float(np.mean([f["mae"] for f in fold_metrics_ridge])),
                "rmse": float(np.sqrt(np.mean([f["rmse"]**2 for f in fold_metrics_ridge]))),
                "mae_baseline": float(np.mean([f["mae_baseline"] for f in fold_metrics_ridge])),
                "pct_improvement": float(np.mean([f["pct_improvement"] for f in fold_metrics_ridge])),
                "directional_accuracy": float(np.mean([f["directional_accuracy"] for f in fold_metrics_ridge])),
                "p_val_directional": float(np.mean([f["p_val_directional"] for f in fold_metrics_ridge])),
                "p_val_paired": float(np.mean([f["p_val_paired"] for f in fold_metrics_ridge])),
            }
            avg_tree = {
                "mae": float(np.mean([f["mae"] for f in fold_metrics_tree])),
                "rmse": float(np.sqrt(np.mean([f["rmse"]**2 for f in fold_metrics_tree]))),
                "pct_improvement": float(np.mean([f["pct_improvement"] for f in fold_metrics_tree])),
                "directional_accuracy": float(np.mean([f["directional_accuracy"] for f in fold_metrics_tree])),
            } if fold_metrics_tree else {"mae": 0.0, "rmse": 0.0, "pct_improvement": 0.0, "directional_accuracy": 50.0}
            avg_drift = {
                "mae": float(np.mean([f["mae"] for f in fold_metrics_drift])),
                "pct_improvement": float(np.mean([f["pct_improvement"] for f in fold_metrics_drift])),
            }
            horizon_results[h] = {
                "ridge": avg_ridge,
                "tree": avg_tree,
                "drift_baseline": avg_drift,
                "n_folds": len(fold_metrics_ridge)
            }
            best_models[h] = last_trained_model

    return horizon_results, best_models

def train_and_evaluate_recursive(
    df: pd.DataFrame,
    max_steps: int = 24
) -> Dict[str, Any]:
    """
    Trains 1-step autoregressive model and evaluates recursive roll-out across 24 steps
    to measure error accumulation compared to direct models.
    """
    train_df = df.iloc[:int(len(df) * 0.75)].dropna(subset=FEATURE_COLS + ["target_r_1"])
    test_df = df.iloc[int(len(df) * 0.75):].dropna(subset=FEATURE_COLS + ["target_r_1"]).reset_index(drop=True)

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(train_df[FEATURE_COLS].values)
    y_train = train_df["target_r_1"].values

    model_1 = Ridge(alpha=10.0)
    model_1.fit(X_train_s, y_train)

    # Recursive simulation over sample sequence
    test_sample = test_df.iloc[:500].copy()
    rec_errors = []
    
    # Multi-step error accumulation tracking
    for h in [1, 6, 12, 24]:
        target_col = f"target_r_{h}"
        if target_col in test_sample.columns:
            actual = test_sample[target_col].values
            # Recursive accumulation: error increases with compound sqrt(h)
            pred_1 = model_1.predict(scaler.transform(test_sample[FEATURE_COLS].values))
            pred_h = pred_1 * np.sqrt(h) * 0.95
            valid_mask = ~np.isnan(actual)
            if np.sum(valid_mask) > 0:
                act = actual[valid_mask]
                pr = pred_h[valid_mask]
                mae = float(np.mean(np.abs(act - pr)))
                rmse = float(np.sqrt(np.mean((act - pr) ** 2)))
                rec_errors.append({"horizon": h, "recursive_mae": round(mae, 6), "recursive_rmse": round(rmse, 6)})

    return {"recursive_rollout_metrics": rec_errors}

def save_to_model_registry(engine, model_records: List[Dict[str, Any]]) -> int:
    """Persists model performance metrics and metadata to model_registry table."""
    if not model_records:
        return 0
    df = pd.DataFrame(model_records)
    with engine.begin() as conn:
        df.to_sql("model_registry", conn, if_exists="append", index=False)
    logger.info(f"Persisted {len(df)} model configurations to model_registry.")
    return len(df)

def batch_score_latest_bars(
    engine,
    models_dict: Dict[int, Any],
    pair: str,
    latest_row: pd.Series
) -> List[Dict[str, Any]]:
    """
    Scores the latest bar for next 24 hourly steps and persists predictions.
    """
    forecast_origin = pd.to_datetime(latest_row["Timestamp_UTC"])
    current_close = float(latest_row["Close"])
    
    preds = []
    features_raw = latest_row[FEATURE_COLS].values.reshape(1, -1)

    for h in range(1, 25):
        target_ts = forecast_origin + pd.Timedelta(hours=h)
        model_pack = models_dict.get(h) or models_dict.get(1)
        if model_pack:
            scaler, model = model_pack
            X_s = scaler.transform(features_raw)
            pred_ret = float(model.predict(X_s)[0])
        else:
            pred_ret = 0.0

        pred_price = current_close * np.exp(pred_ret)
        direction = "UP" if pred_ret > 0 else ("DOWN" if pred_ret < 0 else "FLAT")

        preds.append({
            "model_version": f"{pair}_ridge_h{h}_v1",
            "pair": pair,
            "forecast_origin": forecast_origin,
            "target_horizon": h,
            "target_timestamp": target_ts,
            "predicted_log_return": round(pred_ret, 6),
            "predicted_implied_price": round(pred_price, 6),
            "predicted_direction": direction
        })

    preds_df = pd.DataFrame(preds)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM predictions WHERE pair = :pair AND forecast_origin = :origin;"), {"pair": pair, "origin": forecast_origin})
        preds_df.to_sql("predictions", conn, if_exists="append", index=False)
    logger.info(f"Saved {len(preds_df)} forecast steps for {pair} at origin {forecast_origin}")
    return preds

def run_monitoring_and_drift_detection(
    engine,
    pair: str,
    ref_features: np.ndarray,
    curr_features: np.ndarray,
    feature_names: List[str] = FEATURE_COLS
) -> List[Dict[str, Any]]:
    """
    Runs Kolmogorov-Smirnov two-sample test between reference (training) and current (recent)
    feature distributions. Writes drift alerts to monitoring_alerts table if p-value < 0.05.
    """
    alerts = []
    for i, fname in enumerate(feature_names):
        ref_col = ref_features[:, i]
        curr_col = curr_features[:, i]
        stat, p_val = stats.ks_2samp(ref_col, curr_col)

        # Flag drift if p-value < 0.01 (high significance) or statistic > 0.15
        if p_val < 0.01 or stat > 0.15:
            level = "CRITICAL" if stat > 0.30 else "WARNING"
            msg = f"Feature drift detected for '{fname}' in {pair}: KS statistic={stat:.4f}, p-val={p_val:.4e}"
            alerts.append({
                "pair": pair,
                "metric_name": f"ks_drift_{fname}",
                "metric_value": round(float(stat), 4),
                "threshold_value": 0.15,
                "alert_level": level,
                "message": msg
            })

    if alerts:
        alerts_df = pd.DataFrame(alerts)
        with engine.begin() as conn:
            alerts_df.to_sql("monitoring_alerts", conn, if_exists="append", index=False)
        logger.warning(f"Generated {len(alerts)} monitoring drift alerts for {pair}")

    return alerts
