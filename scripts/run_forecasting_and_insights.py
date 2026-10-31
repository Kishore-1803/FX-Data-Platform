"""
Full Forecasting Evaluation & Insights Script:
Executes:
  1. Target Masking Audit (counts dropped/masked targets due to forward fills)
  2. 5-Fold Walk-Forward Cross-Validation (Direct Ridge, Direct Tree, Recursive, Baselines)
  3. Walk-Forward with vs without 2023
  4. Per-Pair vs Pooled Model
  5. Statistical Tests (Binomial test on Directional Accuracy, Paired test vs baseline)
  6. Phase 3 Data Insights (Hour-of-day, Day-of-week, Volatility Clustering, Macro events)
  7. Registry & Scored Predictions persistence to PostgreSQL
"""

import os
import sys
import json
import logging
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.linear_model import Ridge
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.abspath("."))
from src.db_loader import get_db_engine, init_db
from src.forecasting import (
    FEATURE_COLS,
    WalkForwardValidator,
    compute_metrics,
    train_and_evaluate_direct,
    train_and_evaluate_recursive,
    save_to_model_registry,
    batch_score_latest_bars,
    run_monitoring_and_drift_detection
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ForecastingInsights")

def main():
    print("================================================================")
    print("PHASE 2: FORECASTING EXPERIMENTS (Real Data, 6 Pairs, 2017-2024)")
    print("================================================================")

    # 1. Load Gold Data
    gold_path = "data/gold"
    if not os.path.exists(gold_path):
        print(f"Error: {gold_path} not found.")
        return

    print("Loading Gold layer dataset...")
    df_gold = pd.read_parquet(gold_path)
    df_gold["Timestamp_UTC"] = pd.to_datetime(df_gold["Timestamp_UTC"])
    pairs = sorted(df_gold["Pair"].unique())
    print(f"Loaded {len(df_gold):,} total Gold rows across {len(pairs)} pairs: {pairs}")

    # 2. Target Masking Audit
    print("\n--- 1. TARGET MASKING AUDIT ---")
    total_targets_evaluated = 0
    total_targets_masked = 0
    for h in [1, 6, 12, 24]:
        col_name = f"target_r_{h}"
        if col_name in df_gold.columns:
            tot = len(df_gold)
            valid = df_gold[col_name].notnull().sum()
            masked = tot - valid
            total_targets_evaluated += tot
            total_targets_masked += masked
            print(f"Horizon h={h:02d}: Total={tot:,}, Valid={valid:,}, Masked/Dropped={masked:,} ({masked/tot*100:.2f}%)")
    print(f"Overall Target Masking: {total_targets_masked:,} / {total_targets_evaluated:,} targets dropped ({total_targets_masked/total_targets_evaluated*100:.2f}%)")

    # Connect to DB
    engine = get_db_engine()
    init_db(engine)

    # 3. Direct Multi-Horizon Walk-Forward Evaluation per Pair
    print("\n--- 2. WALK-FORWARD 5-FOLD EVALUATION (DIRECT RIDGE & TREE) ---")
    all_registry_records = []
    pair_horizon_metrics = {}

    for p in pairs:
        print(f"\n>> Evaluating Pair: {p} (Full 2017-2024)")
        p_df = df_gold[df_gold["Pair"] == p].sort_values("Timestamp_UTC").reset_index(drop=True)
        h_results, trained_models = train_and_evaluate_direct(p_df, p, horizons=list(range(1, 25)))
        pair_horizon_metrics[p] = h_results

        for h, res in h_results.items():
            r = res["ridge"]
            t = res["tree"]
            d = res["drift_baseline"]
            print(f"  h={h:02d} | Baseline MAE: {r['mae_baseline']:.6f} | Ridge MAE: {r['mae']:.6f} ({r['pct_improvement']:+.2f}%) DA: {r['directional_accuracy']:.1f}% (p={r['p_val_directional']:.4f}, paired_p={r['p_val_paired']:.4f}) | Tree MAE: {t['mae']:.6f} ({t['pct_improvement']:+.2f}%) DA: {t['directional_accuracy']:.1f}%")

            # Prepare registry record
            all_registry_records.append({
                "model_version": f"{p}_ridge_h{h}_v1",
                "pair": p,
                "model_type": "Direct_Ridge",
                "horizon_hours": h,
                "mae": r["mae"],
                "rmse": r["rmse"],
                "directional_accuracy": r["directional_accuracy"],
                "pct_improvement_over_naive": r["pct_improvement"],
                "data_range_start": p_df["Timestamp_UTC"].min(),
                "data_range_end": p_df["Timestamp_UTC"].max(),
                "features_list": ",".join(FEATURE_COLS)
            })

        # Save registry records first so foreign key is satisfied
        save_to_model_registry(engine, all_registry_records)
        all_registry_records = []

        # Batch score latest bar for this pair
        latest_bar = p_df.iloc[-1]
        batch_score_latest_bars(engine, trained_models, p, latest_bar)

    # 4. Walk-Forward With 2023 vs Without 2023 (EURUSD Benchmark)
    print("\n--- 3. ABLATION: WITH 2023 VS WITHOUT 2023 (EURUSD) ---")
    eur_df = df_gold[df_gold["Pair"] == "EURUSD"].sort_values("Timestamp_UTC").reset_index(drop=True)
    eur_with_2023, _ = train_and_evaluate_direct(eur_df, "EURUSD", horizons=[1, 6, 12, 24], exclude_year=None)
    eur_without_2023, _ = train_and_evaluate_direct(eur_df, "EURUSD", horizons=[1, 6, 12, 24], exclude_year=2023)

    print("Horizon | With 2023 MAE (Ridge) | Without 2023 MAE (Ridge) | Diff (%) | With 2023 DA | Without 2023 DA")
    print("--------|-----------------------|--------------------------|----------|--------------|----------------")
    for h in [1, 6, 12, 24]:
        w = eur_with_2023[h]["ridge"]
        wo = eur_without_2023[h]["ridge"]
        diff = ((w["mae"] - wo["mae"]) / w["mae"]) * 100.0
        print(f"  h={h:02d}   |       {w['mae']:.6f}        |         {wo['mae']:.6f}         |  {diff:+.2f}%  |    {w['directional_accuracy']:.1f}%    |     {wo['directional_accuracy']:.1f}%")

    # 5. Direct vs Recursive 24-step Comparison
    print("\n--- 4. DIRECT VS RECURSIVE MULTI-HORIZON COMPARISON (EURUSD) ---")
    rec_results = train_and_evaluate_recursive(eur_df, max_steps=24)
    print("Horizon | Direct Ridge MAE | Recursive Rollout MAE | Error Accumulation Penalty (%)")
    print("--------|------------------|-----------------------|-------------------------------")
    for item in rec_results["recursive_rollout_metrics"]:
        h = item["horizon"]
        d_mae = eur_with_2023[h]["ridge"]["mae"]
        r_mae = item["recursive_mae"]
        penalty = ((r_mae - d_mae) / d_mae) * 100.0
        print(f"  h={h:02d}   |     {d_mae:.6f}     |       {r_mae:.6f}        |           {penalty:+.2f}%")

    # 6. Pooled Multi-Pair Model vs Per-Pair Models
    print("\n--- 5. POOLED MULTI-PAIR MODEL VS PER-PAIR MODELS ---")
    # Build pooled dataset with one-hot pair indicator
    pooled_df = df_gold.dropna(subset=FEATURE_COLS + ["target_r_1"]).copy()
    pair_dummies = pd.get_dummies(pooled_df["Pair"], prefix="pair", drop_first=True)
    pooled_features = FEATURE_COLS + list(pair_dummies.columns)
    pooled_df = pd.concat([pooled_df, pair_dummies], axis=1)

    train_pooled = pooled_df[pooled_df["Timestamp_UTC"].dt.year <= 2022]
    test_pooled = pooled_df[pooled_df["Timestamp_UTC"].dt.year >= 2024]

    scaler_p = StandardScaler()
    X_tr_p = scaler_p.fit_transform(train_pooled[pooled_features].values)
    X_te_p = scaler_p.transform(test_pooled[pooled_features].values)
    y_tr_p = train_pooled["target_r_1"].values
    y_te_p = test_pooled["target_r_1"].values

    ridge_pooled = Ridge(alpha=10.0)
    ridge_pooled.fit(X_tr_p, y_tr_p)
    pred_pooled = ridge_pooled.predict(X_te_p)
    m_pooled = compute_metrics(y_te_p, pred_pooled, np.zeros_like(y_te_p))
    print(f"Pooled Model (6 pairs, h=1): MAE={m_pooled['mae']:.6f}, DA={m_pooled['directional_accuracy']:.1f}%, Improvement={m_pooled['pct_improvement']:+.2f}%")

    # Save to model registry
    save_to_model_registry(engine, all_registry_records)

    # 7. Run Monitoring & Feature Drift on Postgres
    print("\n--- 6. FEATURE DRIFT & RESIDUAL MONITORING ---")
    total_alerts = 0
    for p in pairs:
        p_df = df_gold[df_gold["Pair"] == p].dropna(subset=FEATURE_COLS).sort_values("Timestamp_UTC").reset_index(drop=True)
        if len(p_df) > 3000:
            ref_f = p_df.iloc[:2000][FEATURE_COLS].values
            curr_f = p_df.iloc[-500:][FEATURE_COLS].values
            alerts = run_monitoring_and_drift_detection(engine, p, ref_f, curr_f)
            total_alerts += len(alerts)
            if alerts:
                print(f"Pair {p}: {len(alerts)} drift alerts generated (e.g. {alerts[0]['metric_name']} stat={alerts[0]['metric_value']})")
    print(f"Total Drift & Quality Alerts Recorded to PostgreSQL: {total_alerts}")

    # ================================================================
    # PHASE 3: DATA INSIGHTS
    # ================================================================
    print("\n================================================================")
    print("PHASE 3: DATA INSIGHTS (Aggregated from Gold Layer)")
    print("================================================================")

    # 1. Volatility by Hour of Day (UTC)
    print("\n--- A. VOLATILITY BY HOUR OF DAY (UTC) ---")
    df_gold["hour"] = df_gold["Timestamp_UTC"].dt.hour
    df_gold["dow"] = df_gold["Timestamp_UTC"].dt.dayofweek
    df_gold["abs_ret"] = df_gold["ret_lag1"].abs()

    hourly_vol = df_gold.groupby("hour")["abs_ret"].mean().reset_index()
    print("Hour (UTC) | Mean Absolute Return | Market Session Activity")
    print("-----------|----------------------|------------------------")
    for _, r in hourly_vol.iterrows():
        hr = int(r["hour"])
        session = ""
        if 8 <= hr <= 11:
            session = "London Morning"
        elif 12 <= hr <= 16:
            session = "London / NY Overlap (Peak)"
        elif 17 <= hr <= 21:
            session = "NY Afternoon"
        elif 0 <= hr <= 7:
            session = "Asian Session"
        print(f"    {hr:02d}:00  |       {r['abs_ret']:.6f}       | {session}")

    # 2. Weekday Effect
    print("\n--- B. VOLATILITY BY WEEKDAY ---")
    dow_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dow_vol = df_gold.groupby("dow")["abs_ret"].mean().reset_index()
    for _, r in dow_vol.iterrows():
        d = int(r["dow"])
        if d < 5:
            print(f"  {dow_names[d]:<10}: Mean Abs Return = {r['abs_ret']:.6f}")

    # 3. Volatility Clustering (Autocorrelation of |Return|)
    print("\n--- C. VOLATILITY CLUSTERING (Autocorrelation of |Return|) ---")
    eur_series = df_gold[df_gold["Pair"] == "EURUSD"]["abs_ret"].dropna()
    print("Autocorrelation of absolute returns (|r_t|) vs raw returns (r_t):")
    for lag in [1, 2, 3, 5, 10, 24]:
        ac_abs = eur_series.autocorr(lag=lag)
        ac_raw = df_gold[df_gold["Pair"] == "EURUSD"]["ret_lag1"].autocorr(lag=lag)
        print(f"  Lag {lag:02d}h: Autocorr(|r|) = {ac_abs:+.4f} | Autocorr(raw r) = {ac_raw:+.4f}")

    # Save summary report
    print("\nForecasting & Insights pipeline complete. All tables updated in PostgreSQL.")

if __name__ == "__main__":
    main()

