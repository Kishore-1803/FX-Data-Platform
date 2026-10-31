"""
Apache Airflow DAG: 24-Hour Forex Data Platform Batch Pipeline
Lineage:
  replay >> bronze >> gate >> silver >> gold >> load >> score >> monitor
"""

import os
import sys
from datetime import datetime, timedelta

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "kishore_b",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

def task_replay(**context):
    from src.replay import HistoricalReplayDriver
    replay = HistoricalReplayDriver()
    released = replay.release_next_slice(slice_size_hours=168)
    msg = f"Released {len(released)} slice files to landing"
    context["ti"].xcom_push(key="released_files", value=released)
    return msg

def task_bronze(**context):
    from src.spark_session import get_spark_session
    from src.bronze import ingest_to_bronze
    spark = get_spark_session("Airflow_Bronze")
    df = ingest_to_bronze(spark)
    cnt = df.count()
    spark.stop()
    context["ti"].xcom_push(key="bronze_count", value=cnt)
    return f"Bronze layer count: {cnt}"

def task_gate(**context):
    from src.spark_session import get_spark_session
    from src.quality_gate import QualityGate
    spark = get_spark_session("Airflow_Gate")
    b_df = spark.read.parquet("data/bronze")
    qgate = QualityGate()
    v_df, q_df, metrics = qgate.evaluate_and_filter(b_df)
    v_cnt = v_df.count()
    q_cnt = q_df.count()
    spark.stop()
    context["ti"].xcom_push(key="valid_count", value=v_cnt)
    context["ti"].xcom_push(key="quarantine_count", value=q_cnt)
    return f"Gate passed: {v_cnt}, quarantined: {q_cnt}"

def task_silver(**context):
    from src.spark_session import get_spark_session
    from src.quality_gate import QualityGate
    from src.silver import process_silver_layer
    spark = get_spark_session("Airflow_Silver")
    b_df = spark.read.parquet("data/bronze")
    qgate = QualityGate()
    v_df, _, _ = qgate.evaluate_and_filter(b_df)
    s_df, fills = process_silver_layer(spark, v_df)
    cnt = s_df.count()
    spark.stop()
    context["ti"].xcom_push(key="silver_count", value=cnt)
    return f"Silver count: {cnt}, fills added: {len(fills)}"

def task_gold(**context):
    from src.spark_session import get_spark_session
    from src.gold import build_gold_features
    spark = get_spark_session("Airflow_Gold")
    s_df = spark.read.parquet("data/silver")
    g_df = build_gold_features(spark, s_df)
    cnt = g_df.count()
    spark.stop()
    context["ti"].xcom_push(key="gold_count", value=cnt)
    return f"Gold count: {cnt}"

def task_load(**context):
    from src.db_loader import (
        get_db_engine, init_db, load_silver_to_postgres,
        load_quarantine_to_postgres, load_fill_log_to_postgres
    )
    engine = get_db_engine()
    init_db(engine)
    curated_cnt = load_silver_to_postgres(engine)
    q_cnt = load_quarantine_to_postgres(engine)
    fill_cnt = load_fill_log_to_postgres(engine)
    return f"Loaded to DB: {curated_cnt} curated, {q_cnt} quarantined, {fill_cnt} fills"

def task_score(**context):
    from src.db_loader import get_db_engine
    from src.forecasting import batch_score_latest_bars
    import pandas as pd
    engine = get_db_engine()
    gold_df = pd.read_parquet("data/gold")
    pairs = gold_df["Pair"].unique()
    total_scored = 0
    for p in pairs:
        pair_gold = gold_df[gold_df["Pair"] == p].sort_values("Timestamp_UTC").reset_index(drop=True)
        if not pair_gold.empty:
            latest_bar = pair_gold.iloc[-1]
            scored = batch_score_latest_bars(engine, {}, p, latest_bar)
            total_scored += len(scored)
    return f"Scored {total_scored} forecast steps across {len(pairs)} pairs into predictions table."

def task_monitor(**context):
    from src.db_loader import get_db_engine
    from src.forecasting import run_monitoring_and_drift_detection, FEATURE_COLS
    import pandas as pd
    engine = get_db_engine()
    gold_df = pd.read_parquet("data/gold")
    pairs = gold_df["Pair"].unique()
    total_alerts = 0
    for p in pairs:
        pair_df = gold_df[gold_df["Pair"] == p].dropna(subset=FEATURE_COLS).sort_values("Timestamp_UTC").reset_index(drop=True)
        if len(pair_df) > 2000:
            ref_features = pair_df.iloc[:2000][FEATURE_COLS].values
            curr_features = pair_df.iloc[-500:][FEATURE_COLS].values
            alerts = run_monitoring_and_drift_detection(engine, p, ref_features, curr_features)
            total_alerts += len(alerts)
    return f"Monitoring completed: {total_alerts} drift alerts recorded to monitoring_alerts table."

with DAG(
    dag_id="forex_data_platform_dag",
    default_args=default_args,
    description="Full batch data platform pipeline (Replay -> Bronze -> Gate -> Silver -> Gold -> Load -> Score -> Monitor)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["forex", "pyspark", "data_platform"],
) as dag:

    replay = PythonOperator(task_id="replay", python_callable=task_replay)
    bronze = PythonOperator(task_id="bronze", python_callable=task_bronze)
    gate = PythonOperator(task_id="gate", python_callable=task_gate)
    silver = PythonOperator(task_id="silver", python_callable=task_silver)
    gold = PythonOperator(task_id="gold", python_callable=task_gold)
    load = PythonOperator(task_id="load", python_callable=task_load)
    score = PythonOperator(task_id="score", python_callable=task_score)
    monitor = PythonOperator(task_id="monitor", python_callable=task_monitor)

    # Strict Pipeline Lineage
    replay >> bronze >> gate >> silver >> gold >> load >> score >> monitor
