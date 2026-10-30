import os
import sys
from datetime import datetime, timedelta

# Ensure source modules are discoverable by Airflow workers
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def task_ingest(**context):
    from src.spark_session import get_spark_session
    from src.ingest import ingest_all_raw_data

    spark = get_spark_session("Airflow_Ingest")
    raw_dir = os.path.join(BASE_DIR, "data", "raw")
    df = ingest_all_raw_data(spark, raw_dir)
    count = df.count()
    context["ti"].xcom_push(key="raw_count", value=count)
    spark.stop()
    return f"Successfully ingested {count} rows"

def task_clean(**context):
    from src.spark_session import get_spark_session
    from src.ingest import ingest_all_raw_data
    from src.clean import clean_forex_data

    spark = get_spark_session("Airflow_Clean")
    raw_dir = os.path.join(BASE_DIR, "data", "raw")
    raw_df = ingest_all_raw_data(spark, raw_dir)
    cleaned_df = clean_forex_data(raw_df)
    count = cleaned_df.count()
    context["ti"].xcom_push(key="cleaned_count", value=count)
    spark.stop()
    return f"Successfully cleaned data, resulting in {count} records"

def task_validate(**context):
    from src.spark_session import get_spark_session
    from src.ingest import ingest_all_raw_data
    from src.clean import clean_forex_data
    from src.quality import DataQualityChecker

    spark = get_spark_session("Airflow_Validate")
    raw_dir = os.path.join(BASE_DIR, "data", "raw")
    raw_df = ingest_all_raw_data(spark, raw_dir)
    cleaned_df = clean_forex_data(raw_df)

    dq = DataQualityChecker(spark)
    dq.record_stage_count("airflow_cleaned_stage", cleaned_df.count())
    dq.check_duplicates(cleaned_df, ["Pair", "Date"], "airflow_cleaned_duplicates")
    dq.check_null_counts(cleaned_df, "airflow_cleaned_stage")

    report_path = os.path.join(BASE_DIR, "data_quality_report.json")
    dq.save_report(report_path)
    spark.stop()
    return f"Quality validation passed; report saved to {report_path}"

def task_features(**context):
    from src.spark_session import get_spark_session
    from src.ingest import ingest_all_raw_data
    from src.clean import clean_forex_data
    from src.features import build_features

    spark = get_spark_session("Airflow_Features")
    raw_dir = os.path.join(BASE_DIR, "data", "raw")
    raw_df = ingest_all_raw_data(spark, raw_dir)
    cleaned_df = clean_forex_data(raw_df)
    features_df = build_features(cleaned_df)
    count = features_df.count()
    context["ti"].xcom_push(key="features_count", value=count)
    spark.stop()
    return f"Engineered features computed for {count} rows"

def task_write_parquet(**context):
    from src.spark_session import get_spark_session
    from src.ingest import ingest_all_raw_data
    from src.clean import clean_forex_data
    from src.features import build_features
    from src.output import write_partitioned_parquet

    spark = get_spark_session("Airflow_WriteParquet")
    raw_dir = os.path.join(BASE_DIR, "data", "raw")
    out_dir = os.path.join(BASE_DIR, "data", "processed", "parquet")

    raw_df = ingest_all_raw_data(spark, raw_dir)
    cleaned_df = clean_forex_data(raw_df)
    features_df = build_features(cleaned_df)

    output_path = write_partitioned_parquet(features_df, out_dir)
    spark.stop()
    return f"Parquet dataset persisted to {output_path}"

with DAG(
    dag_id="forex_batch_pipeline",
    default_args=default_args,
    description="24-Hour Forex Batch Processing Pipeline (Ingest -> Clean -> Validate -> Features -> Parquet)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["forex", "pyspark", "bigdata", "data_engineering"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=task_ingest,
    )

    clean = PythonOperator(
        task_id="clean_and_deduplicate",
        python_callable=task_clean,
    )

    validate = PythonOperator(
        task_id="validate_data_quality",
        python_callable=task_validate,
    )

    features = PythonOperator(
        task_id="engineer_technical_features",
        python_callable=task_features,
    )

    write_parquet = PythonOperator(
        task_id="write_partitioned_parquet",
        python_callable=task_write_parquet,
    )

    # Linear Pipeline Execution Dependency
    ingest >> clean >> validate >> features >> write_parquet
