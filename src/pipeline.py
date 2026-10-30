import os
import sys
import logging
from src.spark_session import get_spark_session
from src.ingest import ingest_all_raw_data
from src.clean import clean_forex_data
from src.features import build_features
from src.quality import DataQualityChecker
from src.output import write_partitioned_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ForexPipelineOrchestrator")

def run_pipeline(
    raw_dir: str = "data/raw",
    output_parquet_dir: str = "data/processed/parquet",
    quality_report_path: str = "data_quality_report.json",
    spark=None
):
    import time
    logger.info("=" * 70)
    logger.info("STARTING END-TO-END FOREX BATCH ETL & FEATURE ENGINEERING PIPELINE")
    logger.info("=" * 70)

    owns_spark = False
    if spark is None:
        spark = get_spark_session("ForexBatchPipelineProduction")
        owns_spark = True

    timings = {}
    total_start = time.perf_counter()

    dq = DataQualityChecker(spark)

    # 1. Ingestion
    t0 = time.perf_counter()
    logger.info("Phase 1: Ingestion with explicit schema")
    raw_df = ingest_all_raw_data(spark, raw_dir)
    raw_count = raw_df.count()
    timings["ingest"] = time.perf_counter() - t0
    dq.record_stage_count("raw_ingestion", raw_count)

    # 2. Cleaning & Imputation
    t0 = time.perf_counter()
    logger.info("Phase 2: Data cleaning (deduplication, UTC timezone conversion, partitioned forward-fill)")
    cleaned_df = clean_forex_data(raw_df)
    cleaned_count = cleaned_df.count()
    timings["clean"] = time.perf_counter() - t0
    dq.record_stage_count("cleaned_stage", cleaned_count)

    # 3. Feature Engineering
    t0 = time.perf_counter()
    logger.info("Phase 3: Technical indicator and lag feature engineering (partitionBy('Pair').orderBy('Date'))")
    features_df = build_features(cleaned_df)
    features_count = features_df.count()
    timings["features"] = time.perf_counter() - t0
    dq.record_stage_count("features_stage", features_count)

    # 4. Data Quality & Profiling
    t0 = time.perf_counter()
    logger.info("Phase 4: Data Quality Checks, Schema Validation, and Anomaly Detection")
    dq.check_duplicates(raw_df, ["Pair", "Date"], "raw_duplicates")
    dq.check_null_counts(raw_df, "raw_stage")
    dq.check_null_counts(cleaned_df, "cleaned_stage")
    dq.check_null_counts(features_df, "features_stage")
    dq.validate_schema(features_df, [
        "Date", "Pair", "Timeframe", "Timestamp_UTC", "Year",
        "Open", "High", "Low", "Close", "Volume",
        "Range", "Change", "ChangePct",
        "MA5", "MA20", "Volatility20",
        "Close_lag1", "Close_lag2", "Close_lag3",
        "Change_lag1", "Change_lag2", "MA5_MA20_Ratio", "Gain", "Loss"
    ])
    dq.record_profile_and_anomalies(features_df)
    dq_path = dq.save_report(quality_report_path)
    timings["quality"] = time.perf_counter() - t0
    logger.info(f"Data quality report saved to {dq_path}")

    # 5. Parquet Output
    t0 = time.perf_counter()
    logger.info("Phase 5: Writing snappy Parquet partitioned by Pair and Year")
    out_path = write_partitioned_parquet(features_df, output_parquet_dir)
    timings["write_parquet"] = time.perf_counter() - t0

    timings["total"] = time.perf_counter() - total_start

    logger.info("=" * 70)
    logger.info(f"PIPELINE COMPLETED in {timings['total']:.2f}s: {features_count:,} records processed and written to {out_path}")
    logger.info(f"Stage breakdown: {timings}")
    logger.info("=" * 70)

    if owns_spark:
        spark.stop()

    return {
        "timings": timings,
        "counts": {
            "raw": raw_count,
            "cleaned": cleaned_count,
            "features": features_count,
        },
        "output_path": out_path,
        "quality_report_path": dq_path
    }

if __name__ == "__main__":
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    raw_data = os.path.join(base_dir, "data", "raw")
    parquet_data = os.path.join(base_dir, "data", "processed", "parquet")
    report_file = os.path.join(base_dir, "data_quality_report.json")
    run_pipeline(raw_data, parquet_data, report_file)
