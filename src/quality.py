import os
import json
import logging
from typing import Dict, Any, List
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull

logger = logging.getLogger("DataQuality")

class DataQualityChecker:
    """
    Automated data quality verification engine:
      - Tracks row counts across pipeline stages
      - Detects duplicate records
      - Measures null and NaN counts per column
      - Validates expected schema and data types
      - Exports structured report to data_quality_report.json and logs with Python logging
    """

    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.report: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "pipeline_stages": {},
            "duplicate_checks": {},
            "null_counts": {},
            "schema_validation": {},
            "pairs_covered": [],
            "date_range_per_pair": {},
            "anomalies": {},
            "passed": True
        }

    def record_stage_count(self, stage_name: str, count_value: int) -> None:
        logger.info(f"[STAGE: {stage_name}] Row count = {count_value:,}")
        self.report["pipeline_stages"][stage_name] = count_value

    def record_profile_and_anomalies(self, df: DataFrame) -> Dict[str, Any]:
        from pyspark.sql.functions import min, max, dayofweek
        all_expected_15 = [
            "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
            "USDCHF", "NZDUSD", "EURGBP", "EURJPY", "GBPJPY",
            "AUDJPY", "EURAUD", "EURCHF", "GBPCHF", "NZDJPY"
        ]
        pairs = [r["Pair"] for r in df.select("Pair").distinct().collect()]
        self.report["pairs_covered"] = sorted(pairs)

        date_ranges = {}
        for r in df.groupBy("Pair").agg(min("Date").alias("min_d"), max("Date").alias("max_d")).collect():
            date_ranges[r["Pair"]] = {"earliest": str(r["min_d"]), "latest": str(r["max_d"])}
        self.report["date_range_per_pair"] = date_ranges

        zero_vol = df.filter(col("Volume") == 0).count()
        # dayofweek: Sunday=1, Saturday=7
        weekend_rows = df.filter(dayofweek(col("Timestamp_UTC")).isin([1, 7])).count()
        missing_pairs = [p for p in all_expected_15 if p not in pairs]

        self.report["anomalies"] = {
            "zero_volume_rows": zero_vol,
            "weekend_rows": weekend_rows,
            "missing_pairs": missing_pairs,
            "expected_pair_count": len(all_expected_15),
            "actual_pair_count": len(pairs)
        }
        logger.info(f"[ANOMALIES] Zero volume: {zero_vol}, Weekend rows: {weekend_rows}, Missing pairs: {missing_pairs}")
        return self.report["anomalies"]

    def check_duplicates(self, df: DataFrame, subset: List[str], check_name: str) -> int:
        total_rows = df.count()
        distinct_rows = df.dropDuplicates(subset).count()
        duplicate_count = total_rows - distinct_rows

        logger.info(
            f"[DUPLICATES: {check_name}] Total = {total_rows:,}, "
            f"Distinct = {distinct_rows:,}, Duplicates = {duplicate_count:,} on {subset}"
        )

        self.report["duplicate_checks"][check_name] = {
            "subset": subset,
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count
        }
        return duplicate_count

    def check_null_counts(self, df: DataFrame, stage_name: str) -> Dict[str, int]:
        exprs = []
        for c in df.columns:
            # Check for both null and NaN (for floating point columns)
            data_type = df.schema[c].dataType.simpleString()
            if data_type in ("double", "float"):
                expr = count(when(isnull(col(c)) | isnan(col(c)), c)).alias(c)
            else:
                expr = count(when(isnull(col(c)), c)).alias(c)
            exprs.append(expr)

        null_row = df.select(exprs).collect()[0]
        null_counts = {c: int(null_row[c]) for c in df.columns}

        logger.info(f"[NULL CHECK: {stage_name}] Column null breakdown: {null_counts}")
        self.report["null_counts"][stage_name] = null_counts
        return null_counts

    def validate_schema(self, df: DataFrame, expected_columns: List[str]) -> bool:
        actual_cols = set(df.columns)
        missing_cols = [c for c in expected_columns if c not in actual_cols]
        is_valid = len(missing_cols) == 0

        logger.info(
            f"[SCHEMA CHECK] Expected cols: {len(expected_columns)}, "
            f"Actual cols: {len(actual_cols)}, Missing: {missing_cols}"
        )

        self.report["schema_validation"] = {
            "expected_count": len(expected_columns),
            "actual_count": len(actual_cols),
            "missing_columns": missing_cols,
            "is_valid": is_valid
        }
        if not is_valid:
            self.report["passed"] = False
        return is_valid

    def save_report(self, output_path: str = "data_quality_report.json") -> str:
        abs_path = os.path.abspath(output_path)
        with open(abs_path, "w", encoding="utf-8") as f:
            json.dump(self.report, f, indent=2)
        logger.info(f"Data quality report saved to {abs_path}")
        return abs_path

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    from src.spark_session import get_spark_session
    from src.ingest import ingest_all_raw_data
    from src.clean import clean_forex_data
    from src.features import build_features

    spark = get_spark_session("QualityTest")
    dq = DataQualityChecker(spark)

    raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
    raw_df = ingest_all_raw_data(spark, raw_dir)
    dq.record_stage_count("raw_ingestion", raw_df.count())
    dq.check_duplicates(raw_df, ["Pair", "Date"], "raw_duplicates")
    dq.check_null_counts(raw_df, "raw_stage")

    cleaned_df = clean_forex_data(raw_df)
    dq.record_stage_count("cleaned_stage", cleaned_df.count())
    dq.check_null_counts(cleaned_df, "cleaned_stage")

    features_df = build_features(cleaned_df)
    dq.record_stage_count("features_stage", features_df.count())
    dq.check_null_counts(features_df, "features_stage")
    dq.validate_schema(features_df, ["Date", "Pair", "Close", "MA5", "MA20", "Volatility20", "Close_lag1"])

    report_path = os.path.join(os.path.dirname(__file__), "..", "data_quality_report.json")
    dq.save_report(report_path)
    spark.stop()
