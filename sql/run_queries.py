import os
import sys
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, base_dir)
import logging
from pyspark.sql import SparkSession
from src.spark_session import get_spark_session
from src.ingest import ingest_all_raw_data
from src.clean import clean_forex_data
from src.features import build_features

logger = logging.getLogger("SparkSQLRunner")

SQL_FILES = [
    "monthly_volatility.sql",
    "top_volatility_days.sql",
    "volume_aggregates.sql",
    "row_count_reconciliation.sql"
]

def run_all_queries(spark: SparkSession, sql_dir: str):
    import time
    parquet_dir = os.path.abspath(os.path.join(sql_dir, "..", "data", "processed", "parquet"))
    logger.info(f"Loading full Parquet output from {parquet_dir}...")
    parquet_df = spark.read.parquet(parquet_dir)

    # Register temporary view for Spark SQL execution
    parquet_df.createOrReplaceTempView("forex_cleaned")
    logger.info("Temporary view 'forex_cleaned' registered on Parquet output.")

    results = {}
    for sql_file in SQL_FILES:
        file_path = os.path.join(sql_dir, sql_file)
        logger.info(f"\n{'='*70}\nExecuting SQL: {sql_file}\n{'='*70}")
        with open(file_path, "r", encoding="utf-8") as f:
            query = f.read()

        clean_query = "\n".join([line for line in query.splitlines() if not line.strip().startswith("--")])
        
        t0 = time.perf_counter()
        result_df = spark.sql(clean_query)
        # Materialize first 5 rows
        first_5_rows = result_df.take(5)
        runtime = time.perf_counter() - t0

        logger.info(f"Query {sql_file} executed in {runtime:.4f}s. First 5 rows:")
        result_df.show(5, truncate=False)

        results[sql_file] = {
            "runtime_seconds": round(runtime, 4),
            "columns": result_df.columns,
            "first_5_rows": [row.asDict() for row in first_5_rows]
        }
    return results

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    spark = get_spark_session("SparkSQLQueryDemo")
    sql_dir = os.path.abspath(os.path.dirname(__file__))
    query_results = run_all_queries(spark, sql_dir)
    spark.stop()

    out_file = os.path.join(sql_dir, "sql_query_benchmark_results.json")
    with open(out_file, "w") as f:
        # serialize datetime/date objects
        json.dump(query_results, f, indent=2, default=str)
    print(f"\nQuery results and runtimes saved to {out_file}")
