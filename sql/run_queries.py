import os
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
    logger.info("Setting up cleaned temporary view 'forex_cleaned'...")
    raw_dir = os.path.abspath(os.path.join(sql_dir, "..", "data", "raw"))
    raw_df = ingest_all_raw_data(spark, raw_dir)
    cleaned_df = clean_forex_data(raw_df)
    features_df = build_features(cleaned_df)

    # Register temporary view for Spark SQL execution
    features_df.createOrReplaceTempView("forex_cleaned")
    logger.info("Temporary view 'forex_cleaned' registered successfully.")

    for sql_file in SQL_FILES:
        file_path = os.path.join(sql_dir, sql_file)
        logger.info(f"\n{'='*70}\nExecuting SQL: {sql_file}\n{'='*70}")
        with open(file_path, "r", encoding="utf-8") as f:
            query = f.read()

        # Remove line comments that might disrupt execution
        clean_query = "\n".join([line for line in query.splitlines() if not line.strip().startswith("--")])
        result_df = spark.sql(clean_query)
        result_df.show(10, truncate=False)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    spark = get_spark_session("SparkSQLQueryDemo")
    sql_dir = os.path.abspath(os.path.dirname(__file__))
    run_all_queries(spark, sql_dir)
    spark.stop()
