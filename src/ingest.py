import os
import glob
import logging
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)
from pyspark.sql.functions import lit

logger = logging.getLogger(__name__)

# Explicit Schema for Raw Forex OHLCV CSV Files
RAW_OHLCV_SCHEMA = StructType([
    StructField("Date", StringType(), False),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True)
])

def parse_filename_metadata(filename: str) -> tuple[str, str]:
    """
    Extracts Pair and Timeframe from filenames formatted as {Pair}_{Timeframe}_*.csv
    Example: EURUSD_1h_2017-01-01_2024-12-31.csv -> ('EURUSD', '1h')
    """
    basename = os.path.basename(filename)
    parts = basename.split("_")
    pair = parts[0] if len(parts) > 0 else "UNKNOWN"
    timeframe = parts[1] if len(parts) > 1 else "1h"
    return pair, timeframe

def ingest_raw_csv(
    spark: SparkSession,
    file_path: str,
    pair: Optional[str] = None,
    timeframe: Optional[str] = None,
    schema: StructType = RAW_OHLCV_SCHEMA
) -> DataFrame:
    """
    Reads a single raw OHLCV CSV file using an explicit schema, appending Pair and Timeframe columns.
    """
    if not pair or not timeframe:
        auto_pair, auto_tf = parse_filename_metadata(file_path)
        pair = pair or auto_pair
        timeframe = timeframe or auto_tf

    logger.info(f"Ingesting {file_path} for pair={pair}, timeframe={timeframe}")

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(file_path)
        .withColumn("Pair", lit(pair))
        .withColumn("Timeframe", lit(timeframe))
    )
    return df

def ingest_all_raw_data(
    spark: SparkSession,
    raw_dir: str,
    pattern: str = "*.csv",
    schema: StructType = RAW_OHLCV_SCHEMA
) -> DataFrame:
    """
    Discovers all raw CSV files in raw_dir matching pattern, reads each with explicit schema,
    attaches metadata, and returns a single unioned DataFrame.
    """
    search_path = os.path.join(raw_dir, pattern)
    csv_files = glob.glob(search_path)
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found matching {search_path}")

    logger.info(f"Found {len(csv_files)} files to ingest from {raw_dir}")

    dataframes: List[DataFrame] = []
    for f in csv_files:
        df = ingest_raw_csv(spark, f, schema=schema)
        dataframes.append(df)

    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df)

    return combined_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from src.spark_session import get_spark_session

    spark = get_spark_session("IngestTest")
    raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
    df = ingest_all_raw_data(spark, raw_dir)
    print(f"Total ingested records: {df.count()}")
    df.show(5)
    spark.stop()
