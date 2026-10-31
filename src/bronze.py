"""
Bronze Layer:
Ingests incoming CSV slices from landing area, validates basic schema,
attaches audit metadata (ingest_ts, source_file), and persists as append-only Parquet.
"""

import os
import glob
import logging
from datetime import datetime, timezone
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp, to_utc_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)

logger = logging.getLogger(__name__)

BRONZE_SCHEMA = StructType([
    StructField("Date", StringType(), False),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True)
])

def ingest_to_bronze(
    spark: SparkSession,
    landing_dir: str = "data/landing",
    bronze_dir: str = "data/bronze",
    processed_dir: Optional[str] = "data/landing/archived"
) -> DataFrame:
    """
    Ingests all new CSV files from landing_dir into Bronze append-only Parquet.
    Attaches ingest_ts (UTC timestamp) and source_file metadata.
    Moves ingested landing files to archived subfolder to guarantee idempotency.
    """
    abs_landing = os.path.abspath(landing_dir)
    abs_bronze = os.path.abspath(bronze_dir)
    os.makedirs(abs_bronze, exist_ok=True)
    if processed_dir:
        os.makedirs(os.path.abspath(processed_dir), exist_ok=True)

    csv_files = glob.glob(os.path.join(abs_landing, "*.csv"))
    if not csv_files:
        logger.info("Bronze Ingestion: No new files found in landing directory. Returning empty DataFrame.")
        # Return existing bronze if present, or empty DataFrame with Bronze schema
        if os.path.exists(abs_bronze) and os.listdir(abs_bronze):
            return spark.read.parquet(abs_bronze)
        empty_schema = StructType(BRONZE_SCHEMA.fields + [
            StructField("Pair", StringType(), True),
            StructField("ingest_ts", StringType(), True),
            StructField("source_file", StringType(), True)
        ])
        return spark.createDataFrame([], empty_schema)

    logger.info(f"Bronze Ingestion: Processing {len(csv_files)} files from {abs_landing}")
    dfs: List[DataFrame] = []
    current_utc = datetime.now(timezone.utc).isoformat()

    for f in csv_files:
        base_name = os.path.basename(f)
        pair = base_name.split("_")[0]
        
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .schema(BRONZE_SCHEMA)
            .load(f)
            .withColumn("Pair", lit(pair))
            .withColumn("ingest_ts", lit(current_utc))
            .withColumn("source_file", lit(base_name))
        )
        dfs.append(df)

    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df)

    # Append to Bronze partitioned by Pair
    logger.info(f"Writing {combined.count()} rows to Bronze layer at {abs_bronze}")
    (
        combined.write
        .mode("append")
        .partitionBy("Pair")
        .option("compression", "snappy")
        .parquet(abs_bronze)
    )

    # Archive processed landing files
    if processed_dir:
        import shutil
        for f in csv_files:
            dest = os.path.join(os.path.abspath(processed_dir), os.path.basename(f))
            if os.path.exists(dest):
                os.remove(dest)
            shutil.move(f, dest)

    return spark.read.parquet(abs_bronze)

