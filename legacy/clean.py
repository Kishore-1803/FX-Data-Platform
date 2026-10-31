import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_utc_timestamp,
    coalesce,
    last,
    year,
    date_format
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume"]

def clean_forex_data(
    df: DataFrame,
    source_timezone: str = "UTC"
) -> DataFrame:
    """
    Cleans raw Forex DataFrame:
      1. Deduplicates on (Pair, Date)
      2. Drops rows where critical OHLCV values are null
      3. Parses Date with explicit timezone and converts to UTC Timestamp
      4. Forward-fills short gaps using last(..., ignoreNulls=True) over Window.partitionBy('Pair').orderBy('Timestamp_UTC')
      5. Derives 'Year' column for partitioned storage
    """
    initial_count = df.count()
    logger.info(f"Starting cleaning pipeline on {initial_count} records")

    # Step 1: Deduplicate on (Pair, Date)
    deduped_df = df.dropDuplicates(["Pair", "Date"])
    after_dedup_count = deduped_df.count()
    duplicates_removed = initial_count - after_dedup_count
    logger.info(f"Deduplication on ('Pair', 'Date') removed {duplicates_removed} duplicate rows")

    # Step 2: Parse Date with explicit timezone and convert to UTC
    # Robustly handles both 'yyyy-MM-dd HH:mm:ss' and 'yyyy-MM-dd' formats
    from pyspark.sql.functions import when, length
    parsed_timestamp = when(
        length(col("Date")) <= 10,
        to_timestamp(col("Date"), "yyyy-MM-dd")
    ).otherwise(
        to_timestamp(col("Date"), "yyyy-MM-dd HH:mm:ss")
    )
    utc_timestamp = to_utc_timestamp(parsed_timestamp, source_timezone)

    df_with_ts = (
        deduped_df
        .withColumn("Timestamp_UTC", utc_timestamp)
        .withColumn("Year", year(col("Timestamp_UTC")))
    )

    # Step 3: Forward-fill short gaps over window partitioned by Pair
    # Window specification: partitionBy("Pair").orderBy("Timestamp_UTC")
    window_ffill = (
        Window
        .partitionBy("Pair")
        .orderBy("Timestamp_UTC")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    df_ffilled = df_with_ts
    for c in OHLCV_COLS:
        df_ffilled = df_ffilled.withColumn(c, last(col(c), True).over(window_ffill))

    # Step 4: Drop any remaining rows where OHLCV is still null (e.g., initial row was null)
    cleaned_df = df_ffilled.dropna(subset=OHLCV_COLS)
    final_count = cleaned_df.count()
    dropped_nulls = after_dedup_count - final_count
    logger.info(f"Dropped {dropped_nulls} unfillable null rows. Final cleaned count: {final_count}")

    return cleaned_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import os
    from src.spark_session import get_spark_session
    from src.ingest import ingest_all_raw_data

    spark = get_spark_session("CleanTest")
    raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
    raw_df = ingest_all_raw_data(spark, raw_dir)
    cleaned_df = clean_forex_data(raw_df)
    cleaned_df.show(5)
    spark.stop()
