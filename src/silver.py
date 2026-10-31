"""
Silver Layer:
Cleans validated market data:
  1. Deduplicates on (Pair, Date).
  2. Parses Date string to explicit UTC Timestamp.
  3. Capped forward-fill: fills gaps <= 3 hours only.
  4. Appends audit columns: is_filled (bool) and gap_length (int).
  5. Logs all fills to fill_log table.
  6. Writes clean partitioned Parquet.
"""

import os
import logging
from typing import Tuple, List, Dict, Any
from datetime import datetime, timezone
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, to_utc_timestamp, year, lit, lag, when, coalesce, last
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

def process_silver_layer(
    spark: SparkSession,
    valid_bronze_df: DataFrame,
    silver_output_dir: str = "data/silver",
    max_gap_hours: int = 3,
    fill_log_path: str = "data/audit/fill_log.json"
) -> Tuple[DataFrame, List[Dict[str, Any]]]:
    """
    Cleans Bronze data, applies capped forward-fill, records audit columns,
    and returns (clean_silver_df, fill_logs).
    """
    if valid_bronze_df.take(1) == []:
        logger.info("Silver Layer: Input DataFrame is empty. Skipping.")
        abs_silver = os.path.abspath(silver_output_dir)
        if os.path.exists(abs_silver) and os.listdir(abs_silver):
            return spark.read.parquet(abs_silver), []
        return valid_bronze_df, []

    # 1. Deduplicate on (Pair, Date)
    deduped = valid_bronze_df.dropDuplicates(["Pair", "Date"])

    # 2. Convert to UTC Timestamp and Year
    df_ts = (
        deduped
        .withColumn("Timestamp_UTC", to_timestamp(col("Date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("Year", year(col("Timestamp_UTC")))
    )

    # 3. Detect unexpected gaps and apply capped forward-fill per Pair
    # Convert to Pandas per pair for precise time-grid alignment & capped filling
    pdf = df_ts.toPandas()
    clean_pairs = []
    fill_records: List[Dict[str, Any]] = []

    for pair, group in pdf.groupby("Pair"):
        group = group.sort_values("Timestamp_UTC").reset_index(drop=True)
        group["is_filled"] = False
        group["gap_length"] = 0

        # Calculate time diff between consecutive bars
        diff_h = (group["Timestamp_UTC"].diff().dt.total_seconds() / 3600.0).fillna(1.0)
        
        # Identify gaps <= max_gap_hours (e.g. 2h or 3h gaps)
        # Note: Gaps that span Friday evening to Sunday evening are expected closures (not filled)
        new_rows = []
        for i in range(1, len(group)):
            delta = diff_h.iloc[i]
            if 1.0 < delta <= max_gap_hours:
                prev_row = group.iloc[i - 1]
                curr_row = group.iloc[i]
                
                # Check if it's a weekend closure (do not fill)
                prev_dow = prev_row["Timestamp_UTC"].dayofweek # 4 = Friday
                curr_dow = curr_row["Timestamp_UTC"].dayofweek # 6 = Sunday
                if prev_dow == 4 and curr_dow == 6:
                    continue
                
                # Generate missing hourly bars
                missing_steps = int(delta) - 1
                for step in range(1, missing_steps + 1):
                    fill_time = prev_row["Timestamp_UTC"] + pd.Timedelta(hours=step)
                    filled_row = prev_row.copy()
                    filled_row["Timestamp_UTC"] = fill_time
                    filled_row["Date"] = fill_time.strftime("%Y-%m-%d %H:%M:%S")
                    filled_row["Year"] = fill_time.year
                    filled_row["is_filled"] = True
                    filled_row["gap_length"] = int(delta)
                    # For forward-filled bars, volume is 0 ticks
                    filled_row["Volume"] = 0
                    new_rows.append(filled_row)
                    
                    fill_records.append({
                        "pair": pair,
                        "filled_timestamp": str(fill_time),
                        "gap_length": int(delta),
                        "columns_filled": ["Open", "High", "Low", "Close"],
                        "fill_method": "forward_fill_carry_close",
                        "recorded_at": datetime.now(timezone.utc).isoformat()
                    })

        if new_rows:
            group = pd.concat([group, pd.DataFrame(new_rows)], ignore_index=True)
            group = group.sort_values("Timestamp_UTC").reset_index(drop=True)

        clean_pairs.append(group)

    full_cleaned_pdf = pd.concat(clean_pairs, ignore_index=True)
    logger.info(f"Silver Layer: Capped forward-fill added {len(fill_records)} bars across all pairs.")

    # Convert back to Spark DataFrame
    silver_spark_df = spark.createDataFrame(full_cleaned_pdf)

    # Persist Silver partitioned by Pair and Year
    abs_silver = os.path.abspath(silver_output_dir)
    (
        silver_spark_df.write
        .mode("overwrite")
        .partitionBy("Pair", "Year")
        .option("compression", "snappy")
        .parquet(abs_silver)
    )
    logger.info(f"Silver Layer successfully persisted to {abs_silver}")

    # Save fill logs
    if fill_records:
        import json
        abs_log = os.path.abspath(fill_log_path)
        os.makedirs(os.path.dirname(abs_log), exist_ok=True)
        with open(abs_log, "w") as f:
            json.dump(fill_records, f, indent=2)
        logger.info(f"Fill log recorded {len(fill_records)} events to {abs_log}")

    return spark.read.parquet(abs_silver), fill_records

