"""
Gold Layer:
Computes point-in-time safe features and aggregate analytics.
Strict point-in-time rule: NO bar's future or current target price is ever used as a feature.
All window functions are partitioned by Pair.
"""

import os
import logging
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, avg, stddev, lag, log, sin, cos, hour, dayofweek, when, lit, abs as spark_abs
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

def build_gold_features(
    spark: SparkSession,
    silver_df: DataFrame,
    gold_output_dir: str = "data/gold"
) -> DataFrame:
    """
    Constructs Gold feature dataset:
      - Point-in-time lagged log-returns (ret_lag1..ret_lag5)
      - Rolling volatility (rolling_vol_5, rolling_vol_20)
      - Moving Average ratios (ma_ratio_5_20, ma_ratio_5_50)
      - Intraday cyclical time encodings (hour_sin, hour_cos, dow_sin, dow_cos)
      - Bar Range & Volume Change
      - Multi-horizon targets (r_1 to r_24): future log-returns strictly for training/evaluation
    """
    if silver_df.take(1) == []:
        logger.info("Gold Layer: Input DataFrame is empty. Skipping.")
        abs_gold = os.path.abspath(gold_output_dir)
        if os.path.exists(abs_gold) and os.listdir(abs_gold):
            return spark.read.parquet(abs_gold)
        return silver_df

    logger.info("Building Gold Layer point-in-time technical features...")

    # Strict partitionBy("Pair").orderBy("Timestamp_UTC")
    w_pair = Window.partitionBy("Pair").orderBy("Timestamp_UTC")
    w_5 = Window.partitionBy("Pair").orderBy("Timestamp_UTC").rowsBetween(-4, 0)
    w_20 = Window.partitionBy("Pair").orderBy("Timestamp_UTC").rowsBetween(-19, 0)
    w_50 = Window.partitionBy("Pair").orderBy("Timestamp_UTC").rowsBetween(-49, 0)

    # 1. Base log return of historical bar
    df = silver_df.withColumn("prev_close_1", lag("Close", 1).over(w_pair))
    df = df.withColumn(
        "ret_lag1",
        when(col("prev_close_1").isNotNull(), log(col("Close") / col("prev_close_1"))).otherwise(0.0)
    )

    # 2. Lagged returns
    for l in range(2, 6):
        df = df.withColumn(f"ret_lag{l}", lag("ret_lag1", l - 1).over(w_pair))

    # 3. Rolling Moving Averages & Volatility
    df = (
        df
        .withColumn("ma_5", avg("Close").over(w_5))
        .withColumn("ma_20", avg("Close").over(w_20))
        .withColumn("ma_50", avg("Close").over(w_50))
        .withColumn("rolling_vol_5", stddev("ret_lag1").over(w_5))
        .withColumn("rolling_vol_20", stddev("ret_lag1").over(w_20))
        .withColumn("ma_ratio_5_20", col("ma_5") / col("ma_20"))
        .withColumn("ma_ratio_5_50", col("ma_5") / col("ma_50"))
        .withColumn("range_pct", ((col("High") - col("Low")) / col("Close")) * 100.0)
    )

    # 3b. True Range & ATR14
    w_14 = Window.partitionBy("Pair").orderBy("Timestamp_UTC").rowsBetween(-13, 0)
    df = (
        df
        .withColumn("tr_hl", col("High") - col("Low"))
        .withColumn("tr_hc", when(col("prev_close_1").isNotNull(), spark_abs(col("High") - col("prev_close_1"))).otherwise(col("tr_hl")))
        .withColumn("tr_lc", when(col("prev_close_1").isNotNull(), spark_abs(col("Low") - col("prev_close_1"))).otherwise(col("tr_hl")))
    )
    df = df.withColumn(
        "true_range",
        when((col("tr_hl") >= col("tr_hc")) & (col("tr_hl") >= col("tr_lc")), col("tr_hl"))
        .when((col("tr_hc") >= col("tr_hl")) & (col("tr_hc") >= col("tr_lc")), col("tr_hc"))
        .otherwise(col("tr_lc"))
    )
    df = df.withColumn("atr_14", avg("true_range").over(w_14))

    # 4. Cyclical Calendar Encodings (hour-of-day, day-of-week)
    pi = 3.141592653589793
    df = (
        df
        .withColumn("hour_val", hour(col("Timestamp_UTC")))
        .withColumn("dow_val", dayofweek(col("Timestamp_UTC")))
        .withColumn("hour_sin", sin(col("hour_val") * (2.0 * pi / 24.0)))
        .withColumn("hour_cos", cos(col("hour_val") * (2.0 * pi / 24.0)))
        .withColumn("dow_sin", sin(col("dow_val") * (2.0 * pi / 7.0)))
        .withColumn("dow_cos", cos(col("dow_val") * (2.0 * pi / 7.0)))
    )

    # 5. Volume Change
    df = df.withColumn("prev_vol", lag("Volume", 1).over(w_pair))
    df = df.withColumn(
        "volume_change",
        when(
            (col("prev_vol").isNotNull()) & (col("prev_vol") > 0),
            (col("Volume") - col("prev_vol")) / col("prev_vol")
        ).otherwise(0.0)
    )

    # 6. Targets for forecasting (r_h = log(Close_{t+h} / Close_t)) strictly using LEAD
    # Target bars that are forward-filled are masked later during model training
    for h in range(1, 25):
        w_lead_h = Window.partitionBy("Pair").orderBy("Timestamp_UTC")
        lead_close = lag("Close", -h).over(w_lead_h)
        lead_filled = lag("is_filled", -h).over(w_lead_h)
        # Calculate target return
        target_ret = log(lead_close / col("Close"))
        # Mask target as NULL if EITHER endpoint (current bar or lead bar) was forward-filled
        df = df.withColumn(
            f"target_r_{h}",
            when((lead_filled == False) & (col("is_filled") == False), target_ret).otherwise(lit(None))
        )

    # Persist Gold layer
    abs_gold = os.path.abspath(gold_output_dir)
    (
        df.write
        .mode("overwrite")
        .partitionBy("Pair", "Year")
        .option("compression", "snappy")
        .parquet(abs_gold)
    )
    logger.info(f"Gold Layer persisted to {abs_gold}")

    return spark.read.parquet(abs_gold)

