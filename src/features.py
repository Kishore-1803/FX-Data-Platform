import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    avg,
    stddev,
    lag,
    when
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

def build_features(df: DataFrame) -> DataFrame:
    """
    Computes technical indicators and lag features for Forex time-series data.
    Ensures EVERY window specification strictly utilizes partitionBy("Pair").orderBy("Date").

    Features generated:
      - Range: High - Low
      - Change: Close - Open
      - ChangePct: ((Close - Open) / Open) * 100
      - MA5: 5-period rolling moving average of Close
      - MA20: 20-period rolling moving average of Close
      - Volatility20: 20-period rolling standard deviation of Close
      - Close_lag1, Close_lag2, Close_lag3: Lagged Close prices
      - Change_lag1, Change_lag2: Lagged Change values
      - MA5_MA20_Ratio: Momentum ratio (MA5 / MA20)
      - Gain, Loss: Directional movement components for RSI
    """
    logger.info("Computing engineered features with Pair-partitioned windows")

    # Window specifications strictly following partitionBy("Pair").orderBy("Date")
    window_5 = (
        Window
        .partitionBy("Pair")
        .orderBy("Date")
        .rowsBetween(-4, 0)
    )

    window_20 = (
        Window
        .partitionBy("Pair")
        .orderBy("Date")
        .rowsBetween(-19, 0)
    )

    window_lag = (
        Window
        .partitionBy("Pair")
        .orderBy("Date")
    )

    features_df = (
        df
        # 1. Bar spread and directional metrics
        .withColumn("Range", col("High") - col("Low"))
        .withColumn("Change", col("Close") - col("Open"))
        .withColumn("ChangePct", ((col("Close") - col("Open")) / col("Open")) * 100.0)

        # 2. Moving Averages
        .withColumn("MA5", avg("Close").over(window_5))
        .withColumn("MA20", avg("Close").over(window_20))

        # 3. Volatility (20-period rolling standard deviation)
        .withColumn("Volatility20", stddev("Close").over(window_20))

        # 4. Lag Features
        .withColumn("Close_lag1", lag("Close", 1).over(window_lag))
        .withColumn("Close_lag2", lag("Close", 2).over(window_lag))
        .withColumn("Close_lag3", lag("Close", 3).over(window_lag))
        .withColumn("Change_lag1", lag("Change", 1).over(window_lag))
        .withColumn("Change_lag2", lag("Change", 2).over(window_lag))

        # 5. Trend momentum ratio
        .withColumn("MA5_MA20_Ratio", col("MA5") / col("MA20"))

        # 6. RSI directional components
        .withColumn("Gain", when(col("Change") > 0, col("Change")).otherwise(0.0))
        .withColumn("Loss", when(col("Change") < 0, -col("Change")).otherwise(0.0))
    )

    return features_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import os
    from src.spark_session import get_spark_session
    from src.ingest import ingest_all_raw_data
    from src.clean import clean_forex_data

    spark = get_spark_session("FeaturesTest")
    raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
    raw_df = ingest_all_raw_data(spark, raw_dir)
    cleaned_df = clean_forex_data(raw_df)
    feat_df = build_features(cleaned_df)
    print("Features schema:")
    feat_df.printSchema()
    feat_df.select("Pair", "Date", "Close", "MA5", "MA20", "Volatility20", "Close_lag1").show(5)
    spark.stop()
