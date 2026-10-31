import os
import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

def write_partitioned_parquet(
    df: DataFrame,
    output_path: str,
    partition_cols: list[str] = ["Pair", "Year"],
    compression: str = "snappy",
    mode: str = "overwrite"
) -> str:
    """
    Writes processed Forex features to columnar Parquet format partitioned by Pair and Year.

    Architectural Decision:
      Target location is local persistent disk / mounted storage instead of direct HDFS
      RPC endpoints to definitively avoid the documented Hadoop RPC protocol version mismatch
      ('RPC response exceeds maximum data length') that affected the original legacy Scala pipeline.
      Parquet with Snappy compression delivers optimal columnar scan speeds, predicate pushdown,
      and partition pruning.
    """
    abs_output_path = os.path.abspath(output_path)
    logger.info(
        f"Writing Parquet dataset to {abs_output_path} "
        f"[partitionBy={partition_cols}, compression={compression}, mode={mode}]"
    )

    (
        df.write
        .mode(mode)
        .partitionBy(*partition_cols)
        .option("compression", compression)
        .parquet(abs_output_path)
    )

    logger.info(f"Parquet export completed successfully at {abs_output_path}")
    return abs_output_path

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from src.spark_session import get_spark_session
    from src.ingest import ingest_all_raw_data
    from src.clean import clean_forex_data
    from src.features import build_features

    spark = get_spark_session("OutputTest")
    raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
    out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "processed", "parquet"))

    raw_df = ingest_all_raw_data(spark, raw_dir)
    cleaned_df = clean_forex_data(raw_df)
    feat_df = build_features(cleaned_df)

    write_partitioned_parquet(feat_df, out_dir)
    read_back = spark.read.parquet(out_dir)
    print(f"Verified read-back from Parquet: {read_back.count()} rows")
    spark.stop()
