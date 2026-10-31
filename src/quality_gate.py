"""
Quality Gate Module:
Pure Spark SQL implementation: performs schema validation, null checks,
price plausibility bounds, and outlier jump detection entirely on the Catalyst optimizer.
"""

import os
import yaml
import logging
from typing import Tuple, Dict, Any
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)

class QualityGate:
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, "r") as f:
            self.cfg = yaml.safe_load(f)["quality_gate"]

    def evaluate_and_filter(
        self,
        df: DataFrame,
        quarantine_dir: str = "data/quarantine"
    ) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
        """
        Splits DataFrame into valid records (Silver candidates) and quarantined records
        using pure Spark SQL execution without Python worker IPC serialization.
        """
        if df.take(1) == []:
            return df, df, {"total": 0, "passed": 0, "quarantined": 0}

        df.createOrReplaceTempView("bronze_in")

        # Construct pure SQL price bounds expression
        bounds = self.cfg["price_bounds"]
        bound_clauses = []
        for pair, b in bounds.items():
            clause = f"(Pair = '{pair}' AND Close >= {b['min']} AND Close <= {b['max']} AND Open >= {b['min']} AND Open <= {b['max']})"
            bound_clauses.append(clause)
        plausible_sql = " OR ".join(bound_clauses)
        max_jump = self.cfg.get("max_hourly_jump_pct", 5.0)

        spark = df.sparkSession
        annotated_sql = f"""
            SELECT 
                Date, Open, High, Low, Close, Volume, Pair, ingest_ts, source_file,
                CASE 
                    WHEN Date IS NULL OR Open IS NULL OR High IS NULL OR Low IS NULL OR Close IS NULL 
                        THEN 'MISSING_MANDATORY_VALUE'
                    WHEN NOT ({plausible_sql}) 
                        THEN 'PRICE_IMPLAUSIBLE'
                    WHEN prev_close IS NOT NULL AND ABS((Close - prev_close) / prev_close) * 100.0 > {max_jump}
                        THEN 'OUTLIER_HOURLY_JUMP'
                    ELSE NULL
                END AS quarantine_reason
            FROM (
                SELECT *,
                       LAG(Close, 1) OVER (PARTITION BY Pair ORDER BY Date) AS prev_close
                FROM bronze_in
            )
        """
        annotated = spark.sql(annotated_sql)
        annotated.createOrReplaceTempView("annotated_bronze")

        quarantined_df = spark.sql("SELECT * FROM annotated_bronze WHERE quarantine_reason IS NOT NULL")
        valid_df = spark.sql("SELECT Date, Open, High, Low, Close, Volume, Pair, ingest_ts, source_file FROM annotated_bronze WHERE quarantine_reason IS NULL")

        q_count = quarantined_df.count()
        v_count = valid_df.count()
        total = q_count + v_count

        logger.info(f"Quality Gate Results: Total={total}, Passed={v_count}, Quarantined={q_count}")

        if q_count > 0:
            abs_q = os.path.abspath(quarantine_dir)
            logger.warning(f"Persisting {q_count} quarantined records to {abs_q}")
            (
                quarantined_df
                .write
                .mode("append")
                .partitionBy("Pair")
                .parquet(abs_q)
            )

        metrics = {
            "total_records": total,
            "passed_records": v_count,
            "quarantined_records": q_count,
            "pass_rate_pct": round((v_count / total) * 100.0, 2) if total > 0 else 100.0
        }

        return valid_df, quarantined_df, metrics
