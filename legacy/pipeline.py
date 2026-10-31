"""
End-to-End Forex Data Platform Pipeline Orchestrator:
Executes:
  1. Replay driver: releases next historical slice.
  2. Bronze: ingest to append-only Parquet.
  3. Quality Gate: schema, bounds, outlier jumps -> Quarantine invalid rows.
  4. Silver: clean, UTC timestamp, capped forward fill (<=3h), audit tags.
  5. Gold: point-in-time technical features & multi-horizon targets.
  6. Database Load: persist to PostgreSQL / curated tables.
"""

import os
import sys
import time
import logging
from typing import Dict, Any

from src.spark_session import get_spark_session
from src.replay import HistoricalReplayDriver
from src.bronze import ingest_to_bronze
from src.quality_gate import QualityGate
from src.silver import process_silver_layer
from src.gold import build_gold_features
from src.db_loader import (
    get_db_engine, init_db, load_silver_to_postgres,
    load_quarantine_to_postgres, load_quality_results
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ForexPlatformPipeline")

def run_platform_pipeline(
    full_init: bool = False,
    slice_hours: int = 168,
    spark=None
) -> Dict[str, Any]:
    owns_spark = False
    if spark is None:
        spark = get_spark_session("ForexDataPlatformBatch")
        owns_spark = True

    timings = {}
    total_start = time.perf_counter()

    # Step 1: Replay driver
    t0 = time.perf_counter()
    replay = HistoricalReplayDriver()
    if full_init:
        released = replay.release_full_history()
    else:
        released = replay.release_next_slice(slice_size_hours=slice_hours)
    timings["replay"] = time.perf_counter() - t0
    logger.info(f"Stage 1 [Replay]: Released {len(released)} slice files.")

    # Step 2: Bronze
    t0 = time.perf_counter()
    bronze_df = ingest_to_bronze(spark)
    bronze_count = bronze_df.count()
    timings["bronze"] = time.perf_counter() - t0
    logger.info(f"Stage 2 [Bronze]: Total records in Bronze layer = {bronze_count:,}")

    # Step 3: Quality Gate
    t0 = time.perf_counter()
    qgate = QualityGate()
    valid_df, quarantined_df, q_metrics = qgate.evaluate_and_filter(bronze_df)
    timings["quality_gate"] = time.perf_counter() - t0
    logger.info(f"Stage 3 [Quality Gate]: Pass rate = {q_metrics.get('pass_rate_pct', 100.0)}%")

    # Step 4: Silver
    t0 = time.perf_counter()
    silver_df, fill_records = process_silver_layer(spark, valid_df)
    silver_count = silver_df.count()
    timings["silver"] = time.perf_counter() - t0
    logger.info(f"Stage 4 [Silver]: Clean records = {silver_count:,} (Fills added: {len(fill_records)})")

    # Step 5: Gold
    t0 = time.perf_counter()
    gold_df = build_gold_features(spark, silver_df)
    gold_count = gold_df.count()
    timings["gold"] = time.perf_counter() - t0
    logger.info(f"Stage 5 [Gold]: Feature records = {gold_count:,}")

    # Step 6: Database Load
    t0 = time.perf_counter()
    engine = get_db_engine()
    init_db(engine)
    curated_loaded = load_silver_to_postgres(engine)
    quarantine_loaded = load_quarantine_to_postgres(engine)
    load_quality_results(engine, q_metrics)
    timings["db_load"] = time.perf_counter() - t0
    logger.info(f"Stage 6 [DB Load]: Curated = {curated_loaded:,}, Quarantine = {quarantine_loaded:,}")

    total_time = time.perf_counter() - total_start
    timings["total"] = total_time
    logger.info(f"PIPELINE COMPLETED in {total_time:.2f}s")

    if owns_spark:
        spark.stop()

    return {
        "timings": timings,
        "counts": {
            "bronze": bronze_count,
            "silver": silver_count,
            "gold": gold_count,
            "quarantined": q_metrics.get("quarantined_records", 0),
            "curated_db": curated_loaded
        },
        "quality_metrics": q_metrics
    }

if __name__ == "__main__":
    init_flag = "--init" in sys.argv or "-i" in sys.argv
    run_platform_pipeline(full_init=init_flag)
