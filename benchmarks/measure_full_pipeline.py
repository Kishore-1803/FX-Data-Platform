import os
import sys
import time
import json
import statistics
import shutil
import psutil

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, base_dir)

from src.spark_session import get_spark_session
from src.pipeline import run_pipeline
from src.ingest import ingest_all_raw_data

def get_machine_specs():
    cpu_count_physical = psutil.cpu_count(logical=False)
    cpu_count_logical = psutil.cpu_count(logical=True)
    mem = psutil.virtual_memory()
    return {
        "cpu_physical_cores": cpu_count_physical,
        "cpu_logical_cores": cpu_count_logical,
        "total_ram_gb": round(mem.total / (1024**3), 2),
        "available_ram_gb": round(mem.available / (1024**3), 2),
    }

def get_dir_size_and_files(path):
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            total_size += os.path.getsize(fp)
            file_count += 1
    return total_size, file_count

def main():
    print("Gathering Machine Specs...")
    specs = get_machine_specs()
    print("Specs:", specs)

    raw_dir = os.path.join(base_dir, "data", "raw")
    parquet_dir = os.path.join(base_dir, "data", "processed", "parquet")
    report_path = os.path.join(base_dir, "data_quality_report.json")

    # Spark configs
    spark_config = {
        "master": "local[*]",
        "spark.driver.memory": "4g",
        "spark.sql.shuffle.partitions": "4",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
    }

    # Run full pipeline 3 times
    # Run 1: Cold (new Spark session)
    print("\n--- Starting Full Pipeline Run 1 (Cold) ---")
    spark1 = get_spark_session("ForexPipelineRun1")
    run1 = run_pipeline(raw_dir, parquet_dir, report_path, spark=spark1)
    spark1.stop()

    # Runs 2 & 3: Warm (shared active Spark session)
    print("\n--- Starting Full Pipeline Runs 2 & 3 (Warm) ---")
    spark_warm = get_spark_session("ForexPipelineWarmRuns")
    run2 = run_pipeline(raw_dir, parquet_dir, report_path, spark=spark_warm)
    run3 = run_pipeline(raw_dir, parquet_dir, report_path, spark=spark_warm)

    # Parquet Output Metrics
    total_parquet_size, parquet_file_count = get_dir_size_and_files(parquet_dir)
    parquet_data_files = [
        os.path.join(r, f)
        for r, d, fs in os.walk(parquet_dir)
        for f in fs if f.endswith(".parquet")
    ]
    data_file_count = len(parquet_data_files)

    # Partition layout
    partitions = []
    for root, dirs, files in os.walk(parquet_dir):
        rel = os.path.relpath(root, parquet_dir)
        if rel != "." and ("Pair=" in rel or "Year=" in rel):
            partitions.append(rel)

    # Measure Parquet size with ONLY the same 6 CSV columns
    same_cols_parquet_dir = os.path.join(base_dir, "data", "processed", "parquet_same_cols")
    raw_df = ingest_all_raw_data(spark_warm, raw_dir)
    csv_cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
    (
        raw_df.select(csv_cols)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(same_cols_parquet_dir)
    )
    same_cols_parquet_size, same_cols_file_count = get_dir_size_and_files(same_cols_parquet_dir)

    spark_warm.stop()

    # Raw CSV sizes
    total_csv_size = sum(
        os.path.getsize(os.path.join(raw_dir, f))
        for f in os.listdir(raw_dir)
        if f.endswith(".csv")
    )

    results = {
        "machine_specs": specs,
        "spark_config": spark_config,
        "runs": {
            "run1_cold": run1["timings"],
            "run2_warm": run2["timings"],
            "run3_warm": run3["timings"],
        },
        "mean_timings": {
            stage: statistics.mean([
                run1["timings"][stage],
                run2["timings"][stage],
                run3["timings"][stage]
            ])
            for stage in run1["timings"]
        },
        "row_counts": run1["counts"],
        "parquet_metrics": {
            "total_size_bytes": total_parquet_size,
            "total_size_mb": round(total_parquet_size / (1024*1024), 2),
            "total_file_count": parquet_file_count,
            "parquet_data_file_count": data_file_count,
            "compression_codec": "snappy",
            "partition_dirs_count": len(partitions),
            "sample_partition_layout": sorted(partitions)[:10],
        },
        "size_comparisons": {
            "total_input_csv_bytes": total_csv_size,
            "total_input_csv_mb": round(total_csv_size / (1024*1024), 2),
            "same_cols_parquet_bytes": same_cols_parquet_size,
            "same_cols_parquet_mb": round(same_cols_parquet_size / (1024*1024), 2),
            "same_cols_ratio_csv_to_parquet": round(total_csv_size / same_cols_parquet_size, 2) if same_cols_parquet_size else None,
            "all_cols_parquet_bytes": total_parquet_size,
            "all_cols_parquet_mb": round(total_parquet_size / (1024*1024), 2),
            "all_cols_ratio_parquet_to_csv": round(total_parquet_size / total_csv_size, 2) if total_csv_size else None,
        }
    }

    results_file = os.path.join(base_dir, "benchmarks", "pipeline_measurement_results.json")
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nMeasurements written to {results_file}")
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
