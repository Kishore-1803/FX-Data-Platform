import os
import sys
import shutil
sys.path.insert(0, os.path.abspath("."))
import pandas as pd
from src.spark_session import get_spark_session
from src.quality_gate import QualityGate

spark = get_spark_session("QGate_Evaluation")
qgate = QualityGate()

# 1. Real data check
print("--- REAL DATA QUALITY GATE EVALUATION ---")
b_df = spark.read.parquet("data/bronze")
v_df, q_df, metrics = qgate.evaluate_and_filter(b_df, quarantine_dir="data/temp_quarantine")
print(f"Real data total: {metrics['total_records']:,}")
print(f"Real data passed: {metrics['passed_records']:,}")
print(f"Real data quarantined: {metrics['quarantined_records']:,}")

# 2. Synthetic sample data check
print("\n--- SYNTHETIC SAMPLE DATA QUALITY GATE EVALUATION ---")
sample_pdf = pd.read_csv("data/raw/EURUSD_1h_2017-01-01_2024-12-31.csv")
sample_pdf["Pair"] = "EURUSD"
sample_pdf["ingest_ts"] = "2026-10-31 00:00:00"
sample_pdf["source_file"] = "EURUSD_1h_synthetic.csv"
sample_df = spark.createDataFrame(sample_pdf)

v_s, q_s, s_metrics = qgate.evaluate_and_filter(sample_df, quarantine_dir="data/temp_quarantine")
print(f"Synthetic sample total: {s_metrics['total_records']:,}")
print(f"Synthetic sample passed: {s_metrics['passed_records']:,}")
print(f"Synthetic sample quarantined: {s_metrics['quarantined_records']:,}")

q_reasons = q_s.groupBy("quarantine_reason").count().toPandas()
print("Synthetic quarantine breakdown by reason:")
for _, r in q_reasons.iterrows():
    print(f"   {r['quarantine_reason']}: {r['count']:,} rows")

spark.stop()

# Clean up temp
if os.path.exists("data/temp_quarantine"):
    shutil.rmtree("data/temp_quarantine", ignore_errors=True)
