import pytest
import os
import sys

# Ensure project modules are importable
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.spark_session import get_spark_session

@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession fixture for unit testing."""
    session = get_spark_session("PytestForexPipelineFixture", master="local[1]")
    yield session
    session.stop()

@pytest.fixture
def tiny_raw_data(spark):
    """
    Creates a tiny synthetic fixture containing:
      - Normal rows
      - A duplicate (Pair, Date) row
      - A row with None in Close to test forward-fill
      - Two distinct currency pairs to verify partition isolation
    """
    data = [
        # EURUSD rows
        ("2024-01-01 00:00:00", 1.1000, 1.1050, 1.0950, 1.1020, 1000, "EURUSD", "1h"),
        ("2024-01-01 01:00:00", 1.1020, 1.1080, 1.1010, 1.1060, 1200, "EURUSD", "1h"),
        ("2024-01-01 01:00:00", 1.1020, 1.1080, 1.1010, 1.1060, 1200, "EURUSD", "1h"), # Duplicate
        ("2024-01-01 02:00:00", 1.1060, 1.1070, 1.1020, None,   800,  "EURUSD", "1h"), # Null Close for forward-fill
        ("2024-01-01 03:00:00", 1.1060, 1.1120, 1.1050, 1.1100, 1500, "EURUSD", "1h"),

        # GBPUSD rows
        ("2024-01-01 00:00:00", 1.2500, 1.2550, 1.2480, 1.2520, 2000, "GBPUSD", "1h"),
        ("2024-01-01 01:00:00", 1.2520, 1.2590, 1.2500, 1.2580, 2200, "GBPUSD", "1h"),
    ]
    columns = ["Date", "Open", "High", "Low", "Close", "Volume", "Pair", "Timeframe"]
    return spark.createDataFrame(data, columns)
