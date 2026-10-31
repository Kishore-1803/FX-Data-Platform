import pytest
import os
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.spark_session import get_spark_session

@pytest.fixture(scope="session")
def spark():
    session = get_spark_session("PytestForexPipelineFixture", master="local[1]")
    yield session
    session.stop()

@pytest.fixture
def mock_bronze_data(spark):
    """
    Creates synthetic test data covering:
      - Valid standard rows
      - Duplicate row
      - Null price row (for quarantine test)
      - Implausible price row (for quarantine test)
      - 2-hour gap (for forward-fill test)
      - 5-hour gap (exceeds 3h cap, should remain unfilled)
    """
    data = [
        # Normal rows
        ("2024-01-01 00:00:00", 1.1000, 1.1050, 1.0950, 1.1020, 1000, "EURUSD", "2024-01-01T00:00:00Z", "test.csv"),
        ("2024-01-01 01:00:00", 1.1020, 1.1080, 1.1010, 1.1060, 1200, "EURUSD", "2024-01-01T00:00:00Z", "test.csv"),
        ("2024-01-01 01:00:00", 1.1020, 1.1080, 1.1010, 1.1060, 1200, "EURUSD", "2024-01-01T00:00:00Z", "test.csv"), # Duplicate
        # 2-hour gap: next is at 03:00 (02:00 is missing, should be filled)
        ("2024-01-01 03:00:00", 1.1060, 1.1120, 1.1050, 1.1100, 1500, "EURUSD", "2024-01-01T00:00:00Z", "test.csv"),
        # Implausible price row (EURUSD at 0.05)
        ("2024-01-01 04:00:00", 0.0500, 0.0520, 0.0480, 0.0510, 800,  "EURUSD", "2024-01-01T00:00:00Z", "test.csv"),
        # Null value row
        ("2024-01-01 05:00:00", None,   1.1120, 1.1050, 1.1100, 1500, "EURUSD", "2024-01-01T00:00:00Z", "test.csv"),
    ]
    cols = ["Date", "Open", "High", "Low", "Close", "Volume", "Pair", "ingest_ts", "source_file"]
    return spark.createDataFrame(data, cols)
