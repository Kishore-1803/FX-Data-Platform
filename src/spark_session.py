import os
import sys
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "ForexDataPipeline", master: str = "local[*]") -> SparkSession:
    """
    Initializes and returns a configured SparkSession optimized for local execution
    on Windows, ensuring correct Python environment binding and loopback socket connections.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.driver.extraJavaOptions",
            "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
            "--add-opens=java.base/java.nio=ALL-UNNAMED"
        )
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
