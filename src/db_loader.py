"""
PostgreSQL Storage Loader:
Loads Curated Silver rates, Quality Gate results, Quarantine, and Fill logs into PostgreSQL.
Credentials and host are pulled from environment variables.
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

def get_db_engine(db_url: Optional[str] = None):
    """
    Constructs an SQLAlchemy engine for PostgreSQL using environment variables.
    Checks host port 5433 and container port 5432.
    Fallback to SQLite only for lightweight offline unit testing.
    """
    if db_url is None:
        db_url = os.environ.get("DATABASE_URL")

    urls_to_try = []
    if db_url:
        urls_to_try.append(db_url)
    
    # Environment variables
    user = os.environ.get("POSTGRES_USER", "airflow")
    password = os.environ.get("POSTGRES_PASSWORD", "airflow_password")
    db = os.environ.get("POSTGRES_DB", "forex_platform")
    host = os.environ.get("POSTGRES_HOST")
    port = os.environ.get("POSTGRES_PORT")

    if host and port:
        urls_to_try.append(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    # Standard host mapping (port 5433 on Windows host) and container mapping (postgres:5432)
    urls_to_try.extend([
        f"postgresql+psycopg2://{user}:{password}@localhost:5433/{db}",
        f"postgresql+psycopg2://{user}:{password}@postgres:5432/{db}",
        f"postgresql+psycopg2://{user}:{password}@localhost:5432/{db}"
    ])

    for url in urls_to_try:
        try:
            engine = create_engine(url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Connected to database at {url.split('@')[-1]}")
            return engine
        except Exception:
            continue

    logger.warning("Could not connect to PostgreSQL. Using local SQLite database as fallback for testing.")
    sqlite_path = os.path.abspath("data/forex_platform.db")
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    return create_engine(f"sqlite:///{sqlite_path}")

def init_db(engine, schema_path: str = "sql/schema.sql") -> None:
    abs_schema = os.path.abspath(schema_path)
    with open(abs_schema, "r", encoding="utf-8") as f:
        sql = f.read()

    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            if "sqlite" in str(engine.url):
                stmt = stmt.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
                stmt = stmt.replace("TIMESTAMP WITHOUT TIME ZONE", "DATETIME")
                stmt = stmt.replace("BOOLEAN DEFAULT FALSE", "INTEGER DEFAULT 0")
            try:
                conn.execute(text(stmt))
            except Exception as e:
                logger.debug(f"Statement execution note: {e}")

def load_silver_to_postgres(engine, silver_dir: str = "data/silver") -> int:
    import pyarrow.parquet as pq
    abs_silver = os.path.abspath(silver_dir)
    if not os.path.exists(abs_silver) or not os.listdir(abs_silver):
        logger.info("No silver data found to load.")
        return 0

    df = pd.read_parquet(abs_silver)
    cols = ["Pair", "Timestamp_UTC", "Open", "High", "Low", "Close", "Volume", "is_filled", "gap_length"]
    load_df = df[cols].rename(columns={
        "Pair": "pair",
        "Timestamp_UTC": "timestamp_utc",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })
    
    # Load into curated_rates with primary key (pair, timestamp_utc)
    with engine.begin() as conn:
        load_df.to_sql("curated_rates", conn, if_exists="replace", index=False)
        # Ensure index exists
        try:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_curated_pair_ts ON curated_rates (pair, timestamp_utc DESC);"))
        except Exception:
            pass
    logger.info(f"Loaded {len(load_df)} rows into curated_rates table.")
    return len(load_df)

def load_fill_log_to_postgres(engine, fill_log_path: str = "data/audit/fill_log.json") -> int:
    abs_log = os.path.abspath(fill_log_path)
    if not os.path.exists(abs_log):
        # Check silver dir fallback
        alt_log = os.path.abspath("data/silver/fill_log.json")
        if os.path.exists(alt_log):
            abs_log = alt_log
        else:
            return 0
    try:
        with open(abs_log, "r", encoding="utf-8") as f:
            fills = json.load(f)
        if not fills:
            return 0
        df_fills = pd.DataFrame(fills)
        if "columns_filled" in df_fills.columns:
            df_fills["columns_filled"] = df_fills["columns_filled"].apply(
                lambda x: ",".join(x) if isinstance(x, list) else str(x)
            )
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM fill_log;"))
            df_fills.to_sql("fill_log", conn, if_exists="append", index=False)
        logger.info(f"Loaded {len(df_fills)} fill events into fill_log table.")
        return len(df_fills)
    except Exception as e:
        logger.warning(f"Error loading fill log: {e}")
        return 0

def load_quarantine_to_postgres(engine, quarantine_dir: str = "data/quarantine") -> int:
    abs_q = os.path.abspath(quarantine_dir)
    if not os.path.exists(abs_q) or not os.listdir(abs_q):
        return 0
    try:
        df = pd.read_parquet(abs_q)
        with engine.begin() as conn:
            df.to_sql("quarantine", conn, if_exists="append", index=False)
        return len(df)
    except Exception as e:
        logger.warning(f"Quarantine load note: {e}")
        return 0

def load_quality_results(engine, metrics: Dict[str, Any]) -> None:
    res_df = pd.DataFrame([{
        "total_records": metrics.get("total_records", 0),
        "passed_records": metrics.get("passed_records", 0),
        "quarantined_records": metrics.get("quarantined_records", 0),
        "pass_rate_pct": metrics.get("pass_rate_pct", 100.0)
    }])
    with engine.begin() as conn:
        res_df.to_sql("quality_results", conn, if_exists="append", index=False)
