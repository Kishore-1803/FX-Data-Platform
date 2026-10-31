# Forex Data Platform: System & Performance Metrics

## 1. Machine & Environment Specifications
- **Operating System**: Windows 11 (10.0.26300)
- **Processor**: Intel64 Family 6 Model 183 Stepping 1, GenuineIntel (24 physical cores, 32 logical cores)
- **Total RAM**: 15.63 GB
- **Python Version**: 3.12.9
- **Database**: PostgreSQL 15 (Docker container `forex-airflow-postgres`)
- **Orchestration**: Apache Airflow 2.8.1 (Docker compose environment)

## 2. Dataset Row Counts Across Pipeline Layers
| Pair | Bronze (Raw) | Quarantine (Failed) | Silver (Cleaned + Capped Fills) | Gold (Features + Masked Targets) |
| :--- | :---: | :---: | :---: | :---: |
| AUDUSD | 48,966 | 0 | 49,743 (+777 fills) | 49,743 |
| EURUSD | 48,986 | 0 | 49,754 (+768 fills) | 49,754 |
| GBPUSD | 48,968 | 0 | 49,749 (+781 fills) | 49,749 |
| USDCAD | 48,970 | 0 | 49,737 (+767 fills) | 49,737 |
| USDCHF | 48,965 | 0 | 49,732 (+767 fills) | 49,732 |
| USDJPY | 48,967 | 0 | 49,741 (+774 fills) | 49,741 |
| **TOTAL** | **293,822** | **0** | **298,456** (+4,634 fills) | **298,456** |

*Quality Gate Fixture Validation*:
- Real Data (293,822 rows): 0 rows quarantined (100% pass rate).
- Synthetic Sample Fixture (70,128 rows decaying to 0.011): 67,634 rows quarantined (96.44%) under rule `PRICE_IMPLAUSIBLE`.

## 3. PostgreSQL Database Table Counts
| Table Name | Row Count | Primary Key / Indexing | Purpose |
| :--- | :---: | :--- | :--- |
| `curated_rates` | **298,456** | `PRIMARY KEY (pair, timestamp_utc)` | Clean Silver bars with UTC timestamps and audit columns |
| `fill_log` | **4,634** | `PRIMARY KEY (id)` | Audit trail of every forward-fill event with gap length |
| `quarantine` | **0** | `PRIMARY KEY (id)` | Corrupted / implausible rows (0 on real data) |
| `model_registry` | **144** | `PRIMARY KEY (model_version)` | Registry of 24 direct models across 6 currency pairs |
| `predictions` | **144** | `UNIQUE (pair, forecast_origin, target_horizon)` | 24-step multi-horizon forecasts scored at origin |
| `monitoring_alerts` | **31** | `PRIMARY KEY (id)` | Real feature drift and volatility alerts (KS test) |

## 4. Airflow DAG Stage Execution Latencies (Full Batch Run)
**DAG ID**: `forex_data_platform_dag`  
**Run ID**: `manual_run_phase2_complete` (State: **SUCCESS**, Success Rate: **100%**)

| Task ID | Stage Function | Measured Duration (s) | State |
| :--- | :--- | :---: | :---: |
| `replay` | Releases next historical slice to landing | 1.32 s | success |
| `bronze` | PySpark schema validation & append-only Parquet | 7.67 s | success |
| `gate` | Catalyst pure SQL bounds & outlier jump audit | 10.46 s | success |
| `silver` | UTC parsing, dedup, and 3h capped forward fill | 68.76 s | success |
| `gold` | Technical indicators, cyclical encodings, and ATR14 | 39.41 s | success |
| `load` | PostgreSQL upsert of curated rates & fill logs | 9.17 s | success |
| `score` | Multi-horizon batch forecasting into predictions | 3.62 s | success |
| `monitor` | Kolmogorov-Smirnov drift test into monitoring_alerts | 4.46 s | success |
| **TOTAL** | **End-to-End Orchestrated Pipeline** | **144.87 s (~2m 25s)** | **SUCCESS** |

## 5. Incremental Replay vs Full Reload
- **Monthly Slice Addition (720h / pair)**: 4,320 rows added to Bronze, Silver, and Gold.
- **Incremental Ingestion Runtime**: ~18.2 seconds per slice.
- **Idempotency**: Running ingestion with no new slice added 0 rows and left Parquet row counts unchanged.

## 6. API Latency Benchmarks (100 Requests per Endpoint)
| Endpoint | Mean Latency (ms) | Median p50 (ms) | 95th Percentile p95 (ms) |
| :--- | :---: | :---: | :---: |
| `GET /rates/latest` | 31.49 ms | 30.56 ms | 35.78 ms |
| `GET /rates/history` | 37.33 ms | 35.39 ms | 54.15 ms |
| `GET /quality/status` | 22.3 ms | 19.27 ms | 44.0 ms |
| `GET /predict` | 10.91 ms | 8.91 ms | 22.43 ms |
| `GET /model/metrics` | 13.73 ms | 10.19 ms | 34.89 ms |

## 7. Forecasting Performance Summary (5-Fold Walk-Forward CV)
- **Target Masking**: 32,270 / 1,193,824 targets (2.70%) dropped across horizons h=1, 6, 12, 24 due to forward-filled endpoints.
- **Direct vs Recursive**: Direct Ridge models achieve 15.8% to 28.3% lower MAE than recursive rollouts, eliminating multi-step error accumulation.
- **Statistical Significance**: Model directional accuracy hovers between 48.7% and 51.2% with binomial test p-values > 0.18 and paired loss test p-values > 0.35, confirming hourly returns in liquid FX are near random-walk.
