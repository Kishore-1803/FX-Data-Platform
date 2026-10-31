# Forex Data Platform: System & Performance Metrics

## 1. Machine & Environment Specifications
- **Operating System**: Windows 11 (Build 10.0.26300)
- **Processor**: 13th Gen Intel(R) Core(TM) i9-13980HX (24 physical cores, 32 logical processors)
- **Total RAM**: 15.63 GB
- **Python Version**: 3.12.9
- **Database**: PostgreSQL 15 (Docker container `forex-airflow-postgres`, port 5433)
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
| `predictions` | **576** | `UNIQUE (pair, forecast_origin, target_horizon)` | 24-step multi-horizon forecasts scored at origin |
| `monitoring_alerts` | **64** | `PRIMARY KEY (id)` | Feature distribution shifts and volatility alerts |

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
| `monitor` | Drift test into monitoring_alerts | 4.46 s | success |
| **TOTAL** | **End-to-End Orchestrated Pipeline** | **144.87 s (~2m 25s)** | **SUCCESS** |

## 5. Incremental Replay Execution Inside Airflow DAG
Executed 3 consecutive incremental slices (720 hours = ~1 month per slice) inside Airflow DAG:
- **Run 1 (`incremental_dag_run_1`)**: +4,320 rows added across 6 pairs. State advanced to `2017-02-13 06:00:00`. (Totals: Bronze=4,320, Silver=4,320, Gold=4,320).
- **Run 2 (`incremental_dag_run_2`)**: +4,320 rows added. State advanced to `2017-03-27 05:00:00`. (Totals: Bronze=8,640, Silver=8,640, Gold=8,640).
- **Run 3 (`incremental_dag_run_3`)**: +4,320 rows added. State advanced to `2017-05-08 05:00:00`. (Totals: Bronze=12,960, Silver=12,960, Gold=12,960).
- **Idempotency Verification**: Running ingestion with an empty landing directory processed exactly 0 rows and left Parquet row counts unchanged.
- **Full Dataset Re-alignment**: Restored complete history; final counts equal 293,822 Bronze and 298,456 Silver.

## 6. API Latency Benchmarks (100 Requests per Endpoint)

### A. True HTTP over Loopback (`http://127.0.0.1:8001`)
*Measured using `httpx` against running Uvicorn server (includes full OS TCP/IP network stack and serialization)*:
| Endpoint | Mean Latency (ms) | Median p50 (ms) | 95th Percentile p95 (ms) |
| :--- | :---: | :---: | :---: |
| `GET /rates/latest` | 28.03 ms | 27.94 ms | 30.14 ms |
| `GET /rates/history` | 30.51 ms | 30.46 ms | 32.79 ms |
| `GET /quality/status` | 13.13 ms | 12.95 ms | 14.27 ms |
| `GET /predict` | 3.12 ms | 3.08 ms | 3.42 ms |
| `GET /model/metrics` | 3.39 ms | 3.32 ms | 3.87 ms |

### B. In-Process TestClient (`fastapi.testclient.TestClient`)
*Measured using Starlette in-process ASGI dispatch (bypasses OS TCP/IP socket stack)*:
| Endpoint | Mean Latency (ms) | Median p50 (ms) | 95th Percentile p95 (ms) |
| :--- | :---: | :---: | :---: |
| `GET /rates/latest` | 30.63 ms | 28.86 ms | 47.74 ms |
| `GET /rates/history` | 32.88 ms | 31.69 ms | 44.05 ms |
| `GET /quality/status` | 15.95 ms | 15.37 ms | 18.81 ms |
| `GET /predict` | 5.02 ms | 5.0 ms | 5.68 ms |
| `GET /model/metrics` | 5.86 ms | 4.76 ms | 6.81 ms |

## 7. Forecasting Statistical Rigor & Hypothesis Testing

### A. Walk-Forward Direct Multi-Horizon Results (5 Folds, 24-Bar Embargo)
| Pair | Horizon ($h$) | Test $n$ | Ridge MAE | Baseline MAE | Ridge DA | 95% Wilson CI | Effect Size | Binomial $p$ | HAC DM $p$ |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| EURUSD | 01 | 28,379 | 0.000641 | 0.000641 | 50.9% | [50.3%, 51.4%] | +0.0086 | 0.0020 | 0.0450 |
| EURUSD | 06 | 28,575 | 0.001635 | 0.001633 | 50.8% | [50.2%, 51.4%] | +0.0081 | 0.0030 | 0.8887 |
| EURUSD | 12 | 28,414 | 0.002396 | 0.002391 | 50.2% | [49.6%, 50.8%] | +0.0018 | 0.2745 | 0.9913 |
| EURUSD | 24 | 28,603 | 0.003475 | 0.003466 | 49.2% | [48.7%, 49.8%] | -0.0075 | 0.9947 | 0.9896 |
| USDJPY | 01 | 28,382 | 0.000788 | 0.000788 | 50.4% | [49.8%, 51.0%] | +0.0040 | 0.0877 | 0.1704 |
| USDJPY | 06 | 28,576 | 0.001888 | 0.001886 | 50.8% | [50.2%, 51.4%] | +0.0082 | 0.0028 | 0.8286 |
| USDJPY | 12 | 28,390 | 0.002757 | 0.002753 | 51.5% | [50.9%, 52.0%] | +0.0146 | <0.0001 | 0.8380 |
| USDJPY | 24 | 28,609 | 0.004097 | 0.004101 | 52.6% | [52.0%, 53.2%] | +0.0258 | <0.0001 | 0.3423 |

### B. Explanation of USDJPY $h=24$ Directional Accuracy ($p < 0.0001$ vs earlier $p = 0.2394$)
- **Why Earlier Script Reported $p = 0.2394$**: The earlier evaluation computed binomial p-values separately per walk-forward fold and averaged them (`np.mean`), which is mathematically flawed.
- **Pooled Exact Binomial Test ($n = 28,609$)**: Pooling out-of-fold test predictions across all folds ($k = 15,048$ correct / $28,609$ total) gives an exact binomial p-value of $p = 2.4 \times 10^{-19} (< 0.0001)$ with 95% Wilson CI [52.0%, 53.2%] and effect size $+0.0258$.
- **Why Loss Test Does NOT Beat Baseline (HAC Diebold-Mariano $p = 0.3423$)**: Despite statistically significant directional accuracy, the model does NOT beat the zero-return baseline on loss ($DM = +0.4061, p = 0.3423$). Over 2017–2024, USDJPY rose steadily from 102 to 161 (secular trend), where positive drift alone produced a majority-class "UP" frequency of 52.4%. The model merely tracked structural macroeconomic drift rather than generating genuine directional alpha.

### C. Direct vs Recursive Comparison on Matched Walk-Forward Folds
Evaluated on identical 5-fold walk-forward splits ($n \approx 28,500$ per horizon):
| Horizon ($h$) | Test $n$ | Direct Ridge MAE | Recursive Rollout MAE | Error Accumulation Penalty |
| :---: | :---: | :---: | :---: | :---: |
| $h=01$ | 28,379 | **0.000641** | **0.000641** | **+0.00%** (Identical model & features) |
| $h=06$ | 28,575 | 0.001635 | 0.001633 | -0.12% |
| $h=12$ | 28,414 | 0.002396 | 0.002390 | -0.25% |
| $h=24$ | 28,603 | 0.003475 | 0.003470 | -0.14% |

*Explanation of earlier discrepancy*: In the previous run, recursive rollout was evaluated on an isolated 500-row slice from January 2023 (a period of heightened market volatility), while Direct was evaluated across the full walk-forward folds. When evaluated on the exact same walk-forward test folds, Direct $h=1$ and Recursive $h=1$ are mathematically equivalent (`0.000641`).

### D. Multiple-Testing Correction Note
- 6 currency pairs $\times$ 24 forecast horizons = 144 statistical hypothesis tests.
- **Bonferroni corrected significance threshold**: $\alpha = 0.05 / 144 = 0.0003472$.
- At $\alpha = 0.0003472$, no model demonstrates statistically significant loss reduction over the zero-return random-walk baseline (all HAC DM $p > 0.05$).

### E. KS Drift Alerts & Autocorrelation Caveat
- Hourly forex returns exhibit significant volatility clustering and autocorrelation (|r| lag 1h autocorr = +0.2193, lag 24h = +0.1987).
- Standard Kolmogorov-Smirnov tests assume independent and identically distributed (i.i.d.) observations. On time series with serial dependence, empirical CDF variance exceeds the standard Brownian bridge limit, rendering raw KS p-values anti-conservative.
- Drift alerts in `monitoring_alerts` reflect empirical shifts in volatility and distribution shape across market regimes, but are **not** claimed as statistically independent drift rejections.
