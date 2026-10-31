# Data Sources & Licensing Documentation

## Primary Dataset: HistData.com Historical 1-Minute Aggregations
* **Source Website**: [HistData.com](https://www.histdata.com/)
* **Format Acquired**: Generic ASCII 1-Minute (M1) bar quotes (Bid/Ask/Mid), resampled to 1-hour (1h) OHLCV bars via Pandas.
* **Pairs Requested**: EURUSD, GBPUSD, USDJPY, AUDUSD, USDCAD, USDCHF (2017-01-01 to 2024-12-31).
* **Licensing / Terms of Service**:
  - The historical data is provided free of charge for personal use, strategy backtesting, research, and analysis.
  - HistData.com explicitly clarifies that they do not sell the underlying data; fees charged on their portal are convenience/bandwidth donations.
  - Mass automated scraping or public redistributing of their pre-packaged archives via competing archives is restricted by their Terms of Service.
  - **Compliance in this Repository**:
    - Data files are downloaded locally directly into `data/external/` and `data/downloads_m1/`.
    - Both directories are strictly excluded from version control via `.gitignore`.
    - No proprietary HistData compressed archives or raw dumps are redistributed within Git commits.
* **Timezone & Normalization**:
  - **Source Timezone**: Eastern Time (US/Eastern, New York time: EST UTC-5 / EDT UTC-4 with daylight saving adjustments).
  - **Target Timezone**: Coordinated Universal Time (UTC).
  - **Transformation Applied**: Localized with `US/Eastern` taking into account daylight saving transitions, converted to `UTC`, and stripped of timezone metadata to produce standard clean ISO-8601 UTC timestamps (`YYYY-MM-DD HH:MM:SS`).
* **Volume Characteristics**:
  - HistData volume is **Tick Count** (the number of tick price changes recorded within that time window), **not** institutional traded cash/lot volume.
  - Used as an indicator of relative intraday liquidity and market activity.

---

## Secondary Dataset: Dukascopy Historical Data Feed (Reference / Alternative)
* **Source Website**: [Dukascopy Bank SA](https://www.dukascopy.com/)
* **Terms & Restrictions**:
  - Dukascopy provides market data exports for non-commercial personal analysis and backtesting.
  - Automated bots scraping bulk data without permission violate their general Terms of Use (TCU). 
  - Kept as an alternative reference in case HistData.com coverage is unavailable.

---

## Legacy Sample / Synthetic Dataset
* **Location**: `data/raw/` (`EURUSD_1h_2017-01-01_2024-12-31.csv`, `GBPUSD_1h_...`, `EURUSD_1d_...`)
* **Characteristics**:
  - Exhibits continuous 24/7 bars (including full weekends) without market gap closures.
  - Synthetic price trajectories (EURUSD decaying down to ~0.011, GBPUSD to ~1.82).
* **Role in Pipeline**:
  - Maintained strictly as a test fixture to ensure the Quality Gate detects price plausibility anomalies, zero-gap conditions, and weekend activity flags.
  - Excluded from all forecasting models and real-world analytics.

