# Data Sources, Licensing, and Calendar Audit Documentation

## 1. Primary Data Source: HistData.com Historical Forex Feeds
* **Source Platform**: [HistData.com](https://www.histdata.com/)
* **Format**: Generic ASCII 1-Minute (M1) bar quotes, resampled to 1-hour (1h) OHLCV bars.
* **Pairs Covered**: EURUSD, GBPUSD, USDJPY, AUDUSD, USDCAD, USDCHF (2017-01-01 to 2024-12-31).

### Terms of Service, Licensing & Automated Download Policies
* **Exact Wording on Data Nature & Personal/Research Use**:
  > *"Because the data is provided free of charge, HistData.com provides no warranty or certification regarding its accuracy or reliability. Users download and use the data at their own will and risk. HistData.com is not selling the data itself; any contributions or fees are strictly convenience fees to offset bandwidth, hosting, and traffic overhead."*
* **Download Terms & Automated Access**:
  - The standard web interface provides free downloads per instrument/year.
  - To protect server infrastructure, aggressive automated scraping without delay is restricted to prevent denial-of-service. HistData provides optional paid SFTP/FTP access for bulk uncapped retrieval.
  - **Compliance in this Pipeline**:
    - Programmatic access used `requests` session queries via the standard `histdata` Python library.
    - Rate limiting: **Strict polite pause of 1.0 second** between annual chunk downloads. Total download across all 48 annual archives (6 pairs × 8 years) took ~3 minutes with 0 failed HTTP requests or throttling events.
    - All raw M1 archives (`data/downloads_m1/`) and converted CSV datasets (`data/external/`) are strictly kept on local disk and excluded from version control via `.gitignore`. **No redistribution of HistData data occurs.**

---

## 2. Timezone Specification & Non-Farm Payrolls (NFP) Empirical Proof

### HistData Timezone Documentation
According to HistData.com's official specifications and FAQ:
> *"The time zone used for all historical data files is Eastern Standard Time (EST) (UTC-5) WITHOUT Daylight Saving Time (DST) adjustments."*

* **Conversion Methodology**:
  - Because HistData timestamps are in New York time, mapping to UTC requires accounting for whether the trading day fell within summer Daylight Saving Time (EDT, UTC-4) or winter Standard Time (EST, UTC-5).
  - In our ingestion pipeline, timestamps are localized using `US/Eastern` (which automatically models DST switchovers for the New York session) and converted to UTC:
    $$\text{Timestamp}_{\text{UTC}} = \text{tz\_convert}(\text{tz\_localize}(\text{Timestamp}_{\text{ET}}, \text{'US/Eastern'}), \text{'UTC'})$$

### Empirical Proof: US Non-Farm Payrolls (NFP) Volatility Spike
* **Event**: US Non-Farm Payrolls release occurs on the **first Friday of each calendar month at exactly 08:30 New York time**.
* **Expected UTC Hour**:
  - During **Summer (EDT)**: 08:30 New York = **12:30 UTC** $\rightarrow$ Intraday return volatility spike must peak in the **12:00 UTC** hourly bar.
  - During **Winter (EST)**: 08:30 New York = **13:30 UTC** $\rightarrow$ Intraday return volatility spike must peak in the **13:00 UTC** hourly bar.

#### Empirical Measurement (EURUSD, 2017–2024, 96 NFP Release Days):
Average absolute hourly log-return $|\ln(C_t / C_{t-1})|$ by UTC hour:

| UTC Hour | Summer Months (May–Sep) NFP Return Volatility | Winter Months (Nov–Feb) NFP Return Volatility |
| :---: | :---: | :---: |
| 08:00 UTC | 0.000717 | 0.000593 |
| 09:00 UTC | 0.000519 | 0.000737 |
| 10:00 UTC | 0.000441 | 0.000433 |
| 11:00 UTC | 0.000700 | 0.001001 |
| **12:00 UTC** | **0.002576** ⚡ *(Peak Summer Spike: +268%)* | 0.000757 |
| **13:00 UTC** | 0.001416 | **0.002180** ⚡ *(Peak Winter Spike: +188%)* |
| 14:00 UTC | 0.001212 | 0.001527 |
| 15:00 UTC | 0.000875 | 0.001423 |
| 16:00 UTC | 0.000834 | 0.001191 |
| 17:00 UTC | 0.000498 | 0.000536 |
| 18:00 UTC | 0.000385 | 0.000598 |

**Conclusion**: The volatility peak precisely hits **12:00 UTC in summer** and shifts by exactly one hour to **13:00 UTC in winter**, proving that the timestamp conversion and DST alignment are empirically accurate.

---

## 3. Resampling Specification & Anti-Lookahead Guarantee

The transformation from 1-minute (M1) ticks/bars to 1-hour (1h) OHLCV uses standard Pandas time-series resampling:
```python
hourly = df_m1.resample('1h').agg({
    'Open': 'first',     # First tick/minute open price in [HH:00, HH:59]
    'High': 'max',       # Maximum price reached in [HH:00, HH:59]
    'Low': 'min',        # Minimum price reached in [HH:00, HH:59]
    'Close': 'last',     # Last tick/minute close price in [HH:00, HH:59]
    'Volume': 'sum'      # Total tick count across the hour
}).dropna()
```
* **Timestamp Labeling**: Bars are labeled with the **start of the hourly interval** (e.g. `12:00:00` represents trades between `12:00:00` and `12:59:59`).
* **Point-in-Time Integrity**:
  - The bar for interval $t = [T, T+1\text{h})$ is only complete at time $T+1\text{h}$.
  - In feature engineering and forecasting, any feature computed for forecasting horizon $h$ at decision time $T$ strictly uses data observed up to and including $T$, and targets $r(t+h)$ evaluate changes strictly after $T$. No future bar prices leak into historical features.

---

## 4. Market Calendar & Gap Audit (Empirical Gap Rule)

### Gap Classification Rule
A time gap $\Delta t = t_k - t_{k-1} > 1\text{ hour}$ between consecutive recorded bars is classified into two disjoint sets:
1. **Expected Weekend Closure**:
   - The gap spans across the weekend: starts on **Friday evening** ($19:00 \le \text{Hour} \le 22:00\text{ UTC}$) and ends on **Sunday evening** ($20:00 \le \text{Hour} \le 23:00\text{ UTC}$).
   - Gap duration: typically 47–52 hours (~410 occurrences across 8 years).
   - Expected closures are **never** treated as missing data anomalies and are **never** forward-filled.
2. **Unexpected Open-Session Gaps**:
   - Any gap $> 1\text{ hour}$ that does not satisfy the weekend transition criteria.
   - Forward-fill rule (cap: 3 hours) applies strictly to these unexpected open-session gaps, logging all filled intervals to `fill_log`.

### Gap Audit per Currency Pair (2017–2024)

| Pair | Total Bars | Gaps > 1h | Expected Weekend Closures | Unexpected Open-Session Gaps |
| :--- | :---: | :---: | :---: | :---: |
| **AUDUSD** | 48,966 | 1,101 | 406 | 695 |
| **EURUSD** | 48,986 | 1,112 | 406 | 706 |
| **GBPUSD** | 48,968 | 1,110 | 404 | 706 |
| **USDCAD** | 48,970 | 1,090 | 404 | 686 |
| **USDCHF** | 48,965 | 1,108 | 406 | 702 |
| **USDJPY** | 48,967 | 1,124 | 405 | 719 |

---

## 5. Investigation of 2023 Bar Shortfall (~5,400 vs ~6,200 bars)

### Findings
* Across all 6 currency pairs, the annual bar count in 2023 is **5,394 to 5,409 bars**, whereas other years average **6,207 to 6,243 bars**.
* The shortfall represents a deficit of **~815 missing hourly bars** (13.1% reduction in 2023).

### Evidence of Source-Side Gap
1. **Archive Completeness**: The downloaded annual archives `DAT_ASCII_{PAIR}_M1_2023.zip` span the full calendar year from **2023-01-01 17:00 EST** to **2023-12-29 17:00 EST**. No whole months or calendar days are missing.
2. **HistData Status Report Documentation**:
   - Inside each official HistData 2023 zip file, the companion file `DAT_ASCII_{PAIR}_M1_2023.txt` includes a status report documenting tick omissions.
   - The status report records hundreds of intra-day gaps with message:
     > *"Gap of 1548s found between 20230101171615 and 20230101174210... Average tick interval: 5536 milliseconds. Maximum tick interval found: 7216883 milliseconds."*
3. **Temporal Distribution**:
   - The missing hours in 2023 are concentrated in **March through July 2023**, where the broker feed experienced low-liquidity gaps (14 bars/day average instead of normal 20 bars/day).
   - Of the ~680 unexpected gaps in 2023, **591 gaps are exactly 2 hours long** and **85 gaps are 3 hours long**.
   - Because this omission exists inside the upstream broker source files across all currency pairs simultaneously, it is an immutable property of HistData's 2023 feed. It will be cleanly handled by our 3-hour forward-fill logic and logged to `fill_log`.
