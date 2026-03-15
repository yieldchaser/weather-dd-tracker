# ⚡ Weather Desk — Institutional-Grade Weather Intelligence Terminal

> **Live dashboard:** [yieldchaser.github.io/weather-dd-tracker](https://yieldchaser.github.io/weather-dd-tracker)

An automated, production-hardened weather analytics platform purpose-built for **natural gas market analysis**. The system tracks gas-weighted Degree Days (HDD/CDD/TDD), multi-model consensus, weather regime classifications, and wind generation anomalies — updated automatically 4× per day via GitHub Actions and hosted on GitHub Pages.

---

## 🏗️ System Architecture

The platform operates as a decentralized intelligence pipeline. Raw atmospheric data is ingested from global meteorological centers, processed into market-relevant metrics (population-weighted degree days), and passed through a series of "Intelligence Systems" to produce actionable signals.

- **Frontend:** Dual-dashboard interface using vanilla JS and Chart.js.
    - `index.html`: **Weather Desk** (Macro-weather, HDDs, regimes, and teleconnections).
    - `grid.html`: **Power Grid Monitor** (Real-time burn, wind impact, and forward generation).
- **Backend:** Python-based state machine running on GitHub Actions.
- **Persistence:** JSON/CSV flat-file database stored directly in the repository.

---

## 🚀 Setup & Installation

### Prerequisites
- Python 3.10+
- GitHub account with Actions enabled

### Local Development
```bash
git clone https://github.com/yieldchaser/weather-dd-tracker
cd weather-dd-tracker
pip install -r requirements.txt

# Run individual pipeline components
export EIA_KEY=your_key_here
python scripts/poll_models.py               # fetch latest model data
python scripts/wind/forecast_wind_power.py  # run wind forecast
```

### GitHub Actions Secrets
| Secret | Description |
|---|---|
| `EIA_KEY` | Official EIA v2 API key for live grid data and storage withdrawals. |
| `TELEGRAM_TOKEN` | Bot API token for dispatching weather alerts to mobile. |
| `TELEGRAM_CHAT_ID` | Target channel or user ID for Telegram notifications. |
| `KAGGLE_USERNAME` | Kaggle account name for triggering GPU-accelerated AI inference. |
| `KAGGLE_KEY` | Kaggle API key for kernel authentication. |
| `GITHUB_TOKEN` | (Automatic) Standard GHA permission for repository commits. |

---

## 🧠 The 9 Intelligence Systems

### 1. Teleconnections & Analogs
Identifies historical cold risk by monitoring standard climate indices and matching them to past winter analogs.
- **How it works:** Fetches and Z-score normalizes AO, NAO, PNA, and EPO indices against their full 1950+ historical distributions. It calculates a composite cold risk score based on specific threshold rules (e.g., Negative AO + Positive PNA = High Bullish Risk) and identifies the top 3 analog years with **enriched historical context**, including specific HDD anomalies and realized winter outcomes (e.g., "Record warm March", "Late season freeze").
- **Data Source:** [NOAA CPC CPC Daily Indices](https://ftp.cpc.ncep.noaa.gov/cwlinks/) and [NOAA PSL EPO Monitoring](https://downloads.psl.noaa.gov/Public/map/teleconnections/).

### 2. Weather Regime Classifier
Classifies the current atmospheric configuration into distinct weather patterns (e.g., Arctic Trough, Polar Vortex, Pacific Ridge).
- **How it works:** Uses the `Herbie` library to ingest GFS 0.25° Z500 geopotential height fields. The fields are projected onto a pre-trained PCA space and classified via KMeans clustering. A Markov transition matrix is then applied to forecast the probability of regime shifts over the next 24-72 hours.
- **Data Source:** NOAA GFS 0.25° (via NOMADS/AWS).

### 3. Freeze-Off Trigger (Hardened)
Monitors natural gas wellhead freeze risk across 6 major US producing basins with high-reliability fetching logic.
- **How it works:** Tracks 2m temperature forecasts for the Permian, Haynesville, Barnett, Eagle Ford, Fayetteville, and SW Marcellus.
    - **Reliability Engine**: Features a **Herbie retry-with-backoff** mechanism (3 attempts, 10min wait) to handle delayed model index files.
    - **Staggered Fetch**: Schedule is timed (`06:30/18:30 UTC`) specifically to allow for NCEP index file generation.
    - **Partial Success Reporting**: Gracefully handles individual source failures (GFS or ECMWF), allowing the system to provide "Partial" alerts instead of failing entirely.
- **Data Source:** GFS (via Herbie) and ECMWF (via `ecmwf-opendata`).

### 4. Dynamic Sensitivity Coefficient
Calculates the real-time relationship between heating degree days (HDD) and gas demand (Bcf).
- **How it works:** Performs a 30-day rolling OLS (Ordinary Least Squares) regression of weekly EIA net storage withdrawals against population-weighted HDDs. This produces a `Bcf/HDD` coefficient that the system uses to weight the market impact of forecast changes.
- **Data Source:** [EIA v2 Storage API](https://api.eia.gov/v2/natural-gas/stor/wkly/data/).

### 5. Wind & Solar Renewable Power Forecast (Upgraded)
Provides a high-resolution outlook for US renewable generation and potential gas-burn displacement.
- **How it works:** 
    - **Wind**: Aggregates 15 wind nodes (110 GW) using an IEC Class II power curve. Incorporates **GFS Ensemble Spread** (P10/P90 bands) to quantify forecast uncertainty.
    - **Solar**: Tracks 12 geographically diverse solar nodes (~48 GW) across ERCOT, WECC, PJM, MISO, and SPP. Uses a PVWatts-style model converting GHI from **GFS and ECMWF** to Capacity Factor using a **75% Performance Ratio**. (ECMWF AIFS excluded — model does not publish solar radiation variables.)
    - **Drought Consensus**: Identifies "Renewable Droughts" when Wind CF < 35% and Solar Consensus < 25% (requires both solar models below threshold).
    - **Model Agreement Score**: Quantifies confidence based on agreement between GFS, ECMWF, and ICON; higher scores indicate lower regime-shift risk.
    - **Combined Signal**: Synthesizes a unified directional bias based on aggregate **gas displacement loss (GW)** vs. 2-year climatology and **Model Agreement** scoring across GFS, ECMWF, and ICON to quantify forecast confidence.
    - **Seasonal Drought Adjustment**: Thresholds vary by season to reflect structural generation patterns:
        - **Wind drought**: 35% CF winter → 30% shoulder → 25% summer.
        - **Solar drought**: 15% CF winter → 25% shoulder → 35% summer.
        - **Wind drought signal weight**: 1.0x heating → 0.8x shoulder → 0.6x cooling.
    - **35-Day Outlook**: Extends the horizon via GFS/CFS Ensemble API multi-node batching.
- **Data Source:** [Open-Meteo Forecast & Ensemble APIs](https://api.open-meteo.com/v1/).

### 6. Live Grid Monitor
Tracks real-time fuel mix, load, and incremental natural gas burn across 7 major ISOs: **ERCOT, PJM, MISO, SPP, CAISO, ISONE, and NYISO**.
- **How it works:** Queries the EIA v2 API for hourly generation and demand (load) data in ERCOT, PJM, MISO, SPP, CAISO, ISONE, and NYISO. It maintains a 30-day rolling baseline for each fuel type and load profile to compute real-time anomalies. A "NATIONAL" aggregate is synthesized to show the total gas-displacement impact of renewable surges and peaker utilization.
- **Data Source:** [EIA v2 Electricity Data](https://api.eia.gov/v2/electricity/rto/).

### 7. Composite Weather Signal
Integrates multi-system outputs into a single directional market bias banner with detailed catalyst attribution.
- **How it works:** Uses a heuristic "accumulator" that weights contributions from Teleconnections, Freeze-Off alerts, Wind droughts, and Regimes.
    - **Nuanced Polar Vortex Logic**: Distinguishes between "PV Disruption/Strengthening" (Bullish, cold air descending) and "Strong/Established PV" (Bearish, cold locked in Arctic).
    - **Bulls/Bears Breakdown**: Telegram alerts include a full component-level breakdown of individual catalysts and their contributing scores (e.g., "PV Disruption (+2.5)", "WND Drought (+1.2)").
- **Data Source:** Aggregated outputs of Systems 1–6.

### 8. Physics vs. AI Disagreement Index
Measures forecast uncertainty and market volatility risk through model divergence.
- **How it works:** Compares the degree-day (TDD) consensus of physics-based models (GFS, ECMWF) against state-of-the-art AI models (AIFS, GraphCast, Pangu). A "Volatility Risk Score" is produced based on the variance between these two ensembles; higher divergence signals a higher risk of large forecast revisions.
- **Data Source:** Combined `outputs/tdd_master.csv`.

### 9. Market Bias Composite
Calculates a final quantitative score for the next 15 days.
- **How it works:** Applies the Dynamic Sensitivity Coefficient to degree-day anomalies to calculate expected Bcf deviations. The score is further modified by wind premiums (bullish for droughts) and power burn multipliers, then capped between -1.0 (Strong Bear) and +1.0 (Strong Bull).
- **Data Source:** Integrated market logic pipeline.

---

## ❄️ Seasonal Adaptation

The system is fully season-aware, dynamically adjusting thresholds and logic based on the current month:

- **Dashboard Intelligence**: Automatically switches between **HDD/CDD/TDD** logic and labels (Nov-Mar = HDD, May-Sep = CDD, Apr+Oct = TDD).
- **Threshold Dynamics**: All wind and solar drought thresholds adjust by season to maintain relevance against structural climo shifts.
- **Gas Burn Sensitivity**: MW to Bcf/d conversion adapts for summer peaker dispatch efficiency shifts (7,000 → 8,200 BTU/kWh).
- **Composite Signal weighting**: Adjusts the importance of specific signals (like wind) based on their seasonal correlation to total gas demand.

---

## 📅 Data Sources & Frequency

| Source | Endpoint | Variables Used | Frequency |
|---|---|---|---|
| **Open-Meteo Forecast** | `api.open-meteo.com/v1/forecast` | t2m, ws_100m, cloudcover, **direct_radiation, diffuse_radiation** | 4x daily |
| **Open-Meteo Historical**| `historical-forecast-api.open-meteo.com/v1/forecast` | t2m, ws_100m | Monthly (climo) |
| **Open-Meteo Ensemble** | `ensemble-api.open-meteo.com/v1/ensemble` | GFS_CFS (35d), ens_mean | 2x daily |
| **EIA v2 API** | `api.eia.gov/v2/` | Storage, Fuel-mix (NG, COL, NUC, WND, SUN), Withdrawals | Hourly/Weekly |
| **EIA v2 Region Data** | `api.eia.gov/v2/electricity/rto/region-data/data/` | Total load demand (D type) by ISO | Hourly |
| **EIA v2 Outages** | `api.eia.gov/v2/electricity/outages/generators/data/` | Nuclear and coal generator outages | Daily |
| **NOAA CPC** | `ftp.cpc.ncep.noaa.gov/cwlinks/` | AO, NAO, PNA indices | Daily |
| **NOAA PSL** | `downloads.psl.noaa.gov/Public/map/...` | EPO index (dam anomalies) | Daily |
| **Kaggle** | `kaggle.com` | FourCastNetV2 GPU Inference | 2x daily |

---

## ⚙️ Automated Workflows

| Workflow | Schedule (UTC) | Action |
|---|---|---|
| `daily_run.yml` | 04/10/16/22:00 | **Primary Pipeline**: TDDs, plus Grid metrics (`fetch_live_grid`, `fetch_outages`, `fetch_peaker_proxy`). |
| `system3_freeze.yml` | 06:30, 18:30 | **Hardened Freeze Check**: 16-day basin-level freeze monitoring with retry logic. |
| `system5_wind.yml` | 07:30 | Wind & Solar generation forecast and sub-daily drought tracking. |
| `system2_regimes.yml` | 07:00 | Daily weather regime classification and Markov pathing. |
| `system_composite.yml` | 08:30 | Updates translated integrated signal banner. |
| `system4_sensitivity.yml` | 08:00 | Rolling HDD→demand coefficient regression. |
| `teleconnections.yml` | 06:00, 18:00 | Fetches updated NOAA teleconnection indices & analogs. |
| `retrain_solar_climo.yml` | 06:00 (1st of month) | Monthly rebuild of solar peak-hour climatology from GFS history. |
| `retrain_regimes.yml` | 01-01 / 04-01 / 07-01 / 10-01 02:00 | Re-trains KMeans model on latest ERA5 history. |

---

## 🧹 Repository Maintenance

Automated self-pruning via `scripts/cleanup_repo.py`:
- **Spatial Maps:** Retains last 10 GIFs per model.
- **Sub-seasonal Data:** Limits GEFS sub-seasonal folders to the 3 most recent runs.
- **Footprint:** Targets repository size of **~750 MB**.

---

## 🛠️ Reliability & Health Monitoring

The system implements a multi-layered hardening strategy to ensure high availability and data integrity:

- **State-Preserving Guards**: Every pipeline script is wrapped in a top-level exception handler. If a script fails (e.g., API timeout), it captures the error, alerts the user, and **halts before overwriting data**, ensuring the dashboard always displays the "last known good" state.
- **Centralized Health Status**: Scripts report their status to `outputs/health/{script_name}.json`. These logs track success/failure, timestamps, and specific error messages.
- **Telegram Health Alerts**: The messaging system (`send_telegram.py`) scans all health status files before dispatching alerts. Any system failures are prepended to the top of the message: `🚨 SYSTEM HEALTH ALERTS 🚨`.
- **Safe-Write Wrappers**: Replaced standard file-writing with custom logic that validates dataframes (e.g., minimum row counts) before committing to disk, preventing "empty file" corruption.

---

## 📊 Power Grid Charts
The Power Grid Monitor dashboard consumes several rolling history files to visualize market relationships:
- **Wind Generation Forecast**: Multi-model outlook for the next 16 days, including **GFS Ensemble Spread Bands** (P10/P90) and active drought alert markers.
- **National Load vs Gas Generation**: Dual-axis line chart tracking total grid demand against natural gas dispatch.
- **Nuclear Fleet Availability**: Monitors national generator outages to identify potential secondary gas demand from reliability gaps.
- **ISO Regional Breakdown**: Stacked horizontal breakdown of today's natural gas burn across all 7 tracked ISOs.
- **Gas Peaker Utilization**: Tracks the peak-to-offpeak gas generation ratio identifying marginal peaker dispatch.

---

## 🗂️ Key Output Reference

| Path | Description |
|---|---|
| `outputs/composite_signal.json` | Final integrated intelligence signal and confidence. |
| `outputs/wind/drought.json` | Wind drought probabilities, GFS ensemble spread, and Model Agreement scores. |
| `outputs/gas_burn_history.csv` | Rolling 3-year history of national gas burn (Bcf/d) vs temperature for scatter analysis. |
| `outputs/thermal_history.csv` | Historical record of national gas, coal, and nuclear generation MW and gas % metrics. |
| `outputs/wind/solar_power_forecast.csv` | Daily solar generation forecasts across GFS and ECMWF models. |
| `outputs/wind/solar_climo_30d.json` | 2-year solar capacity factor climatology (Peak-hour and All-day). |
| `outputs/wind/combined_drought.json` | Unified renewable drought risk, consensus indicators, and gas displacement metrics. |
| `outputs/wind/wind_power_forecast.csv` | Wind generation forecasts including GFS members (P10/P90) and climo anomalies. |
| `outputs/wind/wind_actuals_history.csv` | Persistent log of historical national and ISO-level wind generation actuals. |
| `outputs/tdd_master.csv` | Master HDD/CDD timeseries across all models and horizons. |
| `outputs/live_grid_generation.csv` | EIA ISO fuel mix (7 ISOs + NATIONAL), load_mw, gas_pct_load, and nuclear_mw. |
| `outputs/grid_outages.csv` | Daily nuclear and coal outage tracking, fleet availability %. |
| `outputs/peaker_history.csv` | Daily peak vs off-peak gas generation ratio, peaker utilization proxy. |
| `outputs/hourly_grid_data.csv` | Intraday hourly national gas generation, passed to peaker proxy script. |
| `outputs/vs_normal.csv` | Comparative data for forecasts vs. 10y and 30y normals. |
| `outputs/health/` | JSON status files for every pipeline script, used for health monitoring and Telegram alerts. |

---

## 🔮 Planned Improvements

### Power Grid Monitor (`grid.html`)
- **Historical Wind Actuals Overlay** — overlay observed wind generation actuals on the forward wind forecast chart. Activates automatically once `wind_actuals_history.csv` accumulates 14+ days (~March 27).
- **Model Skill Scoring** — track how each model's 5-day wind/HDD forecast verified against observed actuals. Rolling 30-day MAE per model displayed on dashboard.
- **LNG Export Tracker** — track LNG feedgas demand (~15+ Bcf/d) as a competing demand signal alongside power burn.
- **Production vs Demand Balance** — daily supply/demand balance showing surplus/deficit, tying all signals together.

### Weather Desk (`index.html`)
- **Analog Enrichment** — include HDD magnitude and outcome descriptions for top analogs.
- **Composite Catalyst Transparency** — add full Bulls/Bears breakdown to the dashboard UI (currently in Telegram only).

### Storage & Production Dashboard (Planned Separate Page)
- **EIA Storage Actuals vs Model-Implied Withdrawal** — ground truth feedback loop validating whether HDD signals moved the market correctly.
- **Production tracker** — Appalachian, Permian, Haynesville basin-level production vs prior year.
- **Supply/demand balance sheet** — integrated daily Bcf/d supply vs demand with surplus/deficit signal.

### Infrastructure
- **Git history size reduction** — implement `git filter-repo` to squash old large GRIB files from git history, reducing clone size.

---

*Built with Python, xarray, scikit-learn, PapaParse, Chart.js, GitHub Actions, and GitHub Pages.*