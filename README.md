# ⚡ Weather Desk — Institutional-Grade Weather Intelligence Terminal

> **Live dashboard:** [yieldchaser.github.io/weather-dd-tracker](https://yieldchaser.github.io/weather-dd-tracker)

An automated, production-hardened weather analytics platform purpose-built for **natural gas market trading**. The system tracks gas-weighted Degree Days (HDD/CDD/TDD), multi-model consensus, weather regime classifications, and wind generation anomalies — updated automatically 4× per day via GitHub Actions and hosted on GitHub Pages.

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
- **How it works:** Fetches and Z-score normalizes AO, NAO, PNA, and EPO indices against their full 1950+ historical distributions. It calculates a composite cold risk score based on specific threshold rules (e.g., Negative AO + Positive PNA = High Bullish Risk) and identifies the top 3 analog years using Euclidean distance between current and historical state vectors.
- **Data Source:** [NOAA CPC CPC Daily Indices](https://ftp.cpc.ncep.noaa.gov/cwlinks/) and [NOAA PSL EPO Monitoring](https://downloads.psl.noaa.gov/Public/map/teleconnections/).

### 2. Weather Regime Classifier
Classifies the current atmospheric configuration into distinct weather patterns (e.g., Arctic Trough, Polar Vortex, Pacific Ridge).
- **How it works:** Uses the `Herbie` library to ingest GFS 0.25° Z500 geopotential height fields. The fields are projected onto a pre-trained PCA space and classified via KMeans clustering. A Markov transition matrix is then applied to forecast the probability of regime shifts over the next 24-72 hours.
- **Data Source:** NOAA GFS 0.25° (via NOMADS/AWS).

### 3. Freeze-Off Trigger
Monitors natural gas wellhead freeze risk across 6 major US producing basins.
- **How it works:** Tracks 2m temperature forecasts for the Permian, Haynesville, Barnett, Eagle Ford, Fayetteville, and SW Marcellus. Alerts are tiered into `EMERGENCY / WARNING / WATCH` based on lead time and required consensus between GFS and ECMWF models. If lead hours are <24 and both models agree on <0°C, an Emergency alert is triggered.
- **Data Source:** GFS (via Herbie) and ECMWF (via `ecmwf-opendata`).

### 4. Dynamic Sensitivity Coefficient
Calculates the real-time relationship between heating degree days (HDD) and gas demand (Bcf).
- **How it works:** Performs a 30-day rolling OLS (Ordinary Least Squares) regression of weekly EIA net storage withdrawals against population-weighted HDDs. This produces a `Bcf/HDD` coefficient that the system uses to weight the market impact of forecast changes.
- **Data Source:** [EIA v2 Storage API](https://api.eia.gov/v2/natural-gas/stor/wkly/data/).

### 5. Wind Power Forecast (Upgraded)
Provides a high-resolution outlook for US wind generation and potential gas-burn displacement.
- **How it works:** Aggregates 15 geographically diverse wind nodes weighted by installed GW capacity. Wind speeds are converted to Capacity Factor (CF) via an IEC Class II power curve. Results are bucketed into sub-daily periods (**Peak / Off-Peak / Shoulder**) to identify specific daytime generation drought risks.
- **Data Source:** [Open-Meteo Forecast & Ensemble APIs](https://api.open-meteo.com/v1/).

### 6. Live Grid Monitor
Tracks real-time fuel mix and incremental natural gas burn across the "Big 4" ISOs.
- **How it works:** Queries the EIA v2 API for hourly generation data in ERCOT, PJM, MISO, and SPP. It maintains a 30-day rolling baseline for each fuel type to compute real-time anomalies. A "NATIONAL" aggregate is synthesized to show the total gas-displacement impact of wind/solar surges or dropouts.
- **Data Source:** [EIA v2 Electricity Data](https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/).

### 7. Composite Weather Signal
Integrates multi-system outputs into a single directional market bias banner.
- **How it works:** Uses a heuristic "accumulator" that weights contributions from Teleconnections, Freeze-Off alerts, Wind droughts, and Regime labels. A "Confidence Score" is calculated based on the percentage of upstream systems reporting real-time (non-stale) data.
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

## 📅 Data Sources & Frequency

| Source | Endpoint | Variables Used | Frequency |
|---|---|---|---|
| **Open-Meteo Forecast** | `api.open-meteo.com/v1/forecast` | t2m, ws_100m, cloudcover | 4x daily |
| **Open-Meteo Historical**| `historical-forecast-api.open-meteo.com/v1/forecast` | t2m, ws_100m | Monthly (climo) |
| **Open-Meteo Ensemble** | `ensemble-api.open-meteo.com/v1/ensemble` | GFS_CFS (35d), ens_mean | 2x daily |
| **EIA v2 API** | `api.eia.gov/v2/` | Storage, Fuel-mix, Withdrawals | Hourly/Weekly |
| **NOAA CPC** | `ftp.cpc.ncep.noaa.gov/cwlinks/` | AO, NAO, PNA indices | Daily |
| **NOAA PSL** | `downloads.psl.noaa.gov/Public/map/...` | EPO index (dam anomalies) | Daily |
| **Kaggle** | `kaggle.com` | FourCastNetV2 GPU Inference | 2x daily |

---

## ⚙️ Automated Workflows

| Workflow | Schedule (UTC) | Action |
|---|---|---|
| `daily_run.yml` | 04/10/16/22:00 | **Primary Pipeline**: Polls models, calculates TDDs, and triggers AI inference. |
| `system5_wind.yml` | 07:30 | Wind generation forecast and sub-daily drought tracking. |
| `system2_regimes.yml` | 07:00 | Daily weather regime classification and Markov pathing. |
| `system_composite.yml` | 08:30 | Updates translated integrated signal banner. |
| `system4_sensitivity.yml` | 08:00 | Rolling HDD→demand coefficient regression. |
| `teleconnections.yml` | 06:00, 18:00 | Fetches updated NOAA teleconnection indices & analogs. |
| `retrain_regimes.yml` | 01-01 / 04-01 / 07-01 / 10-01 02:00 | Re-trains KMeans model on latest ERA5 history. |

---

## 🧹 Repository Maintenance

Automated self-pruning via `scripts/cleanup_repo.py`:
- **Spatial Maps:** Retains last 10 GIFs per model.
- **Sub-seasonal Data:** Limits GEFS sub-seasonal folders to the 3 most recent runs.
- **Footprint:** Targets repository size of **~750 MB**.

---

## 🗂️ Key Output Reference

| Path | Description |
|---|---|
| `outputs/composite_signal.json` | Final integrated intelligence signal and confidence. |
| `outputs/wind/drought.json` | Wind drought probabilities and peak-period risk alerts. |
| `outputs/regimes/current_regime.json` | Current regime, persistence, and transition forecasts. |
| `outputs/teleconnections/latest.json` | Z-scored indices and historical cold risk. |
| `outputs/tdd_master.csv` | Master HDD/CDD timeseries across all models and horizons. |
| `outputs/live_grid_generation.csv` | EIA ISO fuel mix and natural gas burn data. |
| `outputs/vs_normal.csv` | Comparative data for forecasts vs. 10y and 30y normals. |

---

*Built with Python, xarray, scikit-learn, PapaParse, Chart.js, GitHub Actions, and GitHub Pages.*