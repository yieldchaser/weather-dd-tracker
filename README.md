# ⚡ Weather Desk — Institutional-Grade Weather Intelligence Terminal

> **Live dashboard:** [yieldchaser.github.io/weather-dd-tracker](https://yieldchaser.github.io/weather-dd-tracker)

An automated, production-hardened weather analytics platform purpose-built for **natural gas market trading**. Tracks gas-weighted Degree Days (HDD/CDD/TDD), cross-model consensus, weather regime classification, teleconnection indices, and a composite weather intelligence signal — updated automatically 4× per day via GitHub Actions, hosted free on GitHub Pages.

---

## 🏗️ System Architecture

```
NOAA / ECMWF / EIA APIs
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                    GitHub Actions Pipeline                   │
│                                                             │
│  daily_run.yml (04/10/16/22 UTC)  ─── poll_models.py       │
│  system2_regimes.yml (07 UTC)     ─── classify_today.py    │
│  teleconnections.yml              ─── system1_telecon.py   │
│  system_composite.yml             ─── compute_composite.py │
│  system4_sensitivity.yml          ─── sensitivity scripts  │
│  system5_wind.yml                 ─── wind scripts         │
│  retrain_regimes.yml (quarterly)  ─── train_regimes.py     │
└────────────────┬────────────────────────────────────────────┘
                 │ outputs/ (JSON + CSV)
                 ▼
         GitHub Pages (index.html + grid.html)
```

---

## 📡 Data Sources & Update Cadence

| Source | Data | Cadence | Auth Required |
|---|---|---|---|
| NOAA NOMADS | GFS GRIB2 (byte-range, t2m only) | 4× daily | None |
| NOAA NOMADS | HRRR, NAM, NBM, GEFS | 4× daily | None |
| ECMWF Open Data | ECMWF IFS HRES | 2× daily (00z/12z) | None (10-day lag) |
| ECMWF Open Data | ECMWF AIFS (AI model) | 4× daily (00/06/12/18z) | None (real-time, no lag) |
| Open-Meteo Ensemble API | ECMWF ENS, CMC ENS | 2× daily | None |
| AWS S3 (noaa-gefs-pds) | GEFS (16-day), GEFS 35D (sub-seasonal) | 4× daily / daily 00z | None |
| PSL/CPC NOAA | AO, NAO, PNA, EPO teleconnections | Daily | None |
| EIA v2 API | Live grid fuel mix (Big 4 ISOs) | Daily | `EIA_KEY` secret |
| ERA5 (CDS) | Regime model training data | Quarterly retrain | Local file |
| Kaggle (GPU kernel) | AI model inference (FourCastNetV2-Small) | 2× daily (00z/12z) | `KAGGLE_*` secrets |
| **Open-Meteo API** | 79-city demand-weighted TDD | Real-time | None |

---

## 🌦️ Supported Forecast Models

### Short-Range (0–5 Days)
- **HRRR** — 3km High-Resolution Rapid Refresh (NOAA)
- **NAM** — 12km North American Mesoscale (NOAA)
- **NBM** — National Blend of Models (11-day consensus)

### Medium-Range Physics (0–16 Days)
- **GFS** — Global Forecast System (NOAA, 0.25°)
- **ECMWF IFS** — European Centre Operational HRES
- **ECMWF ENS** — European Ensemble (via Open-Meteo `ecmwf_ifs025` ensemble mean API)
- **GEFS** — Global Ensemble Forecast System (NOAA)
- **CMC_ENS** — Canadian Meteorological Centre Ensemble (via Open-Meteo `gem_global_ensemble`)
- **ICON** — Icosahedral Nonhydrostatic (DWD Germany)

### Medium-Range AI (ML Forecasts)
- **ECMWF AIFS** — AI Integrated Forecasting System
- **FourCastNetV2-Small** — NVIDIA Fourier Neural Operator

### Extended / Sub-Seasonal (16–35 Days)
- **GEFS 35D** — NOAA 35-day sub-seasonal ensemble

---

## 🧠 Intelligence Pipeline (7-System Backend)

### System 1 — Teleconnections
**Script:** `scripts/teleconnections/system1_teleconnections.py`
Fetches AO, NAO, PNA, EPO from CPC/PSL. All 4 indices are **z-score normalized** against their full historical distribution (fixes raw EPO dam units). Computes cold risk score and historical analog years.
- Output: `outputs/teleconnections/latest.json`

### System 2 — Weather Regime Classifier
**Scripts:** `scripts/regimes/train_regimes.py` + `classify_today.py`
ERA5 Z500 geopotential anomaly → PCA → KMeans (optimal k selected by silhouette score).
- **Cascading GFS retry:** steps back 6h at a time up to 24h before failing
- **First-order Markov transition matrix** computed at training, stored in pickle, hydrated in JSON
- **Quarterly auto-retrain** via `retrain_regimes.yml`
- Output: `outputs/regimes/current_regime.json`

### System 3 — Freeze-Off Trigger
**Script:** `scripts/freeze/`
Detects natural gas wellhead freeze conditions by monitoring temperature anomalies in major producing basins (Permian, Haynesville, Marcellus, DJ Basin, Appalachian).
- Output: `outputs/freeze/alerts.json`

### System 4 — Dynamic Sensitivity Coefficient
**Script:** `scripts/sensitivity/`
Rolling 30-day regression of HDD → gas demand (EIA storage withdrawals). Outputs a `Bcf/HDD` coefficient that adjusts the composite signal amplitude.
- Output: `outputs/sensitivity/rolling_coeff.json`

### System 5 — Wind Generation Anomaly
**Script:** `scripts/wind/`
Capacity factor anomaly vs 30-day climatology. Wind drought → higher gas burn → bullish.
- Output: `outputs/wind/drought.json`

### System 6 — Live Grid Monitor
**Script:** `scripts/market_logic/fetch_live_grid.py`
EIA v2 API → fuel mix for ERCOT, PJM, MISO, SWPP → NATIONAL aggregate.
- **30s timeout** (handles EIA API latency on MISO/SWPP)
- **Partial aggregation**: missing ISOs excluded from NATIONAL sum, not zeroed
- Output: `outputs/live_grid_generation.csv`

### System 7 — Composite Weather Intelligence Signal
**Script:** `scripts/compute_composite_weather_signal.py`
Integrates all 6 systems into a single `BULLISH / BEARISH / NEUTRAL` signal.
- Confidence = 20% × number of connected upstream systems (0–100%)
- Output: `outputs/composite_signal.json`

### System 8 — Physics vs AI Disagreement Index
**Script:** `scripts/market_logic/physics_vs_ai_disagreement.py`
Compares consensus of physics models (ECMWF, GFS) against AI models (AIFS, GraphCast, Pangu). Divergence signals forecast uncertainty.
- **Volatility Risk Score:** 0–100 based on TDD spread.
- Output: `outputs/physics_vs_ai_disagreement.csv`

### System 9 — Market Bias Composite
**Script:** `scripts/market_logic/composite_score.py`
Combines model agreement, power burn proxies, and wind anomalies into a directional market bias score (-1.0 to +1.0).
- **15d Pct Deviation:** Forecasted TDD vs 10-year normal for the next 15 days.
- Output: `outputs/composite_bull_bear_signal.csv`

---

## 📊 Dashboard Features

### Weather Desk (`index.html`)
| Element | Source | Notes |
|---|---|---|
| Regime Badge (header) | `current_regime.json` | Live Markov probs, clean label parsing |
| Teleconnection Panel | `teleconnections/latest.json` | AO/NAO/PNA/EPO, z-scored, color-coded |
| Intelligence Signal Banner | `composite_signal.json` | Score + component breakdown |
| Algorithmic Market Bias | `composite_bull_bear_signal.csv` | From physics model consensus |
| Fast Revision Alert | `run_change.csv` | >1.0 DD/day threshold |
| Convergence Alert | `convergence_alert.csv` | Multi-model alignment detection |
| HDD Horizon Table | `vs_normal.csv` | Short/Medium/Long-Term vs 30y & 10y normals |
| Model Run Chart | `run_delta.csv` | Top 4 runs vs 30-year normal |
| Shift Matrix | `model_shift_table.csv` | Day-by-day consensus changes |

### Power Grid (`grid.html`)
| Element | Source |
|---|---|
| NATIONAL NatGas Burn | `live_grid_generation.csv` (NATIONAL row) |
| Wind Anomaly | 30-day rolling baseline vs live |
| ERCOT Real-Time | `live_grid_generation.csv` (ERCOT row) |
| Grid Impact Signal | BULLISH (Wind Drought) / BEARISH (Strong Wind) / NEUTRAL |

### Trend Analysis (New)
| Element | Source |
|---|---|
| Historical Monthly Outliers | `build_historical_monthly_charts.py` |
| Season Cumulative (Spaghetti) | `track_cumulative_season.py` |
| Crossover Matrix | `build_crossover_matrix.py` |

---

## ⚙️ Key Engineering Decisions

### Gas-Weighting Methodology
All temperature grids are converted to a **population × gas-consumption weighted average** using `data/weights/conus_gas_weights.npy`. This ensures that a cold snap in sparsely populated Montana contributes less to the HDD signal than the same event over Chicago.

- **Standard models (GFS, ECMWF, GEFS):** bilinear interpolation of weight grid to native model resolution
- **NBM:** nearest-neighbour weight lookup on projected 2D lat/lon grid (Lambert Conformal)
- **AI models:** same weight grid fetched at runtime from GitHub
- **79-City Grid:** Expanded from 17 cities in `demand_constants.py` to improve spatial coverage of the Ohio Valley and Mid-Atlantic.

### 75% Day Coverage Filter
Days with < 75% of their expected hourly/timestep data are excluded from daily averages. Prevents partial-run bias on the first and last days of a forecast.

### Apples-to-Apples Delta Calculation
All model shift comparisons use the **gas-weighted daily average** (`hdd_gw`, `cdd_gw`) not simple grid means, ensuring fair comparison across models with different native resolutions.

### AIFS Completeness Guard
ECMWF AIFS data is checked for completeness at **step=360** (15-day horizon) before triggering a fetch. Checking step=0 only would give a false-positive trigger since the initial analysis field uploads almost immediately — the forecast data takes longer. Partial downloads (GRIB present but no `manifest.json`) are detected and automatically removed for a clean re-fetch.

### Manifest Validation
HRRR and NAM skip guards read and validate the `forecast_hours` list inside each `manifest.json`. An empty list (from a failed run that still wrote a manifest) triggers automatic manifest removal and retry on the next cycle rather than a permanent skip.

### CI Commit Paths
The `daily_run.yml` commit step explicitly adds `data/ecmwf_aifs/*_tdd.csv` and `data/ecmwf_ens/*_tdd.csv`. These paths are separate from the base `data/ecmwf/` path and must be listed individually or they are silently lost when the runner exits.

---

## 🔐 Required GitHub Secrets

| Secret | Used By | Purpose |
|---|---|---|
| `EIA_KEY` | `fetch_live_grid.py`, all workflows | EIA v2 API authentication |
| `TELEGRAM_TOKEN` | `send_telegram.py` | Alert bot token |
| `TELEGRAM_CHAT_ID` | `send_telegram.py` | Target chat/channel |
| `KAGGLE_USERNAME` | `daily_run.yml` | AI model inference |
| `KAGGLE_KEY` | `daily_run.yml` | AI model inference |

---

## 🚀 Local Development

```bash
# Clone
git clone https://github.com/yieldchaser/weather-dd-tracker.git
cd weather-dd-tracker

# Install dependencies
pip install -r requirements.txt

# Run individual pipeline components
export EIA_KEY=your_key_here

python scripts/regimes/classify_today.py        # Regime + Markov
python scripts/teleconnections/system1_teleconnections.py
python scripts/market_logic/fetch_live_grid.py
python scripts/compute_composite_weather_signal.py

# Full pipeline
python scripts/poll_models.py
python scripts/compute_tdd.py
python scripts/merge_tdd.py
```

---

## 📅 Automated Workflow Schedule

| Workflow | Schedule (UTC) | Action |
|---|---|---|
| `daily_run.yml` | 04:00, 10:00, 16:00, 22:00 | Fetch all models, compute TDD, update dashboard |
| `system2_regimes.yml` | 07:00 | GFS Z500 → classify regime → update JSON |
| `teleconnections.yml` | Daily | AO/NAO/PNA/EPO fetch |
| `system_composite.yml` | Daily | Composite signal integration |
| `system4_sensitivity.yml` | Daily | Rolling HDD→demand coefficient |
| `system5_wind.yml` | Daily | Wind capacity factor anomaly |
| `retrain_regimes.yml` | Jan/Apr/Jul/Oct 1st | Quarterly ERA5 regime model retrain |

All workflows support **manual trigger** via GitHub Actions → Run workflow.

---

## 🗂️ Output File Reference

```
outputs/
├── composite_signal.json          # 7-system intelligence signal
├── composite_bull_bear_signal.csv # Physics model market bias
├── teleconnections/latest.json    # AO/NAO/PNA/EPO (z-scored)
├── regimes/current_regime.json    # Regime + Markov transition probs
├── freeze/alerts.json             # Freeze-off wellhead alerts
├── sensitivity/rolling_coeff.json # Bcf/HDD demand coefficient
├── wind/drought.json              # Wind capacity factor anomaly
├── live_grid_generation.csv       # EIA fuel mix (Big 4 ISOs + NATIONAL)
├── tdd_master.csv                 # Master HDD/CDD timeseries (all models)
├── vs_normal.csv                  # Forecast vs 30y & 10y normals
├── run_delta.csv                  # Run-to-run model deltas
├── model_shift_table.csv          # Day-by-day consensus shift matrix
├── physics_vs_ai_disagreement.csv # System 8 Volatility Proxy
├── historical_monthly_charts/     # Monthly anomaly trend PNGs
└── maps/                          # Spatial run-to-run delta GIFs

data/weights/
├── conus_gas_weights.npy          # 2D gas consumption weight grid
├── conus_gas_weights_meta.json    # Grid metadata (lat/lon/resolution)
└── regime_model.pkl               # PCA + KMeans + Markov matrix (quarterly)
```

---

## 🔮 Upgrade Paths (When Budget Allows)

| Upgrade | Cost | Benefit |
|---|---|---|
| Small VPS ($5/mo) | ~$60/yr | 6-hourly pipeline, every GFS cycle |
| ECMWF subscription | ~$500/yr | Real-time ECMWF (removes 10-day lag) |
| Calibrated confidence | Free | Backtest hit-rate per signal level |
| Live EIA storage API | Free | Same-day withdrawal estimates |

---

*Built with Python, xarray, scikit-learn, PapaParse, Chart.js, GitHub Actions, and GitHub Pages.*