# Weather DD Tracker

**A fully automated, trading-grade US natural gas weather analytics pipeline.**

Weather DD Tracker solves the problem of manually tracking and interpreting atmospheric model output for energy market participants trading Natural Gas derivatives (Henry Hub). It automates the entire chain from raw GRIB weather model data → gas-weighted degree day calculations → market bias signals → Telegram alerts.

> *This software provides programmatic weather summaries based on freely available datasets. It constitutes neither financial advice nor a recommendation to execute transactions in commodity futures or any other asset class.*

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Tech Stack](#2-tech-stack)
3. [Project Structure](#3-project-structure)
4. [Prerequisites](#4-prerequisites)
5. [Installation & Setup](#5-installation--setup)
6. [Environment Variables](#6-environment-variables)
7. [How to Run](#7-how-to-run)
8. [Features](#8-features)
9. [Architecture Overview](#9-architecture-overview)
10. [CSV Data Schemas](#10-csv-data-schemas)
11. [API Reference](#11-api-reference)
12. [Known Issues / Limitations](#12-known-issues--limitations)
13. [Contributing Guidelines](#13-contributing-guidelines)

---

## 1. Project Overview

Natural gas demand is highly weather-sensitive. Traders need to know, in near-real-time, whether the current weather forecast is **bullish** (colder than normal → more heating demand) or **bearish** (warmer than normal → less demand). Weather DD Tracker automates this entire workflow.

**Key differentiators:**

- **Gas-weighted precision** — Temperature data is spatially weighted by EIA county-level natural gas consumption density, not a simple geographic average. The Northeast/Midwest (which consume ~70% of national gas in winter) are weighted proportionally higher.
- **Multi-model consensus** — Tracks 14+ weather models simultaneously (physics + AI) and computes disagreement indices as volatility proxies.
- **Run-to-run change tracking** — The speed and direction of model revisions (not just absolute values) is the primary trading signal.
- **Fully free infrastructure** — GitHub Actions (compute) + Kaggle API (GPU for AI models) + NOMADS/ECMWF OpenData (data sources).

---

## 2. Tech Stack

### Backend — Python 3.12

| Library | Purpose |
|---|---|
| `pandas` | DataFrame operations, CSV I/O, data merging |
| `numpy` | Numerical arrays, weight grids, interpolation |
| `xarray` | N-dimensional array operations on GRIB data |
| `cfgrib` | GRIB2 file reading (weather model binary format) |
| `eccodes` | ECMWF GRIB encoding/decoding library |
| `ecmwf-opendata` | ECMWF official Python SDK for data download |
| `requests` | HTTP calls to NOMADS, ECMWF, Telegram API |
| `beautifulsoup4` | HTML parsing (NOMADS directory scraping) |
| `pdfminer.six` | PDF parsing (EIA reports) |
| `openpyxl` | Excel output (`.xlsx` threshold matrices) |
| `matplotlib` | Chart generation (PNG maps, cumulative charts) |
| `cartopy` | Geospatial map projections (CONUS maps) |
| `imageio` | GIF animation generation (spatial delta maps) |
| `pytz` | Timezone handling |
| `selenium` + `webdriver-manager` | Browser automation (legacy) |
| `kaggle` | Kaggle API client (trigger/poll GPU kernels) |

### Frontend — HTML5 / CSS3 / Vanilla JavaScript (ES2020+)

No build step, no framework. CDN-loaded libraries:

| Library | Version | Purpose |
|---|---|---|
| `PapaParse` | 5.4.1 | Client-side CSV parsing from `outputs/` directory |
| `Chart.js` | latest | Interactive line/bar charts |

### Infrastructure

| Component | Role |
|---|---|
| GitHub Actions | Master scheduler/orchestrator (cron + event-driven) |
| Kaggle Kernels API | Free NVIDIA T4/P100 GPU for AI model inference |
| NOMADS (NOAA) | GFS, HRRR, NAM, GEFS data source |
| ECMWF OpenData | ECMWF IFS HRES, ENS, AIFS data source |
| Open-Meteo API | Fallback JSON API (ECMWF, GFS, ICON, ARPEGE) |
| Telegram Bot API | Delivery of trading-grade alerts |

---

## 3. Project Structure

```
weather-dd-tracker/
│
├── .github/
│   └── workflows/
│       └── daily_run.yml          # GitHub Actions CI/CD workflow (cron: 0 4,10,16,22 UTC)
│
├── data/
│   ├── us_daily_normals.csv       # 365-day national temperature normals
│   ├── pipeline_state.json        # State tracker: last processed run IDs + timestamps
│   ├── README.md                  # Placeholder
│   ├── ecmwf/                     # ECMWF IFS HRES TDD outputs (YYYYMMDD_HH_tdd.csv)
│   ├── cmc_ens/                   # Canadian CMC Ensemble TDD outputs
│   ├── ai_models/                 # AI model inference outputs (from Kaggle GPU)
│   │   ├── ai_tdd_latest.csv      # Latest AI model TDD (all AI models combined)
│   │   └── fourcastnetv2-small_tdd.csv  # FourCastNetV2 specific output
│   └── gefs_subseasonal/          # GEFS subseasonal GRIB2 files (raw, gitignored)
│       └── YYYYMMDD_HH/           # Per-run directory of GRIB2 binary files
│
├── scripts/
│   ├── poll_models.py             # Entry point: smart poller, checks for new model runs
│   ├── daily_update.py            # Master orchestrator: 10-step sequential pipeline
│   ├── fetch_ecmwf_ifs.py         # ECMWF IFS HRES fetcher (OpenData SDK)
│   ├── fetch_gfs.py               # GFS fetcher (NOMADS byte-range)
│   ├── fetch_ecmwf_aifs.py        # ECMWF AIFS AI model fetcher
│   ├── fetch_ecmwf_ens.py         # ECMWF ENS 51-member fetcher
│   ├── fetch_gefs.py              # GEFS 31-member fetcher
│   ├── fetch_hrrr.py              # HRRR 3km fetcher
│   ├── fetch_nam.py               # NAM 12km fetcher
│   ├── fetch_icon.py              # ICON via Open-Meteo
│   ├── fetch_nbm.py               # National Blend of Models fetcher
│   ├── fetch_open_meteo.py        # Open-Meteo fallback (17 demand-weighted cities)
│   ├── fetch_historical_eia_normals.py  # EIA normals refresh
│   ├── compute_tdd.py             # GRIB→CSV: computes tdd + tdd_gw per day
│   ├── merge_tdd.py               # Glob all *_tdd.csv + deduplicate → tdd_master.csv
│   ├── select_latest_run.py       # Extract latest run per model
│   ├── compare_to_normal.py       # HDD/CDD anomaly vs 10Y/30Y normals
│   ├── compute_run_delta.py       # Day-by-day delta between consecutive runs
│   ├── run_change.py              # Per-run totals + sequential change
│   ├── build_gas_weights.py       # EIA gas-weight grid + seasonal GW normals
│   ├── build_true_gw_grid.py      # EIA county-level consumption density raster
│   ├── build_model_shift_table.py # GFS vs ECMWF consensus matrix
│   ├── build_freeze_offs.py       # US basin freeze-off predictor (MMcf/d)
│   ├── build_crossover_matrix.py  # HDD/CDD seasonal crossover metrics
│   ├── track_cumulative_season.py # Cumulative winter HDDs vs prior years
│   ├── build_historical_threshold_matrix.py  # 21-yr MB threshold matrix
│   ├── build_historical_monthly_charts.py    # Historical monthly chart data
│   ├── plot_ecmwf_eps.py          # ECMWF ensemble plume chart
│   ├── generate_maps.py           # Spatial run-to-run delta GIF maps
│   ├── send_telegram.py           # Telegram trading-grade alert sender
│   ├── trigger_kaggle.py          # Kaggle API webhook trigger
│   ├── compare_runs.py            # Legacy GFS-only comparison
│   ├── plot_gfs_tdd.py            # Standalone chart generator
│   └── market_logic/
│       ├── physics_vs_ai_disagreement.py  # Volatility/disagreement index (0–100)
│       ├── power_burn_proxy.py            # CDD-weighted power burn proxy
│       ├── renewables_generation_proxy.py # Wind dropout signal
│       └── composite_score.py            # −1.0..+1.0 Bull/Bear composite score
│
├── outputs/                       # Generated at runtime by GitHub Actions; served to dashboard
│   ├── tdd_master.csv             # All models × all runs × all dates
│   ├── ecmwf_latest.csv           # Latest ECMWF run
│   ├── gfs_latest.csv             # Latest GFS run
│   ├── vs_normal.csv              # Per-day anomaly (simple + GW)
│   ├── run_change.csv             # Total TDD per run + sequential delta
│   ├── run_delta.csv              # Day-by-day delta between runs
│   ├── model_shift_table.csv      # GFS vs ECMWF day-by-day shift matrix
│   ├── freeze_off_forecast.csv    # Predicted MMcf/d production loss
│   ├── seasonal_crossover.csv     # HDD/CDD season transition metrics
│   ├── composite_bull_bear_signal.csv  # Market bias signal
│   ├── convergence_alert.csv      # Multi-model convergence alerts
│   ├── live_grid_generation.csv   # Real-time power grid data (ERCOT + National)
│   ├── historical_degree_days.csv # Historical DD data for spaghetti charts
│   ├── historical_hdd_thresholds.xlsx  # 21-yr dynamic MB threshold matrix
│   ├── crossover_chart.png        # Seasonal crossover visual
│   ├── cumulative_hdd_tracker.png # Winter pace vs 10Y/30Y
│   ├── maps_manifest.json         # Index of available spatial delta GIF maps
│   └── maps/                      # Spatial run-to-run delta GIF animations
│
├── index.html                     # Weather Desk dashboard (primary view, GitHub Pages)
├── grid.html                      # Power Grid Monitor dashboard (secondary view)
├── CODE_ANALYSIS.md               # Detailed script-by-script analysis + issue log
├── MASTER_ARCHITECTURE_PLAN.md    # High-level architecture vision + layer definitions
├── README.md                      # This file
├── requirements.txt               # Python dependencies
└── .gitignore                     # Excludes GRIB files, large binaries, secrets
```

---

## 4. Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.12+ | All pipeline scripts |
| `libeccodes-dev` system library | Required by `cfgrib` for GRIB2 decoding |
| GitHub account | For GitHub Actions automation |
| Kaggle account | For free GPU inference (AI models) |
| Telegram Bot | For alert delivery |
| EIA API key | For gas consumption weight data |

Install the system library on Ubuntu/Debian:

```bash
sudo apt-get install libeccodes-dev
```

---

## 5. Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd weather-dd-tracker
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install the system dependency:**
   ```bash
   sudo apt-get install libeccodes-dev
   ```

4. **Configure GitHub Actions secrets** — Add all required secrets to your repository (see [Environment Variables](#6-environment-variables)).

5. **Enable GitHub Pages** — Configure the repository to serve from the root or `docs/` branch. The `index.html` and `grid.html` dashboards will be served statically.

6. **Automated execution** — The pipeline runs automatically via the GitHub Actions cron schedule. No further manual configuration is required.

---

## 6. Environment Variables

All secrets are stored as GitHub Actions repository secrets. No `.env` file is used.

| Variable | Purpose |
|---|---|
| `TELEGRAM_TOKEN` | Telegram Bot API authentication token |
| `TELEGRAM_CHAT_ID` | Target Telegram channel/chat ID for alert delivery |
| `KAGGLE_USERNAME` | Kaggle account username for API authentication |
| `KAGGLE_KEY` | Kaggle API key for kernel push/poll operations |
| `EIA_KEY` | EIA (Energy Information Administration) API key for gas consumption data |
| `GITHUB_TOKEN` | Auto-provided by GitHub Actions for git commit/push operations |

---

## 7. How to Run

### Automated (Recommended)

The pipeline runs automatically via GitHub Actions at **`0 4,10,16,22 UTC`** daily (every 6 hours). It also triggers on push to `main` and supports manual workflow dispatch. A concurrency group (`weather-poller`) prevents overlapping runs.

### Manual Execution

```bash
# Check for new model runs
python scripts/poll_models.py

# Run the full pipeline (if new data found)
python scripts/daily_update.py

# Generate spatial delta maps
python scripts/generate_maps.py
```

### Pipeline Steps

`daily_update.py` executes the following 10-step sequential pipeline:

| Step | Script(s) | Description |
|---|---|---|
| 1 | 10 fetchers via `ThreadPoolExecutor` (5 workers) | Parallel data fetch from NOMADS, ECMWF, Open-Meteo |
| 2 | `compute_tdd.py` | GRIB2 → CSV conversion with gas weighting |
| 3 | `merge_tdd.py` | Deduplicated master CSV (`tdd_master.csv`) |
| 4 | `select_latest_run.py` | Latest run per model |
| 5 | `compare_to_normal.py` | HDD/CDD anomaly vs 10Y/30Y normals |
| 6 | `run_change.py` + `compute_run_delta.py` | Run-to-run change tracking |
| 7 | Feature trackers | Model shift, freeze-offs, crossover, cumulative, threshold matrix, EPS plot |
| 8 | Market logic | Disagreement index, power burn proxy, wind anomaly, composite score |
| 9 | `send_telegram.py` | Telegram alert delivery |
| 10 | `git commit + push` | Outputs committed back to repo for GitHub Pages |

**Kaggle GPU inference** (major cycles only):

```
trigger_kaggle.py → Kaggle NVIDIA T4/P100 GPU
  → data/ai_models/*.csv → merged into tdd_master.csv
```

---

## 8. Features

### 1. Gas-Weighted Degree Days (TDD/HDD/CDD)
Temperature data is spatially weighted by EIA county-level natural gas consumption density on a 101×241 CONUS grid. Produces both simple and gas-weighted metrics (`tdd`, `tdd_gw`, `hdd`, `hdd_gw`, `cdd`, `cdd_gw`).

### 2. 14+ Model Tracking
Simultaneous tracking of physics and AI weather models:

| Category | Models |
|---|---|
| Physics (deterministic) | ECMWF HRES, GFS, HRRR, NAM, ICON, NBM |
| Physics (ensemble) | GEFS (31-member), ECMWF ENS (51-member), CMC ENS |
| AI models | ECMWF AIFS, FourCastNetV2, PanguWeather, GraphCast |
| Subseasonal | GEFS Subseasonal (35-day) |

### 3. Run-to-Run Change Tracking
Detects and quantifies how much each model revision changes the forecast. The speed and direction of model revisions is the primary trading signal.

### 4. Physics vs AI Disagreement Index (0–100)
Measures divergence between traditional NWP models and AI models as a volatility proxy. Computed by `scripts/market_logic/physics_vs_ai_disagreement.py`.

### 5. Composite Bull/Bear Signal (−1.0 to +1.0)
Synthesizes all model data into a single directional market bias score. Computed by `scripts/market_logic/composite_score.py`.

### 6. Telegram Trading Alerts
Near-term (D1–7) and extended (D8–14) split, consecutive trend counter, multi-model spread, conviction label. Delivered via `scripts/send_telegram.py`.

### 7. Fast Revision Alert
Detects rapid forecast changes between consecutive model runs. Output: `outputs/run_change.csv` with `fast_revision` flag.

### 8. Convergence Alert
Identifies when multiple weather models converge on similar forecasts, indicating higher confidence. Output: `outputs/convergence_alert.csv`.

### 9. Seasonal Cumulative Charts (Spaghetti Chart)
Visualizes cumulative HDDs across all forecast models vs historical norms. Output: `outputs/historical_degree_days.csv`.

### 10. Historical Monthly Charts with 10y/30y Normals
Compares current forecast periods against 10-year and 30-year historical averages. Enables pace analysis vs long-term norms.

### 11. Model Diagnostics (Run Comparison Charts)
Tracks run-to-run changes across all models, showing forecast stability and revision patterns. Output: `outputs/run_delta.csv` and `outputs/model_shift_table.csv`.

### 12. Spatial Delta Maps (GIF Animations)
GIF animations showing run-to-run temperature change across CONUS. Generated by `scripts/generate_maps.py`. Indexed via `outputs/maps_manifest.json`.

### 13. Interactive Dashboard (GitHub Pages)
Static SPA (`index.html`) with Chart.js visualizations. Features:
- Season-aware rendering: auto-switches between HDD (Nov–Mar), CDD (May–Sep), TDD (Apr, Oct)
- Loads 9 CSV files + 1 JSON manifest in parallel via `Promise.all()`
- Cache-busting via `?t=Date.now()` on every fetch
- No server-side rendering; all data loaded client-side via `PapaParse`

### 14. Free Infrastructure
The entire pipeline runs on GitHub Actions (free for public repos) + Kaggle free GPU tier. No paid compute required.

---

## 9. Architecture Overview

### 7-Layer Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Layer 7: DELIVERY                                              │
│  Telegram Bot Alerts │ GitHub Pages Dashboard │ Excel/CSV Export│
├─────────────────────────────────────────────────────────────────┤
│  Layer 6: MARKET SIGNALS                                        │
│  composite_score.py │ physics_vs_ai_disagreement.py            │
│  power_burn_proxy.py │ renewables_generation_proxy.py          │
├─────────────────────────────────────────────────────────────────┤
│  Layer 5: AI MODELS (Kaggle GPU)                                │
│  Track A: ECMWF AIFS (no GPU, GitHub Actions native)           │
│  Track B: FourCastNetV2, PanguWeather, GraphCast (NVIDIA T4)   │
├─────────────────────────────────────────────────────────────────┤
│  Layer 4: ENSEMBLE INTELLIGENCE                                 │
│  GEFS (31-member) │ ECMWF ENS (51-member) │ CMC ENS            │
│  GEFS Subseasonal (35-day)                                      │
├─────────────────────────────────────────────────────────────────┤
│  Layer 3: NORMAL vs FORECAST ENGINE                             │
│  compare_to_normal.py │ NOAA CPC 1981-2010 normals             │
│  10-yr (2014–2023) + 30-yr (1991–2020) benchmarks              │
├─────────────────────────────────────────────────────────────────┤
│  Layer 2: MODEL CHANGE & DISAGREEMENT ENGINE                    │
│  run_change.py │ compute_run_delta.py │ build_model_shift_table │
├─────────────────────────────────────────────────────────────────┤
│  Layer 1: CORE PHYSICS WEATHER ENGINE                           │
│  ECMWF HRES │ GFS │ HRRR │ NAM │ ICON │ NBM │ GEFS │ ECMWF ENS│
│  Data sources: NOMADS, ECMWF OpenData, Open-Meteo (fallback)   │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
GitHub Actions (cron: 0 4,10,16,22 UTC)
  └─► poll_models.py
        checks pipeline_state.json vs NOAA/ECMWF servers
        │
        ├─► [NEW_DATA_FOUND=true] daily_update.py
        │     ├─► Parallel fetch (10 fetchers, ThreadPoolExecutor, 5 workers)
        │     ├─► compute_tdd.py       (GRIB2 → CSV + gas weights)
        │     ├─► merge_tdd.py         (→ tdd_master.csv)
        │     ├─► select_latest_run.py
        │     ├─► compare_to_normal.py
        │     ├─► run_change.py + compute_run_delta.py
        │     ├─► Feature trackers
        │     │     (model shift, freeze-offs, crossover, cumulative,
        │     │      threshold matrix, EPS plot)
        │     ├─► Market logic
        │     │     (disagreement index, power burn, composite score)
        │     ├─► send_telegram.py     (Telegram alert)
        │     └─► git commit + push    (outputs/ → GitHub Pages)
        │
        └─► [Major cycles only] trigger_kaggle.py
              └─► Kaggle NVIDIA T4/P100 GPU inference
                    └─► data/ai_models/*.csv → merged into tdd_master.csv
```

### Frontend Architecture

- `index.html` and `grid.html` are static SPAs with no server-side rendering
- All data loaded client-side via `Papa.parse()` with `download: true` from relative `outputs/` paths
- Cache-busting via `?t=Date.now()` on every fetch
- Season-aware rendering: auto-switches between HDD (Nov–Mar), CDD (May–Sep), TDD (Apr, Oct)
- `index.html` loads 9 CSV files + 1 JSON manifest in parallel via `Promise.all()`

---

### Data Sources Loaded by Frontend

The `index.html` dashboard loads the following 10 data sources:

| # | File | Description |
|---|---|---|
| 1 | `tdd_master.csv` | Master dataset with all models × all runs × all dates |
| 2 | `composite_bull_bear_signal.csv` | Algorithmic market bias score (−1.0 to +1.0) |
| 3 | `vs_normal.csv` | Per-day anomaly vs 10Y/30Y normals (simple + gas-weighted) |
| 4 | `run_delta.csv` | Day-by-day delta between consecutive model runs |
| 5 | `run_change.csv` | Total TDD per run + sequential change + fast revision flag |
| 6 | `model_shift_table.csv` | GFS vs ECMWF consensus matrix |
| 7 | `convergence_alert.csv` | Multi-model convergence alerts |
| 8 | `historical_degree_days.csv` | Historical data for spaghetti chart visualization |
| 9 | `maps_manifest.json` | Index of available spatial delta GIF maps |
| 10 | `pipeline_state.json` | State tracker: last processed run IDs + timestamps |

#### Backend Data Sources (Pipeline Processing)

The following data sources are processed by the backend pipeline:

| Source | Files | Description |
|---|---|---|
| ECMWF ensemble runs | `data/ecmwf/*_tdd.csv` | Multiple ECMWF IFS ensemble runs with various initialization times |
| CMC ensemble | `data/cmc_ens/*_tdd.csv` | Canadian Meteorological Centre ensemble TDD outputs |
| GEFS Subseasonal | `data/gefs_subseasonal/*_tdd.csv` | NOAA GEFS 35-day subseasonal forecasts |
| AI models | `data/ai_models/*.csv` | FourCastNetV2 and AI TDD model outputs |

---

## 10. CSV Data Schemas

### Physics Models — `data/ecmwf/*_tdd.csv`

```
date, mean_temp, hdd, cdd, tdd, mean_temp_gw, hdd_gw, cdd_gw, tdd_gw, model, run_id
```

### AI Models — `data/ai_models/ai_tdd_latest.csv`

```
date, model, run_id, mean_temp, tdd, tdd_gw
```

### Ensemble Models — `data/cmc_ens/*_tdd.csv`

```
date, mean_temp, tdd, tdd_gw, model, run_id
```

### Field Definitions

| Field | Description |
|---|---|
| `date` | Forecast valid date (YYYY-MM-DD) |
| `mean_temp` | Population-simple mean temperature (°F) |
| `mean_temp_gw` | Gas-consumption-weighted mean temperature (°F) |
| `hdd` | Heating Degree Days (simple) |
| `hdd_gw` | Heating Degree Days (gas-weighted) |
| `cdd` | Cooling Degree Days (simple) |
| `cdd_gw` | Cooling Degree Days (gas-weighted) |
| `tdd` | Total Degree Days (simple) |
| `tdd_gw` | Total Degree Days (gas-weighted) |
| `model` | Model identifier string (e.g., `ecmwf_ifs`, `gfs`) |
| `run_id` | Model initialization timestamp (YYYYMMDD_HH) |

---

## 11. API Reference

This project **does not expose any REST API**. It consumes the following external APIs:

| API | Endpoint Pattern | Purpose |
|---|---|---|
| NOMADS (NOAA) | `https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/` | GFS, HRRR, NAM, GEFS GRIB2 files |
| ECMWF OpenData | Python SDK (`ecmwf-opendata`) | ECMWF IFS HRES, ENS, AIFS |
| Open-Meteo | `https://api.open-meteo.com/v1/forecast` | Fallback JSON weather data (17 cities) |
| Kaggle Kernels API | `kaggle kernels push/status/output` | GPU inference trigger + result download |
| Telegram Bot API | `https://api.telegram.org/bot{TOKEN}/sendMessage` | Alert delivery |
| EIA API | `https://api.eia.gov/v2/` | Energy consumption data for weight grids |

> **Note:** A REST API endpoint is listed as ❌ Pending in `MASTER_ARCHITECTURE_PLAN.md`.

---

## 12. Known Issues / Limitations

### Structural Notes

- `requirements.txt` contains encoding artifacts; actual dependencies are defined in the GitHub Actions workflow install step (`.github/workflows/daily_run.yml`).
- `data/README.md` is a placeholder with only one line.
- GEFS subseasonal GRIB2 files in `data/gefs_subseasonal/` are present locally but excluded from git via `.gitignore` (large binary files).
- The `outputs/` directory is generated at runtime by GitHub Actions and is not present in the committed repository.
- GitHub Actions workflow runs at `0 4,10,16,22 UTC` (every 6 hours); the architecture plan describes 15-minute polling — the workflow uses a concurrency group `weather-poller` to prevent overlapping runs.
- All 12 development phases are marked complete as of the 2026-02-23 audit; Phase 13 is in planning.

### Implemented Features (Previously Marked Pending)

The following were listed as ❌ Pending in earlier architecture docs but are **confirmed active** based on committed data and pipeline state:

| Feature | Evidence |
|---|---|
| GEFS Subseasonal (35-day) | `data/gefs_subseasonal/*_tdd.csv` with model `GEFS_35D`; dashboard handles it explicitly |
| CMC Ensemble | `data/cmc_ens/*_tdd.csv` committed; dashboard lists `CMC_ENS` as a map model |
| FourCastNetV2 (NVIDIA) | `data/ai_models/fourcastnetv2-small_tdd.csv` with live run output |
| Physics vs AI Disagreement Index | `pipeline_state.json`: `run_disagreement: "success"` |
| Power Burn Proxy | `pipeline_state.json`: `run_power_burn: "success"` |
| Wind/Renewables Anomaly | `pipeline_state.json`: `run_wind_anomaly: "success"`; `grid.html` Power Grid Monitor |
| Composite Bull/Bear Score | `pipeline_state.json`: `run_composite: "success"`; dashboard renders it |
| Fast Revision Alert | UI element in `index.html` reads `fast_revision` flag from `run_change.csv` |
| Convergence Alert | UI element in `index.html` reads `convergence_alert.csv` |
| Spatial Run-to-Run Delta Maps | `generate_maps.py` runs every cycle; `index.html` has full `updateMap()` logic |

### Phase 13 — Pending Features

The following features are genuinely not yet implemented (Phase 13 roadmap):

| Feature | Priority |
|---|---|
| ECMWF AIFS committed output (Track A, no GPU) | Highest Priority — fetcher exists but no committed CSV output yet |
| Storage draw/injection weekly estimate (EIA weekly) | Pending |
| Ensemble spread as implied volatility proxy | Pending |
| All-model confidence score (spread-weighted) | Pending |
| UKMET / ARPEGE via Open-Meteo | Pending |
| NVIDIA Earth-2 Medium Range (Atlas) | Pending |
| NVIDIA Earth-2 Nowcasting (StormScope) | Pending |
| Aurora (Microsoft) | Pending |
| REST API endpoint | Pending |

---

## 13. Contributing Guidelines

1. Fork the repository and create a feature branch.
2. Follow the existing script naming convention:
   - `fetch_*.py` — data fetchers
   - `build_*.py` — derived dataset builders
   - `compute_*.py` — numerical computation scripts
3. All new model fetchers must output CSVs conforming to the standard schema:
   ```
   date, mean_temp, hdd, cdd, tdd, mean_temp_gw, hdd_gw, cdd_gw, tdd_gw, model, run_id
   ```
4. Test locally before submitting a pull request.
5. Document any new environment variables in this README under [Environment Variables](#6-environment-variables).

---

*Weather DD Tracker — automated natural gas weather analytics for energy market participants.*
