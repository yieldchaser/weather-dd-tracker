# 🌦️ Weather DD Tracker

**Free. Automatable. Resilient.**

Weather DD Tracker is a professional-grade, automated pipeline designed to translate meteorological physics into actionable energy market signals. It focuses on Natural Gas thermal demand (HDD/CDD) and Power Burn anomalies, leveraging a distributed architecture across GitHub Actions and Kaggle to maintain high-frequency updates at zero infrastructure cost.

---

## 🏗️ Technical Infrastructure

The system operates as a distributed event-driven pipeline:

1.  **Orchestration (GitHub Actions)**:
    -   **Poller (`scripts/poll_models.py`)**: The primary entry point. Every 6 hours, it synchronizes with NOAA and ECMWF servers to detect new 00z, 06z, 12z, or 18z runs.
    -   **Cycle Logic**: Triggers full AI inference only on **Major Cycles (03-09z and 15-21z)** to optimize Kaggle GPU quotas. Minor cycles focus on physics updates.
    -   **Environment**: Built on Python 3.12 with `cfgrib`, `xarray`, and `eccodes` for efficient byte-range GRIB2 parsing.

2.  **GPU Inference Engine (Kaggle API)**:
    -   **Kernel (`scripts/kaggle_env/`)**: Spin up NVIDIA T4 GPUs to run heavy AI models (e.g., `fourcastnetv2-small`).
    -   **Efficiency**: Subsets global data to CONUS at the source to prevent OOM errors and manages memory nuking between model runs.

3.  **Data Persistence**:
    -   Uses Git as a database. `pipeline_state.json` tracks last processed runs, while `outputs/` stores historical CSVs, PNG diagrams, and XLSX reports.

---

## 📡 Seven-Layer Data Architecture

### Layer 1: Physics Weather Engine
High-resolution processing of deterministic and ensemble models:
- **Primary**: ECMWF IFS HRES (9km), GFS (28km).
- **Secondary/Ensemble**: GEFS (31 mem), ECMWF ENS (51 mem), CMC ENS.
- **Short-Range**: HRRR (3km), NAM (12km hourly), ICON, NBM.

### Layer 2: Model Change & Disagreement Engine
- **Inter-Run Shifts**: Generates the `model_shift_table.csv` comparing latest vs. prior runs with a strict 3-day interpolation limit to prevent "data poisoning."
- **Convergence Alert**: Automatically flags when multiple major models align in a single direction (>0.5 HDD shift).

### Layer 3: Benchmark & Anomaly Engine
- **Gas-Weighted Normals**: NOAA CPC 1981-2010 benchmarks scaled by seasonal residential/commercial consumption factors.
- **Market Signal**: BULLISH / BEARISH indicators derived from anomaly magnitude vs. historical distributions.

### Layer 4: Ensemble Spread & Probability
- *Under Active Development*: Tracking Euro/GFS ensemble consensus and volatility proxies.

### Layer 5: AI-Native Models
- **Track A (Native)**: ECMWF AIFS (runs on GHA CPU).
- **Track B (GPU)**: `fourcastnetv2-small` (runs on Kaggle T4). Designed for 0-15 day horizons.

### Layer 6: Derived Market Signals
- **Thermal Demand (HDD/CDD)**: Weighted by a 48-state Gaussian grid (`data/weights/`) representing gas-sensitive heating demand.
- **Grid Wind Anomaly**: Real-time tracking of the "Big 4" ISOs (**ERCOT, PJM, MISO, SWPP**) via EIA v2 API. Calculates national wind droughts impacting gas-burn for power.
- **Historical Magnitude Matrix**: Monthly ranking of current demand vs. history (detecting Top 5 extreme years).

### Layer 7: Intelligent Delivery
- **Telegram Bot**: Automated multi-channel alerts featuring "Fast Revision" flags (>15 HDD cumulative shift) and algorithmic bias summaries.

---

## 📁 Core Repository Map

- `/scripts`: The engine room.
  - `poll_models.py`: Entry point; state manager.
  - `compute_tdd.py`: Thermal demand logic using interpolated Gaussian weights.
  - `build_gas_weights.py`: Generates the national demand grid based on EIA state consumption.
  - `demand_constants.py`: 17-city representative weights used for AI and localized analysis.
  - `market_logic/`: ISO grid fetching and composite bull/bear scoring.
- `/outputs`: Final data products (Ready for Power Query/Excel ingestion).

---

## ⚙️ Setup & Requirements

- **Python 3.12**: Core requirement.
- **Dependencies**: `pip install -r requirements.txt`. 
  - *Note: `pdfminer`, `selenium`, and `beautifulsoup4` are included in the requirements for scheduled pipeline expansion but are not consumed by the current production core.*
- **API Keys**: Requires `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`, `KAGGLE_USERNAME/KEY`, and `EIA_KEY` (EIA v2).

---

## 🚀 Usage

The pipeline is fully autonomous via GitHub Actions. For local testing:
```bash
# To trigger a full cycle manually:
python scripts/poll_models.py
```
Outputs will refresh in the `outputs/` directory.