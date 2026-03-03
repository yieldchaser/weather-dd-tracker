# Weather Desk: Institutional-Grade Weather Terminal

An algorithmic weather dashboard designed for institutional natural gas trading. This terminal tracks gas-weighted Degree Days (HDD/CDD), cross-model consensus, and market bias signals across physics-based ensembles and machine-learning (AI) forecast models.

## 🌦️ Supported Models & Horizons
The terminal synchronizes data from over a dozen global and regional models, providing a comprehensive view of meteorological evolution:

*   **Short-Range (0-5 Days):** 
    *   **HRRR:** 48h High-Resolution Rapid Refresh
    *   **NAM:** 84h North American Mesoscale
    *   **NBM:** National Blend of Models (Full 11-day consensus)
*   **Medium-Range Physics (0-16 Days):**
    *   **ECMWF / GFS:** Operational global standards
    *   **CMC_ENS / GEFS / ICON:** Physics-based ensembles
*   **Medium-Range AI (ML Forecasts):**
    *   **FourCastNetV2-Small:** NVIDIA-backed machine learning model
    *   **ECMWF_AIFS:** ECMWF's Artificial Intelligence Integrated Forecasting System
*   **Extended (Seasonal):**
    *   **GEFS_35D:** 35-day subseasonal ensemble

## 📊 Core Features & UI
The terminal provides refined visualizations and tables optimized for rapid decision-making:

*   **Model Consensus Shift Table:** Normalizes run-to-run changes to **Average HDDs/Day**. Unlike raw totals, this allows traders to compare a 2-day HRRR shift against a 16-day GFS shift on an apples-to-apples basis.
*   **Historical Magnitude Matrix:** Ranks the current forecast against the last 30 years of weather history. Instantly identifies "Top 5 Coldest/Hottest" regimes for historical context.
*   **75% Stability Filter:** Automatically drops forecast days from averages if they contain <75% of their expected hourly data. This prevents "First-Hour Spikes" where a partial morning run could spoof a massive daily shift.
*   **Gap Flags (*):** Orange asterisk identifies long time-jumps between model cycles, helping traders distinguish fresh trends from stale data gaps.

## ⚙️ Backend & Data Integrity
The backend is designed for rigorous statistical accuracy:

*   **Grid-Synced AI:** AI models (FourCastNet) now dynamically fetch the official **High-Resolution Gas-Weighting Grid** from GitHub at runtime, ensuring their math is 100% identical to the physics-based models (Apples-to-Apples).
*   **Persistent Master Data:** `tdd_master.csv` acts as the source of truth, appending and deduplicating data to preserve historical run history for long-term analytics.
*   **Gas-Weighting Methodology:** Applies high-resolution weighted average temperature calculations based on population-density and natural gas consumption footprints.

## 🤖 Telegram Alert Bot
An automated alert system broadcasts high-priority signals directly to trading channels:

*   **Fast Revision Alerts:** Identifies rapid model shifts (>1.0 DD/day). Includes a **48-Hour Freshness Filter** to automatically quarantine stale data.
*   **Consensus Grouping:** Segregates Primary, AI, and Short-Term signals to provide a structured market view.
*   **Convergence detector:** Fires specifically when multi-model spreads collapse and independent models align on a single weather direction.

## 🚀 Execution & Pipeline
The terminal is designed for automated background operation via `scripts/daily_update.py`:

1.  **Schedule:** Configured for high-frequency updates via GitHub Actions.
2.  **Ingestion:** Parallel fetchers (`fetch_gfs.py`, `fetch_nbm.py`, `fetch_hrrr.py`, etc.) pull data from NOAA/AWS.
3.  **Processing:** 
    *   `compute_tdd.py`: Calculates degree days with stability filters.
    *   `merge_tdd.py`: Aggregates historical and live data payloads.
    *   `build_model_shift_table.py`: Generates UI shift metadata.
4.  **Deployment:** Hosted as a static site via GitHub Pages (accelerated via `.nojekyll` bypass).