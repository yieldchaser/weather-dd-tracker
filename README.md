# Weather Desk: Institutional-Grade Weather Terminal

An algorithmic weather dashboard designed for institutional natural gas trading. This terminal tracks gas-weighted Degree Days (HDD/CDD), cross-model consensus, and market bias signals across physics-based ensembles and machine-learning (AI) forecast models.

## 🌦️ Supported Models & Horizons
The terminal synchronizes data from over a dozen global and regional models, providing a comprehensive view of meteorological evolution:

*   **Short-Range (0-3 Days):** 
    *   **HRRR:** 48h High-Resolution Rapid Refresh
    *   **NAM:** 84h North American Mesoscale
*   **Medium-Range Physics (0-16 Days):**
    *   **ECMWF / GFS:** Operational global standards
    *   **CMC_ENS / GEFS:** Physics-based ensembles
    *   **NBM:** National Blend of Models
*   **Medium-Range AI (ML Forecasts):**
    *   **ECMWF_AIFS:** ECMWF's Artificial Intelligence Integrated Forecasting System
    *   **FourCastNetV2-Small:** NVIDIA-backed machine learning model
*   **Extended (Seasonal):**
    *   **GEFS_35D:** 35-day subseasonal ensemble

## 📊 Core Features & UI
The terminal provides refined visualizations and tables optimized for rapid decision-making:

*   **Model Consensus Shift Table:** Normalizes run-to-run changes to **Average HDDs/Day**. Unlike raw totals, this allows traders to compare a 2-day HRRR shift against a 16-day GFS shift on an apples-to-apples basis.
*   **Gap Flags (*):** The UI automatically detects time-jumps between model fetches. If a run is more than 24 hours behind, it is flagged with a `*Gap` marker to distinguish "weather evolution" from single-cycle "model trends."
*   **Dynamic Horizon Tags:** Models display their effective forecast window (e.g., `HRRR (2d)`, `NAM (3.5d)`) to alert users to the varying limits of predictive data.
*   **X-Axis Clipping:** The main chart dynamically clips historical "rubber-banding," focusing the viewport strictly on forward-looking weather starting from Today.

## ⚙️ Backend & Data Integrity
The backend is designed for high-frequency updates and rigorous statistical accuracy:

*   **Master Data Store:** All processed data is consolidated into `tdd_master.csv`, the source of truth for all frontend and alert components.
*   **The 'Pollution Check':** To prevent artificial spikes (e.g., +100 HDD shifts) caused by missing gas-weights, the pipeline detects if a model run lacks native gas-weighting data and automatically falls back to a **Simple vs. Simple** comparison.
*   **Gas-Weighting methodology:** Applies weighted average temperature calculations based on regional natural gas consumption footprints.

## 🤖 Telegram Alert Bot
An automated alert system broadcasts high-priority shifts directly to trading channels:

*   **Fast Revision Alerts:** Identifies rapid model shifts. Includes a **48-Hour Freshness Filter** to automatically quarantine stale data, ensuring alerts only reflect live, actionable cycles.
*   **Algorithmic Bias & Convergence:** Tracks model alignment (Bullish/Bearish) and daily convergence trends to identify high-conviction weather regimes.
*   **AI vs Physics Consensus:** The bot separates AI models from traditional physics ensembles to generate a clean, ML-driven market signal.

## 🚀 Setup & Execution
The terminal is designed for automated background operation:

1.  **Schedule:** Configured for 4x daily updates (04:00, 10:00, 16:00, 22:00 UTC) via GitHub Actions.
2.  **Data Ingestion:**
    *   `scripts/fetch_nomads.py`: Pulls latest HRRR/NAM GRIB data.
    *   `scripts/fetch_open_meteo.py`: Ingests ensemble and AI payloads.
3.  **Processing Pipeline:**
    *   `scripts/compute_tdd.py`: Calculates degree days and applies gas-weights.
    *   `scripts/build_model_shift_table.py`: Generates the UI shift metadata and JSON payloads.
4.  **Deployment:** Run using a local development server or hosted as a static site via GitHub Pages.