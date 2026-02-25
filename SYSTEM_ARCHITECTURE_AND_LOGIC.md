# Master Blueprint: System Architecture and Logic

## 1. Data Sources & Endpoints
The Weather Desk terminal relies on the following external APIs for tracking weather demand, grid burn, and ensemble shifts:

*   **Open-Meteo ER5 Archive (Historical Normals)**
    *   **Endpoint:** `https://archive-api.open-meteo.com/v1/archive`
    *   **Parameters:** `latitude`, `longitude`, `start_date`, `end_date`, `daily=temperature_2m_mean`, `timezone=UTC`.
    *   **Limits/Constraints:** Open API. Used to construct the 10-year and 30-year daily TDD normals.
*   **Open-Meteo Ensemble API (ECMWF ENS & CMC ENS)**
    *   **Endpoint:** `https://ensemble-api.open-meteo.com/v1/ensemble`
    *   **Parameters:** `models=ecmwf_ifs025` (for European Ensemble), `models=gem_global_ensemble` (for CMC Ensemble), `daily=temperature_2m_mean`.
    *   **Limits/Constraints:** 500 simultaneous connections. 
*   **Open-Meteo Deterministic API (HRRR, NAM, ICON)**
    *   **Endpoint:** `https://api.open-meteo.com/v1/forecast`
    *   **Parameters:** `models` parameter mapped per model.
*   **EIA API v2 (Historical PWHDDs and Live Grid)**
    *   **Endpoints:**
        *   Live Grid: `https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/`
        *   Historical Normals: `https://api.eia.gov/v2/total-energy/data/`
    *   **Parameters:** `api_key`. Live Grid uses `frequency=hourly`, facets on respondents `[ERCO, PJM, MISO, SWPP]`. Historical uses `facets[msn][]=[ZWHDPUS, ZWCDPUS]`.
    *   **Limits/Constraints:** Requires active `EIA_KEY` in environment variables. Pagination limited to 5000 records.
*   **NOAA NOMADS (GFS & NBM)**
    *   **Endpoints:** HTTPS byte-range parsing from NOAA GRIB2 index files.
    *   **Constraints:** Rate limits exist, scripts utilize partial HTTP Range requests mapped from `.idx` files to pull only `t2m` data, avoiding full file downloads.

## 2. Mathematical & Calculation Logic

### Weather Demand
*   **Gas-Weighted Average (`tdd_gw`)**
    *   **Concept:** Calculates the daily temperature average applying geographic demographic scaling.
    *   **Formula:** $\sum (T_{city} \times W_{city}) / \sum W_n$
    *   **Cities and Weights:**
        1. New York (6.0), 2. Chicago (5.0), 3. Boston (4.0), 4. Philadelphia (3.0), 5. Detroit (3.0), 6. Minneapolis (2.5), 7. Pittsburgh (2.0), 8. Cleveland (2.0), 9. Milwaukee (1.5), 10. Columbus (1.5), 11. Indianapolis (1.5), 12. Baltimore (1.5), 13. Charlotte (1.0), 14. Atlanta (1.0), 15. Dallas (1.0), 16. Kansas City (0.8), 17. St Louis (0.8). Total Weight = 38.1.
    *   **TDD Formula:** HDD uses base 65°F: `max(65.0 - T, 0)`. CDD uses `max(T - 65.0, 0)`.
*   **Historical Normals Calculation**
    *   **10-Year Epoch:** 2016-2025 ERA5 Reanalysis data aggregated by month/day.
    *   **30-Year Epoch:** 1991-2020 ERA5 Reanalysis data aggregated by month/day.

### Grid/Power Burn
*   **30-Day Wind Baseline Calculation:** Hourly fuel mix queried from EIA (ERCO, PJM, MISO, SWPP). Excludes current day (`today`), pivots hourly to daily. `hist_wind` is calculated as the mean daily wind generation over the preceding 30 full days.
*   **Wind Anomaly:** $\text{Anomaly} = \text{Live Wind}_{today} - \text{Wind}^{30day}_{avg}$
*   **Gas Burn Impact (Bullish/Bearish):**
    *   **ISO Level:** $\text{Anomaly} < -1000 \text{ MW}$ -> `BULLISH` (Wind Drought, higher gas burn). $\text{Anomaly} > +1500 \text{ MW}$ -> `BEARISH` (Strong wind, displaces gas).
    *   **National Aggregate:** Sums all 4 ISO anomalies. $\text{Anomaly} < -3000 \text{ MW}$ -> `BULLISH`. $\text{Anomaly} > +4000 \text{ MW}$ -> `BEARISH`.

### Ensemble Math & Run-to-Run Deltas
*   **Calculation:** For each respective model, the two most recent runs are extracted (e.g. 12z vs 00z).
*   **Formula:** $\text{Shift} = HDD\_Value^{(latest)} - HDD\_Value^{(previous)}$
*   **Interpolation Handling:** Missing HTTP timesteps are padded utilizing `Pandas` time-based interpolation (`interpolate(method="time", limit=2)`).
*   **T-1 Seeding Logic (e.g. CMC ENS):** Models arriving late or dropping sync must have a seeded prior-run simulated via index copy to prime the delta subtractor.

## 3. Code Architecture & Data Flow

### Execution Flow (`daily_update.py`)
1. **Grid Pre-Check:** Executes `build_true_gw_grid.py` (once) to cache the `.npy` bounding box array.
2. **Parallel Extractor:** Dispatches 13 concurrent fetch scripts across threads (e.g., `fetch_gfs.py`, `fetch_ecmwf_ens.py`), falling back to `fetch_open_meteo.py` if GFS and ECMWF IFS both fail.
3. **Array Processing:** `compute_tdd.py` translates GRIBs to HDD/CDD Arrays.
4. **Data Merging:** `merge_tdd.py` outputs `tdd_master.csv`, comparing to normals via `compare_to_normal.py`.
5. **Delta Engine:** Extracts run subsets (`select_latest_run.py`), logs shifts (`build_model_shift_table.py`), handles convergence algorithms.
6. **Market & Grid Analysis:** Executes `physics_vs_ai_disagreement.py`, `fetch_live_grid.py`, and `composite_score.py`.
7. **Publishing:** Telegram dispatch.

### NaN Handling & Data Poisoning Prevention
*   **File Level (`fetch_historical_eia_normals.py`):** `pd.to_numeric(errors='coerce')` explicitly forces non-numerical/asynchronous empty EIA strings to `NaN`, followed by a strict `dropna()`. Prevents cascading 0s.
*   **National Grid Fallback:** In `fetch_live_grid.py`, if any ISO lacks data or is in localized outage, the `NATIONAL` aggregation row is forced to `NaN` entirely, preventing a deceptive partial sum.
*   **Shift Engine Alignment:** Run-to-run tables execute an outer join and enforce `min_date` termination bounding (Intersection), pruning dangling OP/ENS forecast array ranges.

### Data Flow to Frontend JavaScript
*   The Python backend dumps standard `.csv` files and `.json` payload wrappers into the `outputs/` directory.
*   `index.html` orchestrates asynchronous DOM painting using `Papa.parse()`, executing a `Promise.all` fetch matrix resolving the files (e.g., `run_delta.csv`, `model_shift_table.csv`, `historical_degree_days.csv`).
*   Javascript injects variables directly into standard DOM tables and Chart.js instances rendering cleanly to the end user.

## 4. Update Frequency & Orchestration
*   **Orchestrator:** GitHub Actions
*   **Schedule file:** `.github/workflows/daily_run.yml`
*   **Execution constraints:** Depending on the explicit `cron:` parameter.
*   **Failover Logic:** Pipeline skips missing data safely (tolerances defined via threshold interpolation and script-level exception exits code `0`). 

## 5. Vulnerability Assessment (Self-Audit)
*   **Open-Meteo Ensemble Restrictions:** ECMWF ensemble member extraction is heavily gatekept. The script had to default to Open-Meteo's unified statistical mean array, precluding custom 51-member re-weighting unless an enterprise ECMWF hook is acquired.
*   **EIA v2 Asynchrony:** EIA grid queries sporadically drop. ISOs rotate reporting speeds leading to temporary NaN holes.
*   **GRIB2 Connectivity:** NOAA FTP byte-requests timeout randomly mid-range payload (`[ERR] No IDX / Variable not found`). The array must absorb skipped hours implicitly or rely on linear interpolation.
*   **Dependency on System Timezones:** Timezone shifts during Cron spin-up vs native UTC constraints might desync the 00Z vs 12Z shift selector if executions drift past 7pm/7am pivot boundary lines.
