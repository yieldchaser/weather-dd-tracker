# Master Blueprint: System Architecture and Logic

## 1. Data Sources & Endpoints
The Weather Desk terminal relies on the following external APIs for tracking weather demand, grid burn, and ensemble shifts:

*   **Open-Meteo ER5 Archive (Historical Normals)**: `https://archive-api.open-meteo.com/v1/archive`
    *   Parameters: `latitude`, `longitude`, `start_date`, `end_date`, `daily=temperature_2m_mean`, `timezone=UTC`.
*   **Open-Meteo Ensemble API (ECMWF ENS & CMC ENS)**: `https://ensemble-api.open-meteo.com/v1/ensemble`
    *   Parameters: `models=ecmwf_ifs025`, `models=gem_global_ensemble`, `daily=temperature_2m_mean`.
*   **Open-Meteo Deterministic API (HRRR, NAM, ICON)**: `https://api.open-meteo.com/v1/forecast`
*   **EIA API v2 (Historical PWHDDs and Live Grid)**
    *   Live Grid: `https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/` (frequency=hourly)
    *   Historical Normals: `https://api.eia.gov/v2/total-energy/data/`
*   **NOAA NOMADS (GFS & NBM) & AWS S3 (GEFS)**
    *   Byte-range parsing from GRIB2 index files (`.idx`).

## 2. Mathematical & Calculation Logic

### Weather Demand
*   **Gas-Weighted Average (`tdd_gw`)**: Calculates the daily temperature average applying geographic demographic scaling. Formula: $\sum (T_{city} \times W_{city}) / \sum W_n$
*   **17-City Weights**:
    1. New York (6.0), 2. Chicago (5.0), 3. Boston (4.0), 4. Philadelphia (3.0), 5. Detroit (3.0), 6. Minneapolis (2.5), 7. Pittsburgh (2.0), 8. Cleveland (2.0), 9. Milwaukee (1.5), 10. Columbus (1.5), 11. Indianapolis (1.5), 12. Baltimore (1.5), 13. Charlotte (1.0), 14. Atlanta (1.0), 15. Dallas (1.0), 16. Kansas City (0.8), 17. St Louis (0.8). Total Weight = 38.1.
*   **TDD Formula**: $HDD = \max(65.0 - T, 0)$, $CDD = \max(T - 65.0, 0)$.

### Grid/Power Burn
*   **30-Day Wind Baseline Math**: Hourly fuel mix queried from EIA (ERCO, PJM, MISO, SWPP). Excludes current day (`today`), pivots hourly to daily. `hist_wind` is calculated as the mean daily wind generation over the preceding 30 days.
*   **Wind Anomaly**: $\text{Anomaly} = \text{Live Wind}_{today} - \text{Wind}^{30day}_{avg}$
*   **Gas Burn Impact**:
    *   **ISO Level**: $< -1000 \text{ MW}$ -> `BULLISH` (Wind Drought, higher gas burn). $> +1500 \text{ MW}$ -> `BEARISH` (Strong wind, displaces gas).
    *   **National Aggregate**: $< -3000 \text{ MW}$ -> `BULLISH`. $> +4000 \text{ MW}$ -> `BEARISH`.

### Ensemble Math & Run-to-Run Deltas
*   **Formula**: $\text{Shift} = HDD\_Value^{(latest)} - HDD\_Value^{(previous)}$
*   **T-1 Seeding Logic**: Models arriving late (like CMC ENS) have a seeded prior-run simulated via index copy to prime the delta subtractor if historical runs are unavailable.

## 3. Code Architecture & Data Flow

*   **Execution Flow (`daily_update.py`)**:
    1. Gas grid setup (`build_true_gw_grid.py`).
    2. Parallel Fetchers (13 threads backing GFS, ECMWF, HRRR, etc.).
    3. Aggregation (`compute_tdd.py`).
    4. Merging & Normals comparison (`merge_tdd.py`).
    5. Delta Matrix & Shifts (`build_model_shift_table.py`).
    6. Market analysis (`fetch_live_grid.py`).
    7. Telegram alerts.

*   **NaN Handling & Asynchronous Reporting**:
    *   **EIA v2**: Uses strict `pd.to_numeric(errors='coerce')` coupled with `dropna()` to force asynchronous or blank strings to true NaN. 
    *   **National Grid Fallback**: If *any* individual ISO fails to report or returns NaN, the entire `NATIONAL` aggregation row is strictly forced to `NaN` to prevent deceptive partial sums (data poisoning).

## 4. Stability Patches & Vulnerabilities

*   **NOAA API Stability (urllib3 Exponential Backoff)**:
    *   **Vulnerability**: NOAA NOMADS & AWS fetches are notoriously prone to dropping connections mid-byte range index sweeps.
    *   **Patch**: Replaced nominal `requests.get()` with `requests.Session()` integrated with `urllib3.util.Retry`.
    *   **Configuration**: Enforced `total=5` retries with an exponential `backoff_factor=2` (resulting in 2s, 4s, 8s, 16s sleep delays) listening on `status_forcelist=[429, 500, 502, 503, 504]`. Explicit `timeout=15` headers force-terminate hanging sockets.

*   **Strict Interpolation Limits**:
    *   **Vulnerability**: Legacy pandas smoothing `merged.interpolate(method="time")` allowed unbounded linear patching. If a major storm front shifted 200 miles during a 4-day API dropout, the terminal would chart a fictitious smooth trendline.
    *   **Patch**: Enforced a hardcap `limit=3` inside the arrays under `build_model_shift_table.py`. Dropped runs extending beyond 3 consecutive timesteps now intentionally surface as actual unadulterated `NaN` breaks in the terminal.
