# Weather DD Tracker — Code Analysis & Master Architecture Plan
*Generated: 2026-02-21 | Reflects current state of `main` branch after today's session*

---

## Table of Contents
1. [Repository Structure](#1-repository-structure)
2. [Data Flow Diagram](#2-data-flow-diagram)
3. [Script-by-Script Analysis](#3-script-by-script-analysis)
4. [Current Output Files](#4-current-output-files)
5. [⚠️ Flagged Logical Inconsistencies](#5-️-flagged-logical-inconsistencies-not-changed)
6. [Master Architecture Plan](#6-master-architecture-plan)
7. [Phase Completion Status](#7-phase-completion-status)

---

## 1. Repository Structure

```
weather-dd-tracker/
│
├── .github/workflows/
│   └── daily_run.yml              # GitHub Actions: runs daily at 14:00 UTC
│
├── data/
│   ├── ecmwf/
│   │   ├── 20260221_00/           # GRIB dir (created by fetch, not committed)
│   │   ├── 20260221_00_tdd.csv    # Committed TDD output (simple + GW)
│   │   └── ...                    # One _tdd.csv per run
│   ├── gfs/
│   │   ├── 20260221_06/           # GRIB slices (created by fetch, not committed)
│   │   ├── 20260221_06_tdd.csv    # Committed TDD output
│   │   └── ...
│   ├── normals/
│   │   ├── us_daily_normals.csv          # National simple daily normals (365 rows)
│   │   └── us_gas_weighted_normals.csv   # 🆕 Gas-weighted normals (Phase 2)
│   ├── open_meteo/                # Fallback model outputs (not normally populated)
│   ├── weights/
│   │   ├── conus_gas_weights.npy          # 🆕 101×241 weight grid (Phase 2)
│   │   └── conus_gas_weights_meta.json    # 🆕 Grid coordinate metadata
│   └── us_daily_normals.csv       # Root copy (used by old workflow step)
│
├── outputs/
│   ├── tdd_master.csv             # Unified: all models × all runs × all dates
│   ├── ecmwf_latest.csv           # Latest ECMWF run only
│   ├── gfs_latest.csv             # Latest GFS run only
│   ├── vs_normal.csv              # Per-day anomalies (uses simple national normals)
│   ├── run_change.csv             # Total TDD per run + run-to-run delta
│   └── run_delta.csv              # Day-by-day delta: latest vs prev run per model
│
├── scripts/
│   ├── daily_update.py            # Master orchestrator (6 steps)
│   ├── build_gas_weights.py       # 🆕 Phase 2: builds weight grid + GW normals
│   ├── fetch_ecmwf_ifs.py         # Fetches ECMWF IFS HRES (CONUS area, 0.25°)
│   ├── fetch_gfs.py               # Fetches GFS via NOMADS byte-range (t2m only)
│   ├── fetch_open_meteo.py        # Fallback: Open-Meteo API (no GRIB needed)
│   ├── compute_tdd.py             # 🆕 GRIB→CSV: outputs tdd + tdd_gw per day
│   ├── merge_tdd.py               # Globs all *_tdd.csv into tdd_master.csv
│   ├── compare_to_normal.py       # HDD/CDD anomaly vs simple national normals
│   ├── compute_run_delta.py       # Day-by-day delta between two latest runs
│   ├── run_change.py              # Total TDD per run + sequential difference
│   ├── select_latest_run.py       # Extracts latest run per model to *_latest.csv
│   ├── compare_runs.py            # Legacy GFS-only run comparison (outputs to summaries/)
│   ├── plot_gfs_tdd.py            # (Unused in automation) TDD chart generator
│   └── send_telegram.py           # 🆕 Builds and sends full trading-grade report
│
└── CODE_ANALYSIS.md               # This document
```

---

## 2. Data Flow Diagram

```
GitHub Actions (14:00 UTC daily)
│
├─ Step 0: build_gas_weights.py ──────────── [ONCE ONLY if weights missing]
│          data/weights/conus_gas_weights.npy
│          data/normals/us_gas_weighted_normals.csv
│
├─ Step 1: fetch_ecmwf_ifs.py ─────────────── ECMWF OpenData API
│          → data/ecmwf/{run_id}/ifs_t2m.grib2      [CONUS area, 0.25°]
│          → data/ecmwf/{run_id}/manifest.json (not saved)
│          Tries cycles: 18z → 12z → 06z → 00z
│
├─ Step 2: fetch_gfs.py ───────────────────── NOMADS NCEP (byte-range)
│          → data/gfs/{run_id}/gfs.t{cc}z.pgrb2.0p25.f{HHH}  [t2m slice only]
│          → data/gfs/{run_id}/manifest.json
│          Tries cycles: 18z → 12z → 06z → 00z
│          Fallback: fetch_open_meteo.py (if BOTH primary fetches fail)
│
├─ Step 3: compute_tdd.py ─────────────────── GRIB → CSV
│          Reads raw GRIB from each run directory
│          Crops to CONUS: lat 25–50°N, lon 235–295°E (0–360 convention)
│          Loads data/weights/conus_gas_weights.npy
│          Per day: tdd = max(65 - mean_temp_f, 0)     [simple CONUS avg]
│                   tdd_gw = gas-weighted spatial mean   [Phase 2]
│          → data/ecmwf/{run_id}_tdd.csv  [cols: date, mean_temp, tdd, mean_temp_gw, tdd_gw, model, run_id]
│          → data/gfs/{run_id}_tdd.csv
│
├─ Step 4: merge_tdd.py ───────────────────── Glob all *_tdd.csv
│          → outputs/tdd_master.csv  [all models × all runs × all dates]
│
├─ Step 4b: compare_to_normal.py ─────────── Uses SIMPLE national normals
│           → outputs/vs_normal.csv  [per-day HDD/CDD anomaly]
│
├─ Step 5: run_change.py ──────────────────── Sequential run totals
│          → outputs/run_change.csv  [total TDD per run, run-to-run diff]
│
├─ Step 5b: compute_run_delta.py ─────────── Day-by-day delta
│           → outputs/run_delta.csv  [per-date change: latest vs prev run]
│
└─ Step 6: send_telegram.py ───────────────── Trading-grade report
           Uses: tdd_gw if available (Phase 2), else tdd
           Uses: us_gas_weighted_normals.csv if available, else simple normals
           Sends to Telegram: full report with near-term/extended split,
                              model spread, consecutive trend, consensus
```

---

## 3. Script-by-Script Analysis

### `daily_update.py` — Master Orchestrator
- Runs 6 sequential steps via `subprocess`
- Step 0 auto-builds gas weights if `data/weights/conus_gas_weights.npy` is missing
- Clean, linear; no conditional branching except fallback trigger
- **Status: ✅ Correct**

---

### `build_gas_weights.py` — Gas-Weight Grid Builder
- 48 US states with EIA residential+commercial gas consumption (Bcf/yr) × state HDD 30yr normals
- Weight formula: `w(state) = EIA_bcf × HDD_30yr` → amplifies cold high-consumption states (NY, IL, MI, MN, OH, PA), suppresses warm or production states (FL, CA, LA, TX)
- Spreads each state's weight across a 0.25° CONUS grid via 2D Gaussian kernel (σ=2.5° lat, 3.0° lon)
- Top weight cell: lat=41.75°N, lon=-75.25°W (NJ/PA border — New York metro area)
- Normalises: weights sum to 1.0 across CONUS grid
- Gas-weighted normals: scales existing national normals by ratio of GW mean HDD to national mean HDD
- Scale factor: ~1.084 (GW normals are ~8.4% higher than simple national normals in Feb)
- **Status: ✅ Correct. Scientifically sound approximation for Phase 2.**

---

### `fetch_ecmwf_ifs.py` — ECMWF Data Fetcher
- Uses `ecmwf-opendata` SDK (`ecmwf.opendata.Client`)
- Requests: model=ifs, stream=oper, type=fc, resol=0p25
- **CONUS area specified at source:** `area=[50, -125, 25, -65]` (North, West, South, East)
- Tries 4 cycles per day (18z → 12z → 06z → 00z); validates GRIB message count
- Expects 16 forecast steps (0h to 360h in 24h increments)
- **Status: ✅ Correct**

---

### `fetch_gfs.py` — GFS Data Fetcher
- Downloads from NOMADS NCEP via HTTP byte-range extraction
- Only fetches the `TMP:2 m above ground` variable — keeps file size ~5–15 KB per timestep
- Tries 18z → 12z → 06z → 00z with ±1 day lookback
- Stores per-timestep GRIB slices + manifest.json in `data/gfs/{run_id}/`
- No geographic crop at download stage (full global t2m field per slice)
- CONUS crop applied in `compute_tdd.py` post-download
- **Status: ✅ Correct**

---

### `fetch_open_meteo.py` — Open-Meteo Fallback
- Triggered only if BOTH ECMWF and GFS primary fetches fail
- Pulls from Open-Meteo API (free, no key): ECMWF IFS, GFS Seamless, ICON Seamless
- Single point fetch: lat=39.5, lon=-98.4 (CONUS centroid)
- Outputs `date, mean_temp, tdd, model, run_id` to `data/open_meteo/`
- **Status: ⚠️ See Flagged Issues #1**

---

### `compute_tdd.py` — GRIB-to-TDD Converter
- Iterates all run directories in `data/ecmwf/` and `data/gfs/`
- Crops each dataset to CONUS before computing any spatial average
- Loads `data/weights/conus_gas_weights.npy` once; interpolates to data grid per file
- Outputs two TDD metrics per day:
  - `tdd`: simple CONUS equal-area mean
  - `tdd_gw`: gas-consumption-weighted mean (Phase 2)
- Falls back to simple mean if weight file doesn't exist
- **Status: ✅ Correct**

---

### `merge_tdd.py` — Data Merger
- Globs `data/gfs/*_tdd.csv`, `data/ecmwf/*_tdd.csv`, `data/open_meteo/*_tdd.csv`
- Concatenates all into `outputs/tdd_master.csv`
- Auto-assigns model label from filename if column missing
- **Status: ⚠️ See Flagged Issues #2**

---

### `compare_to_normal.py` — Anomaly Calculator
- Reads `tdd_master.csv` and merges with **simple national normals** (`us_daily_normals.csv`)
- Computes HDD anomaly = `tdd` (not `tdd_gw`) vs `hdd_normal`
- Computes CDD anomaly from `mean_temp` vs `cdd_normal`
- Dominant signal: CDD in Jun–Aug, HDD otherwise
- Outputs `vs_normal.csv`
- **Status: ⚠️ See Flagged Issues #3**

---

### `run_change.py` — Run-to-Run Change
- Groups `tdd_master.csv` by model + run, sums total TDD
- Computes sequential difference between runs
- Outputs `run_change.csv`
- **Status: ⚠️ See Flagged Issues #4**

---

### `compute_run_delta.py` — Day-by-Day Delta
- Reads `tdd_master.csv`; finds latest and second-latest run per model
- Inner-joins on date (overlapping window only — correctly aligned)
- Computes per-date `tdd_change = tdd_latest - tdd_prev`
- Uses `tdd` not `tdd_gw`
- **Status: ⚠️ See Flagged Issues #4**

---

### `send_telegram.py` — Telegram Reporter
- Reads `tdd_master.csv`
- **NaN backfill:** if `tdd_gw` column exists but has NaN (old pre-Phase 2 CSVs), backfills with `tdd`
- **Day count** always uses `tdd` (never NaN) — prevents old CSVs from being filtered as "0 days"
- Computes near-term (D1–7) and extended (D8–14) bands separately
- Run change: overlapping-window avg only (corrected vs old version)
- Consecutive trend: counts how many runs in a row have moved the same direction
- Model spread: labels conviction (TIGHT / MODERATE / WIDE)
- Consensus: BULLISH / BEARISH / MIXED / NEUTRAL
- Header tag: `[Gas-Weighted]` or `[CONUS avg]` depending on data available
- **Status: ✅ Correct**

---

### `select_latest_run.py` — Latest Run Extractor
- Reads `tdd_master.csv`; selects max `run_id` per model
- Outputs `outputs/ecmwf_latest.csv`, `outputs/gfs_latest.csv`
- Not called in `daily_update.py` orchestration (standalone utility)
- **Status: ⚠️ See Flagged Issues #5**

---

## 4. Current Output Files

| File | Description | Updated By | Uses GW? |
|---|---|---|---|
| `tdd_master.csv` | All runs, all days, all models | `merge_tdd.py` | Has both `tdd` + `tdd_gw` cols |
| `ecmwf_latest.csv` | Latest ECMWF run | `select_latest_run.py` | No |
| `gfs_latest.csv` | Latest GFS run | `select_latest_run.py` | No |
| `vs_normal.csv` | Per-day HDD/CDD anomaly | `compare_to_normal.py` | No |
| `run_change.csv` | Total TDD + sequential delta | `run_change.py` | No |
| `run_delta.csv` | Day-by-day delta: latest vs prev | `compute_run_delta.py` | No |

---

## 5. ⚠️ Flagged Logical Inconsistencies (Not Changed)

These are identified issues that exist in the current codebase. They are documented here for priority resolution but have **not been modified** to avoid scope creep.

---

### 🚩 Issue #1 — Open-Meteo Fallback Uses a Single Point, Not Spatial Average

**File:** `fetch_open_meteo.py` (lines 28–30)

```python
LATITUDE = 39.5   # ~CONUS center
LONGITUDE = -98.4
```

**Problem:** The fallback uses a **single geographic point** (geographic center of CONUS) to represent the national temperature. This is fundamentally different from the spatial average that ECMWF and GFS use (101×241 grid points). A single point in Kansas has no relation to gas-weighted demand centers in the Northeast/Midwest.

**Impact:** If the fallback ever triggers, the HDD value will be meaningless for Henry Hub trading. The fallback currently produces a model labeled `OM_ECMWF`, `OM_GFS`, or `OM_ICON` which would flow into `tdd_master.csv` and appear in the Telegram report as if it were valid.

**Recommended Fix (Phase 2b):** Use multiple representative city coordinates (e.g., Chicago, NYC, Detroit, Pittsburgh, Minneapolis, Atlanta) with population weights and average them. Or use Open-Meteo's grid endpoint.

**Severity: 🔴 HIGH — incorrect signal if triggered**

---

### 🚩 Issue #2 — `merge_tdd.py` Has No Deduplication Logic

**File:** `merge_tdd.py`

```python
# No dedup: if a run appears in both old committed CSV and newly computed CSV,
# it gets duplicated rows in tdd_master.csv
dfs.append(df)
return pd.concat(dfs, ignore_index=True)
```

**Problem:** If `compute_tdd.py` reprocesses a run that already has a committed `*_tdd.csv` file (e.g., because someone re-triggers the pipeline), that run's rows appear **twice** in `tdd_master.csv`. The `groupby.mean()` in `send_telegram.py` would average the duplicated rows — producing the same result numerically — but the day count would be 32 instead of 16, which could cause issues downstream.

**Recommended Fix:** Add `.drop_duplicates(subset=["model", "run_id", "date"])` after the concat.

**Severity: 🟡 MEDIUM — currently masked by groupby mean, but fragile**

---

### 🚩 Issue #3 — `compare_to_normal.py` Is Not Upgraded to Phase 2

**File:** `compare_to_normal.py`

```python
NORMALS_FILE = Path("data/normals/us_daily_normals.csv")   # simple normals only
# ...
merged["hdd_anomaly"] = merged["tdd"] - merged["hdd_normal"]   # simple tdd, not tdd_gw
```

**Problem:** `compare_to_normal.py` still uses:
- Simple national normals (not gas-weighted)
- `tdd` column (not `tdd_gw`)

This means `outputs/vs_normal.csv` is a Phase 1 artifact even after Phase 2 is active. The `send_telegram.py` already handles its own correct GW comparison independently, so this doesn't break the Telegram report — but the `vs_normal.csv` file is misleading if used for any external analysis.

**Recommended Fix:** Add GW comparison columns to `compare_to_normal.py` output: `hdd_anomaly_gw`, `tdd_gw` vs `hdd_normal_gw`.

**Severity: 🟡 MEDIUM — vs_normal.csv is inconsistent with Phase 2 metrics**

---

### 🚩 Issue #4 — `run_change.py` and `compute_run_delta.py` Use `tdd` Not `tdd_gw`

**Files:** `run_change.py`, `compute_run_delta.py`

```python
# run_change.py
run_totals = df.groupby(["model", "run_id"])["tdd"].sum()

# compute_run_delta.py
df_latest = m[m["run_id"] == latest_id][["date", "tdd"]]
```

**Problem:** Both files compute run-to-run changes using simple `tdd`, not gas-weighted `tdd_gw`. The `outputs/run_change.csv` and `outputs/run_delta.csv` files are Phase 1 artifacts. The Telegram reporter does its own correct overlapping-window run change calculation, so these files are not used by the signal — but they're misleading if read directly.

**Recommended Fix:** Add `tdd_gw` columns to both output files when available.

**Severity: 🟢 LOW — doesn't affect Telegram signal (send_telegram.py computes its own)**

---

### 🚩 Issue #5 — `select_latest_run.py` Is Orphaned from Pipeline

**File:** `select_latest_run.py`

**Problem:** This script is NOT called in `daily_update.py`. It produces `outputs/ecmwf_latest.csv` and `outputs/gfs_latest.csv`, but these are not used by any other script in the automated pipeline. They may be stale if not manually run.

**Recommended Fix:** Either add it as a step in `daily_update.py` after `merge_tdd.py`, or remove it if unused.

**Severity: 🟢 LOW — cosmetic / maintainability**

---

### 🚩 Issue #6 — Gas-Weighted Normals Scale Factor Is Annual, Not Seasonal

**File:** `build_gas_weights.py`

```python
gw_mean_daily = sum(eia * hdd for _, _, _, eia, hdd in STATE_DATA) / \
                sum(eia for _, _, _, eia, _ in STATE_DATA) / 365.0

scale = gw_mean_daily / nat_mean_daily
```

**Problem:** The scale factor is derived from **annual** HDD averages, then applied uniformly to all days of the year. In reality, the gas-weight correction should be larger in deep winter (when the Northeast dominates demand) and smaller in shoulder months (when demand is more evenly distributed). Using a single annual scale factor slightly under-represents the GW correction in January/February and over-represents it in March/April.

**Impact:** The GW normal for deep winter might be very slightly under-corrected. For current February data, GW normal of 25.5 vs national 22.6 is likely directionally correct but not perfectly calibrated.

**Recommended Fix (Phase 2b):** Compute seasonal scale factors (e.g., by month group) using gas consumption by season from EIA data.

**Severity: 🟢 LOW — directionally correct, minor calibration gap**

---

## 6. Master Architecture Plan

### Vision
A fully automated, trading-grade US natural gas weather analytics pipeline delivering Henry Hub demand signals via Telegram, expanding to LNG export weather and multi-commodity signals.

---

### ✅ Phase 1 — CONUS HDD Pipeline (COMPLETE)

| Feature | Status |
|---|---|
| ECMWF IFS HRES fetch (daily, CONUS area) | ✅ Done |
| GFS fetch via NOMADS byte-range (t2m only) | ✅ Done |
| Open-Meteo fallback (both primary fail) | ✅ Done (⚠️ single-point issue) |
| TDD computation from GRIB | ✅ Done |
| CONUS geographic crop (25–50°N, 235-295°E) | ✅ Done |
| Run-to-run change (same-window avg) | ✅ Done |
| Telegram report with HDD/day + vs Normal | ✅ Done |
| GitHub Actions automation (daily 14:00 UTC) | ✅ Done |

---

### ✅ Phase 2 — Gas-Weighted HDDs (COMPLETE)

| Feature | Status |
|---|---|
| CONUS gas-weight grid (48 states, EIA × HDD30yr) | ✅ Done |
| Gaussian kernel spatial spread at 0.25° | ✅ Done |
| `tdd_gw` column in all new TDD outputs | ✅ Done |
| Gas-weighted normals (`us_gas_weighted_normals.csv`) | ✅ Done |
| Backward-compatible NaN backfill (old CSVs) | ✅ Done |
| Telegram: near-term (D1–7) vs extended (D8–14) split | ✅ Done |
| Telegram: model spread with conviction label | ✅ Done |
| Telegram: consecutive run trend counter | ✅ Done |
| Telegram: consensus signal (BULLISH/BEARISH/MIXED) | ✅ Done |

---

### 🔲 Phase 2b — Data Quality Hardening (NEXT)

Priority fixes for flagged inconsistencies above.

| Task | Addresses | Priority |
|---|---|---|
| Fix Open-Meteo fallback: multi-city weighted avg instead of single point | Issue #1 | 🔴 HIGH |
| Add deduplication in `merge_tdd.py` | Issue #2 | 🟡 MEDIUM |
| Upgrade `compare_to_normal.py` to Phase 2 GW columns | Issue #3 | 🟡 MEDIUM |
| Upgrade `run_change.py` + `compute_run_delta.py` to include `tdd_gw` | Issue #4 | 🟢 LOW |
| Add `select_latest_run.py` to `daily_update.py` pipeline | Issue #5 | 🟢 LOW |
| Seasonal scale factors for GW normals | Issue #6 | 🟢 LOW |

---

### 🔲 Phase 3 — Proper Gas-Weighted Grid (True GW HDDs)

Replace the Gaussian-kernel state-centroid approach with true grid-level gas consumption weighting.

| Task | Detail |
|---|---|
| Build EIA gas consumption weight raster | Use EIA county-level gas use data + census shapefiles to create a true 0.25° consumption-density grid |
| Replace Gaussian kernel | Use exact county→grid assignment instead of smooth Gaussian spread |
| Seasonal weight variants | Different winter/summer weighting (heating vs power gen) |
| Validate against CWG/DTN benchmark | Back-test our GW HDDs vs published gas-weighted HDD series |

**Estimated improvement:** This brings the pipeline to near-professional CWG/DTN/HFI quality for GW HDDs.

---

### 🔲 Phase 4 — Advanced Signal Layer

| Task | Detail |
|---|---|
| Day-by-day chart output | Matplotlib PNG attached to Telegram message (like HFI Research) |
| 7-day vs 14-day confidence scoring | Flag when extended forecast is less reliable |
| Historical percentile ranking | "Current HDD anomaly is in top 15% for this date" |
| Ensemble spread (ECMWF EPS) | Add ECMWF ensemble runs alongside HRES for uncertainty quantification |
| Storage withdrawal estimate | Convert HDD anomaly to estimated weekly EIA storage withdrawal (Bcf) |

---

### 🔲 Phase 5 — LNG Export Integration (Global Context)

> *"For Henry Hub, LNG export pull is the demand factor that global weather drives."*

| Task | Detail |
|---|---|
| European TTF weather signal | Northwest Europe heating demand → reduces US LNG export volumes → bearish for HH |
| Asian JKM weather signal | Northeast Asia (Japan, Korea, China) heating demand → pulls LNG exports → bullish for HH |
| LNG terminal utilization tracking | Monitor Sabine Pass, Freeport, Corpus Christi, Calcasieu Pass daily cargo data |
| Combined US + Export demand model | HH price driver = US domestic demand + LNG export demand |

---

### 🔲 Phase 6 — Web Dashboard

| Task | Detail |
|---|---|
| Real-time HDD/GW HDD chart (multi-run overlay) | Like HFI's "ECMWF EPS Model Changes 24 Hours" chart |
| Historical model error tracking | Was ECMWF right vs GFS historically? |
| Signal history log | All Telegram alerts with outcome tracking |
| Portfolio integration | Link HDD signals to KOLD/BOIL/UNG positions |

---

## 7. Phase Completion Status

```
Phase 1  [██████████] 100% — CONUS HDD Pipeline
Phase 2  [██████████] 100% — Gas-Weighted HDDs
Phase 2b [░░░░░░░░░░]   0% — Data Quality Hardening (6 issues flagged above)
Phase 3  [░░░░░░░░░░]   0% — True GW Grid (EIA county-level)
Phase 4  [░░░░░░░░░░]   0% — Advanced Signal Layer
Phase 5  [░░░░░░░░░░]   0% — LNG Export Integration
Phase 6  [░░░░░░░░░░]   0% — Web Dashboard
```

---

*Last updated: 2026-02-21 by automated session. All bugs and inconsistencies are flagged with severity ratings. No changes were made to flag-only items.*
