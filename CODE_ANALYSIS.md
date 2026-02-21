# 🌡️ Weather Degree Day Tracker — Full Code Analysis

> A fully automated pipeline for fetching weather model data, computing Heating/Cooling Degree Days (HDD/CDD), comparing them to historical normals, and sending daily Telegram alerts — all powered by GitHub Actions.

**Last updated**: 2026-02-21 — V2.0 audit pass (GFS byte-range, Open-Meteo fallback, CDD fix, step validation, CSV persistence)

---

## 📦 What is this project?

This project is a **weather data pipeline** built for **energy traders and analysts**. It:

1. Fetches raw temperature forecasts from two major weather models: **GFS** (American) and **ECMWF** (European).
2. Converts those temperatures into **Heating Degree Days (HDD)** and **Cooling Degree Days (CDD)** — measures used in natural gas and power markets.
3. Compares the forecast HDDs/CDDs against **30-year historical climate normals**.
4. Tracks how the models are changing **run-over-run** (bullish/bearish signals).
5. Sends a **Telegram message** every day with a clean weather desk summary.
6. Falls back to **Open-Meteo API** automatically if primary model sources are unavailable.

---

## 🔑 Key Concepts

### Heating Degree Day (HDD)
> **HDD = max(65°F − Average Temperature, 0)**

- If the average temperature is **50°F** → HDD = `65 − 50 = 15`
- If the average temperature is **70°F** → HDD = `0` (no heating needed)
- More HDD = colder = more natural gas demand = **BULLISH** for gas prices

### Cooling Degree Day (CDD)
> **CDD = max(Average Temperature − 65°F, 0)**

- Used in summer to measure air conditioning / power burn demand
- More CDD = hotter = more electricity cooling demand = **BULLISH** for power

The pipeline tracks **both** — HDD in winter months, CDD in summer months.

---

## 🗂️ Repository Structure

```
weather-dd-tracker/
│
├── scripts/                      ← All Python logic lives here
│   ├── daily_update.py           ← 🎯 Master orchestrator (runs everything)
│   ├── fetch_gfs.py              ← Downloads GFS t2m via byte-range GRIB filter
│   ├── fetch_ecmwf_ifs.py        ← Downloads ECMWF t2m with step validation
│   ├── fetch_open_meteo.py       ← 🆕 Open-Meteo API fallback (no GRIB needed)
│   ├── compute_tdd.py            ← Converts raw temp data → HDD/CDD values
│   ├── merge_tdd.py              ← Merges all HDD CSVs into one master file
│   ├── compare_to_normal.py      ← Compares forecast HDD+CDD vs historical normals
│   ├── run_change.py             ← Run-to-run HDD change → outputs/run_change.csv
│   ├── compute_run_delta.py      ← Per-day TDD delta between two runs
│   ├── select_latest_run.py      ← Filters out the latest run per model
│   ├── compare_runs.py           ← Detailed delta summary between two GFS runs
│   ├── plot_gfs_tdd.py           ← Generates a chart (GFS TDD vs Normal)
│   └── send_telegram.py          ← Sends the daily report to Telegram
│
├── data/
│   ├── gfs/                      ← Byte-range extracted t2m GRIB2 slices per run
│   ├── ecmwf/                    ← Raw ECMWF GRIB2 forecast files per run
│   ├── open_meteo/               ← 🆕 Open-Meteo JSON-derived TDD CSVs (fallback)
│   └── normals/
│       └── us_daily_normals.csv  ← 30-year US average HDD/CDD per calendar day
│
├── outputs/
│   ├── tdd_master.csv            ← All HDD data from all models and runs merged
│   ├── vs_normal.csv             ← HDD+CDD vs historical normal per date
│   ├── run_change.csv            ← 🆕 Run-to-run HDD delta per model (saved to CSV)
│   ├── run_delta.csv             ← 🆕 Per-day TDD change between two latest runs
│   ├── ecmwf_latest.csv          ← 🆕 Latest ECMWF run only (quick Excel ingestion)
│   └── gfs_latest.csv            ← 🆕 Latest GFS run only (quick Excel ingestion)
│
└── .github/workflows/
    └── daily_run.yml             ← GitHub Actions: runs everything at 2PM UTC daily
```

---

## ⚙️ Full Pipeline — How Everything Connects

```
daily_update.py
    │
    ├── 1. fetch_ecmwf_ifs.py      → Downloads ECMWF t2m, validates 16 steps
    ├── 2. fetch_gfs.py            → Downloads GFS t2m via .idx byte-range filter
    │        │
    │        └── (if BOTH fail) fetch_open_meteo.py → Free API fallback
    │
    ├── 3. compute_tdd.py          → All ECMWF runs: GRIB2 → HDD CSVs
    ├── 4. compute_tdd.py          → All GFS runs: GRIB2 → HDD CSVs
    ├── 5. merge_tdd.py            → All CSVs (incl. Open-Meteo) → tdd_master.csv
    ├── 5b. compare_to_normal.py   → HDD+CDD anomaly → vs_normal.csv + signals
    ├── 6. run_change.py           → Run-to-run delta → run_change.csv
    ├── 6b. compute_run_delta.py   → Per-day delta → run_delta.csv
    └── 7. send_telegram.py        → Sends final summary to Telegram
```

---

## 📜 Script-by-Script Breakdown

---

### 1. `daily_update.py` — The Orchestrator

**Role**: Master controller that runs the entire pipeline end-to-end.

**Key changes (V2.0)**:
- Uses `sys.executable` instead of hardcoded `python` — ensures correct interpreter on all platforms
- Captures return codes from ECMWF and GFS fetches; if **both** fail, automatically triggers the Open-Meteo fallback
- Calls `compute_run_delta.py` as step 6b after run_change.py
- All subprocess calls use `f"{PY} scripts/..."` pattern

---

### 2. `fetch_gfs.py` — GFS Data Downloader ⚡ Major V2.0 Fix

**Role**: Downloads raw temperature forecast data from NOAA's GFS model.

**Data source**: `https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod`

**V2.0 architecture — byte-range GRIB extraction**:

> Previously: downloaded full GRIB2 files (~500MB–1.5GB each × 16 timesteps = potential 16GB+ disk usage, crashing GitHub Actions' 14GB limit)
>
> Now: reads the `.idx` index file for each timestep, locates the exact byte offset for `TMP:2 m above ground`, and issues an HTTP Range request to pull **only those bytes** (~5–15KB per timestep)

**How it works**:
| Step | Action |
|------|--------|
| 1 | Finds latest available GFS run (18Z→12Z→06Z→00Z, today then yesterday) |
| 2 | For each forecast hour (f000, f024, ..., f360): fetches the `.idx` index |
| 3 | Parses `.idx` to locate `TMP:2 m above ground` byte range |
| 4 | Issues `Range: bytes=START-END` HTTP request → downloads only t2m field |
| 5 | Writes `manifest.json` with fetched/skipped hours |

**Output**: `data/gfs/{run_date}_{cycle}/gfs.t{cycle}z.pgrb2.0p25.f{HHH}` (byte-range slice, ~5-15KB)

**`url_exists()` improvement**: now wrapped in try/except — handles NOMADS returning 200 on HEAD but failing on GET.

---

### 3. `fetch_ecmwf_ifs.py` — ECMWF Data Downloader ⚡ V2.0 Fix

**Role**: Downloads ECMWF IFS HRES (High Resolution) forecast data.

**Data source**: ECMWF Open Data API via the `ecmwf-opendata` Python package

**V2.0 change — step count validation**:

> Previously: accepted any successful download as complete, even if only partial steps were returned
>
> Now: after each download, counts GRIB messages using `eccodes.codes_grib_new_from_file()`. If the count is less than 16 (the expected number of daily steps), the partial file is **deleted** and the next cycle is tried.

**Fallback cycle order**: 18Z → 12Z → 06Z → 00Z (newest → oldest)

**Output**: `data/ecmwf/{date}_{cycle}/ifs_t2m.grib2`

---

### 4. `fetch_open_meteo.py` — Open-Meteo Fallback 🆕 New Script

**Role**: Free API fallback when both ECMWF and GFS primary fetches fail.

**Why it exists**: The Open-Meteo API returns ECMWF, GFS, and ICON t2m directly as JSON — no GRIB parsing, no API key, zero cost, massively faster.

**Models fetched**:
| Key | Open-Meteo model |
|-----|-----------------|
| `OM_ECMWF` | `ecmwf_ifs025` |
| `OM_GFS` | `gfs_seamless` |
| `OM_ICON` | `icon_seamless` |

**What it does**:
1. Calls `https://api.open-meteo.com/v1/forecast` with `daily=temperature_2m_mean`
2. Converts Celsius → Fahrenheit → HDD using the same `max(65 - T, 0)` formula
3. Saves each model to `data/open_meteo/{date}_OM_{model}_tdd.csv`

**Run ID format**: `20260221_OM` — clearly distinguishes Open-Meteo runs from primary model runs in `tdd_master.csv`.

**Trigger**: Automatically called by `daily_update.py` if both ECMWF and GFS return non-zero exit codes.

---

### 5. `compute_tdd.py` — Temperature → HDD Converter

**Role**: Core math engine. Reads raw GRIB2 files and computes HDD per day. Unchanged in V2.0.

**Key formula**:
```python
BASE_TEMP_F = 65

def kelvin_to_f(k):
    return (k - 273.15) * 9/5 + 32

def compute_tdd(temp_f):
    return max(65 - temp_f, 0)
```

**Two processing paths**:
| Model | File type | Averaging method |
|-------|-----------|-----------------|
| **ECMWF** | Single `.grib2` file | Averages across all lat/lon globally |
| **GFS** | One GRIB2 file per forecast day | Averages across all lat/lon per file |

**Output**: `{folder}/{run_id}_tdd.csv` with columns: `date, mean_temp (°F), tdd, model, run_id`

---

### 6. `merge_tdd.py` — Master Aggregator ⚡ V2.0 Fix

**Role**: Scans all computed HDD CSVs and merges them into one master file.

**V2.0 change**: Now also picks up `data/open_meteo/*_tdd.csv` fallback files:
```python
files = (
    glob("data/gfs/*_tdd.csv")
    + glob("data/ecmwf/*_tdd.csv")
    + glob("data/open_meteo/*_tdd.csv")   # Open-Meteo fallback
)
```

**Output**: `outputs/tdd_master.csv` — sorted by model, run_id, and date.

---

### 7. `compare_to_normal.py` — Forecast vs History ⚡ V2.0 Fix (CDD Bug Fixed)

**Role**: Compares forecast HDD/CDD against 30-year historical normals to generate trading signals.

**V2.0 fix — CDD anomaly was always wrong**:

> Previously: only merged `hdd_normal` and computed `tdd_anomaly = tdd - hdd_normal` for **every** date, including summer — producing grossly wrong signals in June/July/August
>
> Now: computes **both** `hdd_anomaly` and `cdd_anomaly` separately, plus a `forecast_cdd` column derived from `max(mean_temp - 65, 0)`

**New columns in `vs_normal.csv`**:
| Column | Description |
|--------|-------------|
| `hdd_anomaly` | `forecast_tdd - hdd_normal` (heating anomaly) |
| `forecast_cdd` | `max(mean_temp - 65, 0)` computed from forecast |
| `cdd_anomaly` | `forecast_cdd - cdd_normal` (cooling anomaly) |
| `anomaly` | Dominant: `cdd_anomaly` in Jun–Aug, `hdd_anomaly` otherwise |

**Console output** (per run):
```
ECMWF  20260220_00  |  HDD Avg: 26.9 (Normal: 22.6, +4.3)  |  CDD Avg: 0.0 (Normal: 0.0, +0.0)  |  → BULLISH
```

**Signal logic** (unchanged):
| `vs_normal_hdd` | Signal |
|-----------------|--------|
| > +0.5 | **BULLISH** 🐂 |
| < −0.5 | **BEARISH** 🐻 |
| −0.5 to +0.5 | **NEUTRAL** |

---

### 8. `run_change.py` — Run-over-Run Change ⚡ V2.0 Fix

**Role**: Tracks how much the total HDD forecast changed from the previous model run.

**V2.0 fix**: Previously only printed to console — now **saves to CSV**:

**Output**: `outputs/run_change.csv`
```
model, run_id, tdd, prev_tdd, hdd_change
ECMWF, 20260220_00, 430.88, 435.08, -4.20
GFS,   20260220_06, 422.36, 430.14, -7.78
```

**Use**: Informs the `send_telegram.py` run-change line and feeds Excel Power Query directly.

---

### 9. `compute_run_delta.py` — Per-Day TDD Delta ⚡ V2.0 Rewrite

**Role**: Computes the day-by-day TDD change between the two most recent runs of each model.

**V2.0 rewrite**: Previously read from a non-existent `outputs/gfs_tdd_master.csv` and used a `lead_day` column that didn't exist — would crash immediately. Now:
- Reads from `outputs/tdd_master.csv` (the correct file)
- Works for **each model** (ECMWF and GFS), not just GFS
- Matches forecast days by `date` (inner join) between latest and previous run

**Output**: `outputs/run_delta.csv`
```
date, tdd_latest, tdd_prev, tdd_change, model, run_latest, run_prev
2026-03-01, 26.52, 27.23, -0.71, ECMWF, 20260220_00, 20260219_00
```

---

### 10. `select_latest_run.py` — Latest Run Filter ⚡ V2.0 Rewrite

**Role**: Filters `tdd_master.csv` down to only the most recent run per model.

**V2.0 rewrite**: Previously read from a non-existent `outputs/gfs_tdd_master.csv` — would crash immediately. Now:
- Reads from `outputs/tdd_master.csv`
- Iterates over all models (ECMWF, GFS, OM_*)
- Writes one file per model

**Outputs**:
- `outputs/ecmwf_latest.csv`
- `outputs/gfs_latest.csv`

---

### 11. `compare_runs.py` — GFS Run Comparison

**Role**: Compares the two most recent GFS runs and writes a text summary. Unchanged in V2.0.

**Output**: `outputs/summaries/gfs_latest.txt`

---

### 12. `plot_gfs_tdd.py` — Chart Generator

**Role**: Creates a line chart comparing the latest GFS run, previous GFS run, and the 30-year normal. Unchanged in V2.0.

> ⚠️ **Pending (Phase 5)**: Not yet wired into `daily_update.py`. Output chart exists but isn't auto-generated or delivered anywhere.

**Output**: `outputs/gfs_tdd_chart.png`

---

### 13. `send_telegram.py` — Alert Sender ⚡ V2.0 Fix

**Role**: Composes and sends the daily weather desk report to a Telegram bot/channel.

**V2.0 fixes**:
- **Removed `parse_mode: "Markdown"`** — `run_id` values like `20260220_12` contain underscores that break Telegram's Markdown parser (underscores trigger italic formatting), causing malformed or undelivered messages
- **Short-run warning**: Instead of silently dropping runs with fewer than 10 forecast days, now prints a `WARNING:` line and sends a pipeline alert message if no qualifying runs exist at all
- Plain text format — no parsing mode needed

**Example Telegram message**:
```
WEATHER DESK -- 2026-02-21

ECMWF | Run: 20260220_00
Avg HDD/day: 26.9 | Normal: 22.6
vs Normal: +4.3 -- BULLISH
Run change: -4.2 HDD vs prev run

GFS | Run: 20260220_12
Avg HDD/day: 26.2 | Normal: 22.7
vs Normal: +3.5 -- BULLISH
Run change: -238.7 HDD vs prev run
```

**Secrets required**:
- `TELEGRAM_TOKEN`
- `TELEGRAM_CHAT_ID`

---

## 📊 Data Files Explained

### `data/normals/us_daily_normals.csv`
- **Source**: 30-year US climate normals (NOAA CPC 1981–2010, CONUS population-weighted)
- **Coverage**: Every day of the year (365 rows)
- **Committed to the repo** — no longer needs a workflow copy step to work

| Column | Description |
|--------|-------------|
| `month` | Month (1–12) |
| `day` | Day of month (1–31) |
| `mean_temp_f` | Average US temp in °F for that calendar day |
| `hdd_normal` | Historical average HDD for that day |
| `cdd_normal` | Historical average CDD for that day |

### `outputs/tdd_master.csv`
- Combined, unified HDD dataset from all model runs (GFS, ECMWF, Open-Meteo fallback)
- Columns: `date, mean_temp, tdd, model, run_id`

### `outputs/vs_normal.csv`
- Each forecast day's HDD and CDD vs historical normals
- New columns in V2.0: `hdd_anomaly`, `forecast_cdd`, `cdd_anomaly`, `anomaly`

### `outputs/run_change.csv` 🆕
- Run-to-run total HDD change per model
- Columns: `model, run_id, tdd, prev_tdd, hdd_change`

### `outputs/run_delta.csv` 🆕
- Day-by-day TDD change between the two latest runs of each model
- Columns: `date, tdd_latest, tdd_prev, tdd_change, model, run_latest, run_prev`

### `outputs/ecmwf_latest.csv` / `outputs/gfs_latest.csv` 🆕
- Latest run only, per model — for direct Excel Power Query ingestion without filtering

---

## 🤖 GitHub Actions Automation

**File**: `.github/workflows/daily_run.yml`

**Trigger**: Every day at **14:00 UTC** (2 PM UTC = 7:30 PM IST)

**What the workflow does**:
1. Checks out the repo
2. Installs Python 3.12 and all dependencies
3. Creates required directories: `data/ecmwf`, `data/gfs`, `data/normals`, `data/open_meteo`, `outputs`
4. Copies the normals file to `data/normals/` (also now committed directly)
5. Runs `python scripts/daily_update.py` with Telegram secrets injected
6. Commits **all** `outputs/*.csv`, `data/ecmwf/*_tdd.csv`, `data/gfs/*_tdd.csv` back to the repo

**Required GitHub Secrets**:
| Secret | Purpose |
|--------|---------|
| `TELEGRAM_TOKEN` | Telegram bot authentication token |
| `TELEGRAM_CHAT_ID` | Target Telegram chat/channel ID |

---

## 🔄 Full Data Flow Diagram (V2.0)

```
NOAA GFS Server ─────────────────────────────────────── ECMWF Open Data
(nomads.ncep.noaa.gov)                                  (data.ecmwf.int)
        │                                                       │
        ▼                                                       ▼
  fetch_gfs.py                                     fetch_ecmwf_ifs.py
  [.idx byte-range]                                [16-step validation]
        │                                                       │
        │  ~5-15KB per timestep                                 │  single .grib2
        ▼                                                       ▼
  data/gfs/{run_id}/                             data/ecmwf/{run_id}/
        │
        └──── IF BOTH FAIL ────► fetch_open_meteo.py
                                 [ECMWF/GFS/ICON JSON, no GRIB]
                                         │
                                         ▼
                                data/open_meteo/*_tdd.csv
        │                                │
        └────────────────┬───────────────┘
                         │
                         ▼
                   compute_tdd.py
                  (Kelvin → °F → HDD)
                         │
                         ▼
             data/gfs/*_tdd.csv
             data/ecmwf/*_tdd.csv
                         │
                         ▼
                   merge_tdd.py
                   (+ open_meteo)
                         │
                         ▼
              outputs/tdd_master.csv
                         │
          ┌──────────────┼──────────────────┬────────────────┐
          │              │                  │                │
          ▼              ▼                  ▼                ▼
compare_to_normal  run_change.py   compute_run_delta  select_latest_run
    (HDD+CDD)      → run_change.csv   → run_delta.csv   → *_latest.csv
          │
          ▼
  outputs/vs_normal.csv
  (BULLISH/BEARISH/NEUTRAL)
          │
          └──────────────────────────────────────────────────────┐
                                                                 │
                                                                 ▼
                                                         send_telegram.py
                                                         [plain text, no Markdown]
                                                                 │
                                                                 ▼
                                                        Telegram Channel Alert
```

---

## 🛠️ Dependencies Required

```bash
pip install cfgrib xarray pandas eccodes requests numpy ecmwf-opendata matplotlib
```

| Package | Purpose |
|---------|---------|
| `cfgrib` | Read GRIB2 meteorological files |
| `xarray` | Multi-dimensional array data handling |
| `pandas` | CSV manipulation and data analysis |
| `eccodes` | ECMWF GRIB encoding/decoding + GRIB message counting (step validation) |
| `requests` | HTTP requests to NOAA/ECMWF/Telegram/Open-Meteo APIs |
| `numpy` | Numerical operations |
| `ecmwf-opendata` | ECMWF Open Data client |
| `matplotlib` | Chart plotting |

---

## 💡 How to Run Locally

```bash
# Clone and enter the repo
cd weather-dd-tracker

# Install dependencies
pip install cfgrib xarray pandas eccodes requests numpy ecmwf-opendata matplotlib

# Set Telegram credentials
export TELEGRAM_TOKEN=your_bot_token
export TELEGRAM_CHAT_ID=your_chat_id

# Run the full pipeline
python3 scripts/daily_update.py

# Or run individual steps:
python3 scripts/fetch_gfs.py             # Step 1: Download GFS (byte-range t2m only)
python3 scripts/fetch_ecmwf_ifs.py       # Step 2: Download ECMWF (validated)
python3 scripts/fetch_open_meteo.py      # (Fallback only) Open-Meteo API
python3 scripts/compute_tdd.py           # Step 3: Compute HDDs
python3 scripts/merge_tdd.py             # Step 4: Merge all outputs
python3 scripts/compare_to_normal.py     # Step 5: Compare HDD+CDD to normals
python3 scripts/run_change.py            # Step 6: Check & save run changes
python3 scripts/compute_run_delta.py     # Step 6b: Per-day delta
python3 scripts/select_latest_run.py     # Step 6c: Extract latest run per model
python3 scripts/send_telegram.py         # Step 7: Send alert
```

> **Note**: Use `python3` on macOS (system Python). The `daily_update.py` orchestrator uses `sys.executable` automatically, so it always uses the correct interpreter regardless of OS.

---

## 🧠 Why This Matters (Use Case Context)

This tool is built for **natural gas energy trading desks**. Here's why it's useful:

- **HDD** directly drives **natural gas demand** — cold days mean more heating, which means more gas burned.
- **CDD** drives **power burn** — hot days mean more air conditioning, which consumes electricity generated by gas-fired plants.
- Comparing forecasts to **normals** tells traders if the upcoming period is expected to be warmer or colder than usual.
- Tracking **run-over-run changes** shows if weather models are trending toward more or less demand — a key signal for trading.
- The **BULLISH/BEARISH** labels translate meteorological data directly into market language.
- The **Open-Meteo fallback** ensures signals are always delivered even on days when NOMADS or ECMWF open-data servers are overloaded.

By automating this and sending it to Telegram daily, the system turns a labor-intensive analyst task into a zero-effort daily brief — with all output CSVs ready for **Excel Power Query** ingestion.

---

## 🚧 Pending — V2.0 Roadmap

| Item | Status |
|------|--------|
| Web dashboard (auto-updating chart output) | ⏳ Phase 5 — pending |
| HRRR (3km intraday, 18-hour) | ⏳ Layer 1 pending |
| GEFS / ECMWF ENS ensemble | ⏳ Layer 4 pending |
| ECMWF AIFS (AI model via ecmwf-opendata) | ⏳ Layer 5 Track A pending |
| Layer 6 derived market signals | ⏳ Pending |
| ECMWF vs GFS disagreement score | ⏳ Layer 2 pending |
