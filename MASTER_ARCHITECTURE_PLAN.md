# Weather DD Tracker — Master Architecture Plan & End Goals

## CORE PHILOSOPHY
Free. Automatable. Resilient. If a free resource exists, we exploit its API. If one pipeline breaks, a fallback triggers. All data resolves to flat CSV files that feed seamlessly into complex Excel financial models and Telegram alerts.

---

## INFRASTRUCTURE STACK
### 1. GitHub Actions (The Master Controller)
**Role:** The central nervous system. 
**Schedule:** Runs auto-polling every 15 minutes during official model dissemination windows.
**Cost:** 100% Free (Unlimited minutes for public repos).
**Tasks:** Fetches all Layer 1 physics models, triggers Layer 5 AI models via Kaggle API, computes HDD/CDD anomalies, merges outputs, and sends Telegram signals.

### 2. Kaggle API (The Free GPU Engine)
**Role:** Replaces Google Colab. Bypasses headless browser bans.
**Specs:** 30 hours per week of free NVIDIA T4/P100 GPU compute with 16GB VRAM and 30GB system RAM.
**Tasks:** Wakes up via a GitHub Actions `kaggle kernels push` webhook, runs the heavy Track B AI models instantly, and pushes CSVs back to the GitHub repo.

### 3. Open-Meteo API (The Safety Net)
**Role:** The non-commercial fallback.
**Tasks:** Used instantly if NOMADS (GFS/NAM) or AWS Open Data servers go down. Returns ECMWF, GFS, ICON, and ARPEGE in a single, fast JSON call without needing to parse heavy GRIB files.

---

## LAYER 1 — CORE PHYSICS WEATHER ENGINE
*All models below are pre-computed. GitHub Actions downloads the output only.*

**Deterministic Models (Short to Medium Range):**
*   **ECMWF IFS HRES** (0.1°, 9km, 10-day) — ✅ Active (via ecmwf-opendata SDK)
*   **GFS** (0.25°, 28km, 16-day) — ✅ Active (via AWS/NOMADS)
*   **HRRR** (3km, hourly updates, 18-hour) — ❌ High Priority (Critical for intraday power burn forecasting)
*   **NAM** (12km, 84-hour) — ❌ Pending (via NOMADS)
*   **ICON** (13km global) — ❌ Pending (via DWD / Open-Meteo)
*   **UKMET & ARPEGE & JMA GSM** — ❌ Pending (via AWS / Open-Meteo)
*   **NBM** (National Blend of Models) — ✅ Active (The US consensus signal)

**Ensemble Models (Probabilistic Layer):**
*Constraint Warning: GitHub Actions will crash on full global ensemble GRIBs. We must use AWS S3 range requests to subset to 2-meter temperature (`t2m`) only.*
*   **GEFS** (0.5°, 31 members) — ❌ Pending
*   **ECMWF ENS** (51 members) — ❌ Pending (via ecmwf-opendata SDK)
*   **Canadian GEPS & UK MOGREPS-G** — ❌ Pending

---

## LAYER 2 — MODEL CHANGE & DISAGREEMENT ENGINE
*This is the alpha. The speed of the run-to-run change matters more than the absolute HDD number.*

*   **Run-to-run HDD change per model** — ✅ Active
*   **Run-to-run CDD change per model** — ✅ Active
*   **ECMWF vs. GFS disagreement score** — ✅ Active (via Model Consensus Shift Table)
*   **Physics vs. AI disagreement score** — ❌ Pending
*   **5-day and 10-day rolling trend change** — ❌ Pending
*   **Fast revision detection** (Flags any model moving >3 HDD in a single run) — ❌ Pending
*   **Model convergence detector** (Flags when diverging models suddenly align) — ❌ Pending

---

## LAYER 3 — NORMAL VS FORECAST ENGINE
*   **Normals source:** NOAA CPC 1981-2010 CONUS population & EIA True Gas-Weighted Grids — ✅ Active
*   **HDD vs. normal** — ✅ Active
*   **CDD vs. normal** — ✅ Active
*   **BULLISH / BEARISH / NEUTRAL daily indicator** — ✅ Active
*   **Seasonal Crossover Engine** (HDD to CDD transition metrics) — ✅ Active
*   **5, 10, and 15-day anomaly windows** — ❌ Pending
*   **Percentile ranking** (Current anomaly vs. 30-year history) — ❌ Pending

---

## LAYER 4 — ENSEMBLE INTELLIGENCE
*   **Ensemble mean HDD/CDD per model** — ❌ Pending
*   **Ensemble spread** (Used as a market uncertainty/volatility proxy) — ❌ Pending
*   **Probability of above/below normal HDD** — ❌ Pending
*   **All-model consensus direction and Confidence Score** — ❌ Pending

---

## LAYER 5 — AI WEATHER MODELS (The Dual-Track Architecture)
*The most significant upgrade. Split to maximize free hardware limits.*

**Track A — No GPU Needed (GitHub Actions Native)**
*   **ECMWF AIFS** (30km, deterministic + ensemble): ❌ Highest Priority AI Addition. ECMWF pre-computes this. It requires zero extra infrastructure, no Colab, no Kaggle. We pull it via the exact same `ecmwf-opendata` SDK used for HRES.

**Track B — GPU Required (The Kaggle API Hijack)**
*GitHub Actions pings the Kaggle API. Kaggle spins up a 16GB VRAM T4 GPU, runs the inference, and pushes the CSV back to your repo. All models below are managed via the unified `earth2studio` and `ai-models` libraries.*
*   **NVIDIA Earth-2 Medium Range (Atlas):** 15-day forecast. Crucial Hack: We must subset this 2.5B parameter model in `earth2studio` to only output `t2m` (temperature) and `u10m/v10m` (wind) to prevent Kaggle's 16GB VRAM from crashing.
*   **NVIDIA Earth-2 Nowcasting (StormScope):** 0 to 6-hour hyper-local storm prediction. Outperforms traditional physics on short-term horizons; critical for sudden intraday generation drops.
*   **Pangu-Weather (Huawei):** 1.4 seconds per step, natively supported by `ai-models`. — ✅ Active
*   **FourCastNetV2 (NVIDIA):** Handled cleanly within `earth2studio`. — ✅ Active
*   **Aurora (Microsoft):** Outstanding for extreme temperature events.
*   **GraphCast (Google DeepMind):** Will be run at a slightly downscaled resolution to prevent Kaggle OOM errors. — ✅ Active

---

## LAYER 6 — DERIVED MARKET SIGNALS
*Translating meteorological data into actionable energy signals.*

*   **Natural Gas Demand Proxy** (HDD weighted by heating fuel consumption mix) — ✅ Active (via EIA 48-State Interpolation Grid)
*   **US Produce Basin Freeze-Offs Forecast** (Estimate MMcf/d losses in Permian/Bakken) — ✅ Active
*   **Composite Bullish/Bearish Score** (HDD + CDD + Anomaly + Model Agreement) — ❌ Pending
*   **Power Burn Proxy** (CDD weighted by electricity cooling demand) — ❌ Pending
*   **Wind/Solar Generation Proxy** (Derived from u100m wind speed and surface radiation fields) — ❌ Pending
*   **Storage Draw/Injection Weekly Estimate** — ❌ Pending
*   **Implied Volatility Signal** (Ensemble spread + Model disagreement index) — ❌ Pending

---

## LAYER 7 — DELIVERY SYSTEM
*   **GitHub Actions full automation** (Event-Driven Smart Polling) — ✅ Active
*   **Telegram daily signal** (Includes Text Tear-Sheets & Multi-Run Charts) — ✅ Active
*   **Output to flat CSV/XLSX** (Ready for Excel Power Query ingestion) — ✅ Active
*   **Dynamic 21-Year Historical HDD Threshold Matrix** — ✅ Active
*   **Web dashboard** (Auto-updating) — ❌ Pending
*   **REST API endpoint** — ❌ Pending

---

## BUILD ORDER — THE EXACT NEXT STEPS

**Phase 1: Expand Physics & AI Access (Lowest Hanging Fruit)**
- Pull ECMWF AIFS via the existing Python SDK (No GPU required).
- Add HRRR, NAM, and ICON physics models to the daily polling rotation.
- Integrate GEFS and ECMWF ENS (with strict variable subsetting to avoid crashing GitHub).

**Phase 2: Establish the Kaggle API Link (Track B Hub)**
- Generate Kaggle API keys and store them in GitHub Actions Secrets.
- Write a single Kaggle Notebook housing `earth2studio` and `ai-models`.
- Configure the `kaggle kernels push` command in the `.github/workflows/daily_run.yml` webhook.

**Phase 3: Deep Market Logic Integration**
- Build the remaining Physics vs AI disagreement matrix.
- Map out localized cooling grids to finalize the Power Burn Proxy.
