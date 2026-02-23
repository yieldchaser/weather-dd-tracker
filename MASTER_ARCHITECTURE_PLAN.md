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
*   **ECMWF IFS HRES** (0.1°, 9km, 10-day) — ✅ Active
*   **GFS** (0.25°, 28km, 16-day) — ✅ Active
*   **HRRR** (3km, hourly, 18-hour) — ✅ Active (byte-range t2m via NOMADS)
*   **NAM** (12km, 84-hour) — ✅ Active (via NOMADS)
*   **ICON** (13km global) — ✅ Active (via Open-Meteo JSON)
*   **NBM** (National Blend of Models) — ✅ Active

**Ensemble Models (Probabilistic Layer):**
*   **GEFS** (0.5°, 31 members) — ✅ Active
*   **ECMWF ENS** (51 members) — ✅ Active (ensemble mean via ecmwf-opendata)
*   **ECMWF AIFS** (AI-native, deterministic) — ✅ Active (Track A, no GPU required)
*   **Canadian GEPS & UK MOGREPS-G** — ❌ Pending (low priority)

---

## LAYER 2 — MODEL CHANGE & DISAGREEMENT ENGINE
*This is the alpha. The speed of the run-to-run change matters more than the absolute HDD number.*

*   **Run-to-run HDD change per model** — ✅ Active
*   **Run-to-run CDD change per model** — ✅ Active
*   **ECMWF vs. GFS disagreement score** — ✅ Active (via Model Consensus Shift Table)
*   **Physics vs. AI disagreement score** — ✅ Active (`physics_vs_ai_disagreement.py`)
*   **5-day and 10-day rolling trend change** — ❌ Pending
*   **Fast revision detection** (>3 HDD single run) — ❌ Pending
*   **Model convergence detector** — ❌ Pending

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

*   **Natural Gas Demand Proxy** (HDD weighted by heating fuel consumption mix) — ✅ Active
*   **US Produce Basin Freeze-Offs Forecast** (Permian/Anadarko/Appalachia/Bakken MMcf/d) — ✅ Active
*   **Composite Bullish/Bearish Score** (TDD anomaly vs normals + disagreement + wind) — ✅ Active
*   **Power Burn Proxy** (CDD weighted to ERCOT/Southeast peakers) — ✅ Active
*   **Wind/Solar Generation Proxy** (Wind dropout → physical spot gas signal) — ✅ Active
*   **Storage Draw/Injection Weekly Estimate** — ❌ Pending
*   **Implied Volatility Signal** (Ensemble spread + model disagreement index) — ❌ Pending

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

**Phase 13: Next Priorities**
- Storage draw/injection weekly estimate (EIA weekly data integration)
- Model convergence detector (flags sudden multi-model alignment)
- Fast revision detection (>3 HDD single run flag in Telegram)
- UKMET / ARPEGE via Open-Meteo (low-cost addition)
- Web dashboard (auto-updating, static output from existing CSVs)
