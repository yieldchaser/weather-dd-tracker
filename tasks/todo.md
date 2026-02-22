# Weather DD Tracker - Implementation Workflow (Nat Gas Focus)

## Phase 1: Expand Physics & AI Access (Lowest Hanging Fruit)
- [ ] 1.1 Pull ECMWF AIFS via `ecmwf-opendata` SDK.
- [ ] 1.2 Add HRRR physics model to daily polling (Critical for intraday power burn forecasting).
- [ ] 1.3 Add NAM and ICON physics models to daily polling.
- [ ] 1.4 Integrate GEFS ensemble (subset to 2-meter temperature `t2m`).
- [ ] 1.5 Integrate ECMWF ENS (subset to 2-meter temperature `t2m`).
- [ ] 1.6 Update NBM (National Blend of Models) if an accessible source is found (currently marked as Pending).

## Phase 2: Establish the Kaggle API Link (Track B Hub)
- [x] 2.1 Write Kaggle Notebook template housing `earth2studio` and `ai-models` for Track B GPU inferences.
- [x] 2.2 Configure GitHub Actions webhook (`kaggle kernels push`) in `.github/workflows/daily_run.yml`.
- [x] 2.3 Document instructions for USER to generate and store Kaggle API keys in GitHub Secrets.

## Phase 3: Deep Market Logic Integration
- [ ] 3.1 Build the Physics (e.g., ECMWF/GFS) vs AI (e.g., AIFS/GraphCast) disagreement matrix.
- [ ] 3.2 Map localized cooling grids to finalize Power Burn Proxy for Nat Gas markets.
- [ ] 3.3 Construct Composite Bullish/Bearish Score (HDD + CDD + Anomaly + Model Agreement).
- [ ] 3.4 Develop Wind/Solar Generation Proxy (for holistic power generation offset tracking).
- [ ] 3.5 Storage Draw/Injection Weekly Estimate logic.
