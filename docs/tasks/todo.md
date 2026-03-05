# Weather DD Tracker — Outstanding Tasks
*Last updated: 2026-03-05*

## ✅ COMPLETED (all phases 1–12 + hardening)
All items from Phases 1–12 are complete. See docs/ROADMAP.md for full history.

### ✅ Recently Completed (2026-03-05)
- [x] **ECMWF AIFS pipeline stall investigation & fix** — partial-GRIB detection guard, step=360 completeness check, missing `manifest.json` recovery, `pipeline_state.json` AIFS key added, CI `git add` path fixed for `ecmwf_aifs/` and `ecmwf_ens/`
- [x] **HRRR / NAM manifest hardening** — skip guard now validates `forecast_hours` list length, not just file existence; auto-removes empty manifests
- [x] **Model Convergence Detector** — fully operational in `build_model_shift_table.py`; fires convergence alert to console + saves `outputs/convergence_alert.csv`; integrated into dashboard
- [x] **Fast Revision Detector (>1 HDD/day)** — operational via `run_change.csv`; displayed on dashboard as Fast Revision Alert banner

---

## 🔮 NEXT PHASE — Future Enhancements

### High Value (do when ready)
- [ ] **EIA Weekly Storage Draw Estimate** — integrate weekly EIA-914 production data + HDD model to produce a same-week storage draw estimate before Thursday's report
- [ ] **Convergence Alert → Telegram** — the convergence detector saves a CSV but does not yet fire a Telegram message; wire `convergence_alert.csv` into `send_telegram.py`
- [ ] **Ensemble Spread as Volatility Proxy** — output GEFS/ENS spread as an implied uncertainty signal for options positioning
- [ ] **Composite Signal Backtesting** — run the 7-system composite against 2020–2025 Henry Hub moves to calibrate confidence thresholds

### Medium Value
- [ ] **UKMET / ARPEGE via Open-Meteo** — low-cost addition, adds 2 more independent model voices
- [ ] **Sub-seasonal (Week 3–4) Probabilistic Outlook** — use GEFS_35D + teleconnection regime to generate a 3-week probabilistic surface
- [ ] **Season Auto-Transition** — auto-switch HDD↔CDD↔TDD weighting based on calendar month without manual config

### Low Value / Nice to Have
- [ ] **REST API endpoint** — expose `outputs/*.json` via a lightweight FastAPI layer for programmatic consumers
- [ ] **Interactive dashboard** — replace static GitHub Pages with a Vite/Next.js app for drill-down capabilities
- [ ] **Canadian GEPS / UK MOGREPS-G** — additional ensemble voices (low priority, CMA/UKMO data access is complex)

---

## 🏗️ Infrastructure Upgrade Path (when budget allows)
| Upgrade | Cost | Unlocks |
|---|---|---|
| Small VPS ($5/mo) | ~$60/yr | True 6-hourly pipeline, every GFS cycle |
| ECMWF real-time | ~$500/yr | Removes 10-day open-data lag |
| PostgreSQL on VPS | ~$0 extra | Replace flat CSVs with queryable time-series DB |

