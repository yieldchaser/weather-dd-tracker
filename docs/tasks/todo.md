# Weather DD Tracker — Outstanding Tasks
*Last updated: 2026-03-03*

## ✅ COMPLETED (all phases 1–12 + hardening)
All items from Phases 1–12 are complete. See docs/ROADMAP.md for full history.

---

## 🔮 NEXT PHASE — Future Enhancements

### High Value (do when ready)
- [ ] **EIA Weekly Storage Draw Estimate** — integrate weekly EIA-914 production data + HDD model to produce a same-week storage draw estimate before Thursday's report
- [ ] **Model Convergence Detector** — fire a Telegram alert when multi-model spread collapses below 1.0 HDD/day (all models agree)
- [ ] **Fast Revision Detector (>3 HDD single run)** — already partially in place, needs threshold surfaced to Telegram
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
