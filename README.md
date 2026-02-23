# Weather Degree Day Tracker

Automated quantitative weather intelligence platform designed for Natural Gas derivatives trading. It autonomously tracks atmospheric models, calculates gas-weighted degree days (HDD/CDD), computes predictive market logic, and delivers structured reports via Telegram.

## Core Features

- **Monolithic Data Pipeline**: End-to-end execution `poll_models.py -> daily_update.py` handling over 14 discrete weather models.
- **Gas-Weighted Precision**: Temperature data is interpolated across true EIA county-level consumption density grids, dynamically scaled per season.
- **Physics + AI Dual Track**:
  - GFS, ECMWF (HRES & ENS), GEFS, HRRR, ICON, NAM
  - Kaggle T4 GPU triggered automated inference for *AIFS, GraphCast, PanguWeather, FourCastNet*.
- **Market Proxy Logic**:
  - AI vs Physics Structural Disagreement Index (Volatility Tracking).
  - Power Burn and Renewables Anomaly models.
  - Basin Production Freeze-Off Models.
  - Multi-run Model Shift Consensus matrix.
- **Trading-Grade Reporting**:
  - Text-based alerts parsing the raw data into actionable Bullish/Bearish biases per duration window.
  - Historical context tracking (e.g. "4th consecutive bearish revision").

## Technical Architecture

The architecture is outlined completely in the `CODE_ANALYSIS.md` and `MASTER_ARCHITECTURE_PLAN.md` documents. 
- Execution is driven by GitHub Actions hitting `poll_models.py` via cron to assess remote server (NOMADS/ECMWF) completeness before waking the monolith.
- `daily_update.py` enforces sequential integrity, ensuring that ML evaluation executes strictly after dataset normalization (interpolating Spatial weights into TDD outputs).

## Disclaimer
This software provides programmatic weather summaries based on freely available datasets. It constitutes neither financial advice nor a recommendation to execute transactions in commodity futures or any other asset class.