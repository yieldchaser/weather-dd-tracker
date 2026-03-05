"""
fetch_icon.py

Purpose:
- Fetch the DWD ICON 13km global model using the Open-Meteo fast JSON API.
- Compute the weighted daily average temperature and TDD (Total Degree Days)
  across 79 representative US gas-demand cities (see demand_constants.py).
- Save to a flat CSV matching the downstream model formats.

Why Open-Meteo for ICON?
  - DWD's raw GRIB file structure is complex and highly fragmented.
  - Open-Meteo aggregates the runs instantly and provides point-level precision
    for our exact gas-demand cities, skipping the heavy grid processing step.
  - Uses om_batch_fetch for batched API calls (2 requests vs 79 serial calls).
"""

import datetime
import os
import requests
import pandas as pd
from pathlib import Path

# Try importing the demand cities from the fallback script if it exists,
# otherwise define them here to keep the script self-contained.
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from demand_constants import DEMAND_CITIES, TOTAL_WEIGHT, compute_tdd
from om_batch_fetch import fetch_all_cities_batch
def celsius_to_f(c): return c * 9 / 5 + 32

OM_FORECAST_ENDPOINT = "https://api.open-meteo.com/v1/forecast"

OUTPUT_DIR = Path("data/icon")
FORECAST_DAYS = 16


def fetch_icon():
    # Use UTC today as the run date identifier
    run_date_str = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")
    run_id = f"{run_date_str}_00"  # Open-Meteo seamless updates continuously

    out_dir = OUTPUT_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "icon_tdd.csv"

    print(f"Fetching ICON (Open-Meteo) across {len(DEMAND_CITIES)} demand cities (batched)...")

    city_data = fetch_all_cities_batch(
        endpoint=OM_FORECAST_ENDPOINT,
        model="icon_seamless",
        forecast_days=FORECAST_DAYS,
    )

    if not city_data:
        raise RuntimeError("All cities failed to fetch ICON data.")

    # Compute weighted daily average
    all_dates = sorted(set(d for _, temps in city_data.values() for d in temps))
    rows = []
    for dt_str in all_dates:
        total_w = 0.0
        weighted_temp = 0.0
        for name, (weight, temps) in city_data.items():
            if dt_str in temps:
                weighted_temp += weight * temps[dt_str]
                total_w += weight

        if total_w == 0:
            continue

        avg_c = weighted_temp / total_w
        avg_f = celsius_to_f(avg_c)
        rows.append({
            "date":      dt_str,
            "mean_temp": round(avg_f, 2),
            "tdd":       round(compute_tdd(avg_f), 2),
            "model":     "ICON",
            "run_id":    run_id,
        })

    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)

    print(f"[OK] ICON TDD computed for {len(rows)} days.")
    print(f"     Active cities: {len(city_data)}/{len(DEMAND_CITIES)}")
    print(f"     Saved -> {out_path}")

if __name__ == "__main__":
    fetch_icon()
