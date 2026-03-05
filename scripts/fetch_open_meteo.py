"""
fetch_open_meteo.py

Open-Meteo API fallback for 2m temperature data.
Used when primary sources (NOMADS/GFS or ecmwf-opendata) fail.

City list expanded to 79 cities (see demand_constants.py).
Uses om_batch_fetch for efficient batched API calls instead of
one HTTP request per city.
"""

import datetime
import os
import requests
import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path("data/open_meteo")
BASE_TEMP_F = 65.0
FORECAST_DAYS = 16

from demand_constants import DEMAND_CITIES, TOTAL_WEIGHT
from om_batch_fetch import fetch_all_cities_batch

OM_FORECAST_ENDPOINT = "https://api.open-meteo.com/v1/forecast"

# Open-Meteo models to pull in fallback
OM_MODELS = {
    "OM_ECMWF":  "ecmwf_ifs025",
    "OM_GFS":    "gfs_seamless",
    "OM_ICON":   "icon_seamless",
    "OM_UKMET":  "uk_met_office_seamless",
    "OM_ARPEGE": "meteofrance_arpege_europe",
}


def celsius_to_f(c):
    return c * 9 / 5 + 32

def compute_tdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)


def fetch_open_meteo(model_key, om_model_name, run_date_str):
    """
    Fetch 16-day weighted-average city temperature for all DEMAND_CITIES.
    Returns a DataFrame with date, mean_temp, tdd, model, run_id.
    """
    print(f"  Fetching Open-Meteo [{model_key}] ({om_model_name}) "
          f"across {len(DEMAND_CITIES)} demand cities (batched)...")

    city_data = fetch_all_cities_batch(
        endpoint=OM_FORECAST_ENDPOINT,
        model=om_model_name,
        forecast_days=FORECAST_DAYS,
    )

    if not city_data:
        print(f"  [ERR] [{model_key}]: all cities failed")
        return None

    # Compute weighted daily average across successful cities
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
            "model":     model_key,
            "run_id":    f"{run_date_str}_OM",
        })

    if not rows:
        print(f"  [ERR] [{model_key}]: no dates computed")
        return None

    active_w = sum(w for _, (w, _) in city_data.items())
    print(f"  [OK] [{model_key}]: {len(rows)} days | "
          f"{len(city_data)}/{len(DEMAND_CITIES)} cities | "
          f"{active_w:.1f}/{TOTAL_WEIGHT:.1f} weight-pts active")
    return pd.DataFrame(rows)


def fetch_all_fallback():
    """
    Fetch all configured Open-Meteo models and save combined output.
    Called when primary model fetches (ECMWF + GFS) both fail.
    Returns list of (model_key, output_path) tuples.
    """
    run_date_str = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    saved = []
    for model_key, om_model_name in OM_MODELS.items():
        df = fetch_open_meteo(model_key, om_model_name, run_date_str)
        if df is None or df.empty:
            continue
        run_id = f"{run_date_str}_OM"
        out_path = OUTPUT_DIR / f"{run_id}_{model_key}_tdd.csv"
        df.to_csv(out_path, index=False)
        print(f"  Saved -> {out_path}")
        saved.append((model_key, str(out_path)))

    if saved:
        print(f"\n[OK] Open-Meteo fallback: {len(saved)} model(s) saved.")
    else:
        print("\n[ERR] Open-Meteo fallback: all models failed.")
    return saved


if __name__ == "__main__":
    results = fetch_all_fallback()
    for model_key, path in results:
        print(f"  {model_key}: {path}")
