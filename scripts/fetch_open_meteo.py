"""
fetch_open_meteo.py

Open-Meteo API fallback for 2m temperature data.
Used when primary sources (NOMADS/GFS or ecmwf-opendata) fail.

Returns ECMWF, GFS, ICON, and ARPEGE t2m data in a single fast JSON call
with no GRIB parsing required.

API docs: https://open-meteo.com/en/docs
Free tier: unlimited for non-commercial use, no API key needed.

Output: data/open_meteo/<run_id>_tdd.csv
  - run_id format: YYYYMMDD_OM (identifies this as Open-Meteo sourced)
  - Columns: date, mean_temp, tdd, model, run_id
"""

import datetime
import json
import os
import requests
import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path("data/open_meteo")

# CONUS population-weighted centroid (approximate)
# Matches the weighting approach of the existing GFS/ECMWF nationwide average
LATITUDE = 39.5   # ~CONUS center
LONGITUDE = -98.4

# Open-Meteo forecast models to pull
OM_MODELS = {
    "OM_ECMWF": "ecmwf_ifs025",
    "OM_GFS":   "gfs_seamless",
    "OM_ICON":  "icon_seamless",
}

BASE_TEMP_F = 65.0
FORECAST_DAYS = 16


def kelvin_to_f(k):
    return (k - 273.15) * 9 / 5 + 32


def celsius_to_f(c):
    return c * 9 / 5 + 32


def compute_tdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)


def fetch_open_meteo(model_key, om_model_name, run_date_str):
    """
    Fetch 16-day daily mean 2m temperature from Open-Meteo for one model.
    Returns a DataFrame with date, mean_temp, tdd, model, run_id.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "daily": "temperature_2m_mean",
        "temperature_unit": "celsius",
        "forecast_days": FORECAST_DAYS,
        "models": om_model_name,
        "timezone": "UTC",
    }

    print(f"  Fetching Open-Meteo [{model_key}] ({om_model_name})...")
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ✖ Open-Meteo [{model_key}] failed: {e}")
        return None

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    temps_c = daily.get("temperature_2m_mean", [])

    if not dates or not temps_c:
        print(f"  ✖ Open-Meteo [{model_key}]: empty response")
        return None

    rows = []
    for dt_str, tc in zip(dates, temps_c):
        if tc is None:
            continue
        tf = celsius_to_f(tc)
        rows.append({
            "date": dt_str,
            "mean_temp": round(tf, 2),
            "tdd": round(compute_tdd(tf), 2),
            "model": model_key,
            "run_id": f"{run_date_str}_OM",
        })

    df = pd.DataFrame(rows)
    print(f"  ✔ [{model_key}]: {len(df)} days fetched")
    return df


def fetch_all_fallback():
    """
    Fetch all configured Open-Meteo models and save combined output.
    Called when primary model fetches fail.
    Returns list of (model_key, output_path) tuples for successful fetches.
    """
    run_date_str = datetime.datetime.utcnow().strftime("%Y%m%d")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    saved = []

    for model_key, om_model_name in OM_MODELS.items():
        df = fetch_open_meteo(model_key, om_model_name, run_date_str)
        if df is None or df.empty:
            continue

        run_id = f"{run_date_str}_OM"
        out_path = OUTPUT_DIR / f"{run_id}_{model_key}_tdd.csv"
        df.to_csv(out_path, index=False)
        print(f"  Saved → {out_path}")
        saved.append((model_key, str(out_path)))

    if saved:
        print(f"\n✔ Open-Meteo fallback: {len(saved)} model(s) saved.")
    else:
        print("\n✖ Open-Meteo fallback: all models failed.")

    return saved


if __name__ == "__main__":
    results = fetch_all_fallback()
    for model_key, path in results:
        print(f"  {model_key}: {path}")
