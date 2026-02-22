"""
fetch_icon.py

Purpose:
- Fetch the DWD ICON 13km global model using the Open-Meteo fast JSON API.
- Compute the weighted daily average temperature and TDD (Total Degree Days)
  across 17 representative US gas-demand cities.
- Save to a flat CSV matching the downstream model formats.

Why Open-Meteo for ICON?
  - DWD's raw GRIB file structure is complex and highly fragmented.
  - Open-Meteo aggregates the runs instantly and provides point-level precision
    for our exact gas-demand cities, skipping the heavy grid processing step.
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
try:
    from fetch_open_meteo import DEMAND_CITIES, TOTAL_WEIGHT, celsius_to_f, compute_tdd
except ImportError:
    # ── Gas-demand representative cities fallback ────────────────────────────────
    DEMAND_CITIES = [
        ("Boston", 42.36, -71.06, 4.0), ("New York", 40.71, -74.01, 6.0),
        ("Philadelphia", 39.95, -75.16, 3.0), ("Pittsburgh", 40.44, -79.99, 2.0),
        ("Detroit", 42.33, -83.05, 3.0), ("Cleveland", 41.50, -81.69, 2.0),
        ("Chicago", 41.85, -87.65, 5.0), ("Milwaukee", 43.04, -87.91, 1.5),
        ("Minneapolis", 44.98, -93.27, 2.5), ("Columbus", 39.96, -82.99, 1.5),
        ("Indianapolis", 39.77, -86.16, 1.5), ("Baltimore", 39.29, -76.61, 1.5),
        ("Charlotte", 35.23, -80.84, 1.0), ("Atlanta", 33.75, -84.39, 1.0),
        ("Dallas", 32.78, -96.80, 1.0), ("Kansas City", 39.09, -94.58, 0.8),
        ("St Louis", 38.63, -90.20, 0.8),
    ]
    TOTAL_WEIGHT = sum(w for _, _, _, w in DEMAND_CITIES)
    def celsius_to_f(c): return c * 9 / 5 + 32
    def compute_tdd(temp_f): return max(65.0 - temp_f, 0)

OUTPUT_DIR = Path("data/icon")
FORECAST_DAYS = 16

def fetch_city_temps_icon(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_mean",
        "temperature_unit": "celsius",
        "forecast_days": FORECAST_DAYS,
        "models": "icon_seamless",
        "timezone": "UTC",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        temps = daily.get("temperature_2m_mean", [])
        return {d: t for d, t in zip(dates, temps) if t is not None}
    except Exception as e:
        return None

def fetch_icon():
    # Use UTC today as the run date identifier
    run_date_str = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")
    run_id = f"{run_date_str}_00" # Open-Meteo seamless updates continuously, we represent it as the daily run
    
    out_dir = OUTPUT_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "icon_tdd.csv"

    print(f"Fetching ICON (Open-Meteo) across {len(DEMAND_CITIES)} demand cities...")

    city_data = {}
    failed = 0
    for name, lat, lon, weight in DEMAND_CITIES:
        temps = fetch_city_temps_icon(lat, lon)
        if temps:
            city_data[name] = (weight, temps)
        else:
            print(f"  [ERR] {name} failed - excluded from average")
            failed += 1

    if not city_data:
        raise RuntimeError("All cities failed to fetch ICON data.")

    # Compute weighted daily average
    all_dates = sorted(set(d for _, temps in city_data.values() for d in temps))
    rows = []
    
    # In ECMWF/GFS pipelines, they usually output TDD. 
    # But some pipelines output the grid. Since this is direct point data, 
    # we output the TDD and mean_temp directly.
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
            "date": dt_str,
            "mean_temp": round(avg_f, 2),
            "tdd": round(compute_tdd(avg_f), 2),
            "model": "ICON",
            "run_id": run_id,
        })

    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    
    active = len(DEMAND_CITIES) - failed
    print(f"[OK] ICON TDD computed for {len(rows)} days.")
    print(f"     Active cities: {active}/{len(DEMAND_CITIES)}")
    print(f"     Saved -> {out_path}")

if __name__ == "__main__":
    fetch_icon()
