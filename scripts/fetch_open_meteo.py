"""
fetch_open_meteo.py

Open-Meteo API fallback for 2m temperature data.
Used when primary sources (NOMADS/GFS or ecmwf-opendata) fail.

FIX (Issue #1): Replaced single CONUS centroid point with a weighted
multi-city average covering the major Henry Hub gas demand regions.
A single point at lat=39.5N, lon=-98.4W (Kansas) had no statistical
relationship to Northeast/Midwest heating demand which drives HH prices.

New approach: 17 representative cities with demand-proportional weights.
Weights mirror the gas-consumption pattern used in the Phase 2 GW grid.
"""

import datetime
import os
import requests
import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path("data/open_meteo")
BASE_TEMP_F = 65.0
FORECAST_DAYS = 16

# ── Gas-demand representative cities ─────────────────────────────────────────
# (name, lat, lon_WE, demand_weight)
# Weights reflect regional residential+commercial gas consumption intensity.
# Northeast and Great Lakes carry the highest weights for Henry Hub signal.
DEMAND_CITIES = [
    # Northeast - pipeline constraints + highest HDD sensitivity
    ("Boston",       42.36, -71.06, 4.0),
    ("New York",     40.71, -74.01, 6.0),
    ("Philadelphia", 39.95, -75.16, 3.0),
    ("Pittsburgh",   40.44, -79.99, 2.0),
    # Great Lakes / Midwest
    ("Detroit",      42.33, -83.05, 3.0),
    ("Cleveland",    41.50, -81.69, 2.0),
    ("Chicago",      41.85, -87.65, 5.0),
    ("Milwaukee",    43.04, -87.91, 1.5),
    ("Minneapolis",  44.98, -93.27, 2.5),
    ("Columbus",     39.96, -82.99, 1.5),
    ("Indianapolis", 39.77, -86.16, 1.5),
    # Mid-Atlantic / Appalachian
    ("Baltimore",    39.29, -76.61, 1.5),
    # Southeast interior
    ("Charlotte",    35.23, -80.84, 1.0),
    ("Atlanta",      33.75, -84.39, 1.0),
    # South Central (production + demand)
    ("Dallas",       32.78, -96.80, 1.0),
    ("Kansas City",  39.09, -94.58, 0.8),
    ("St Louis",     38.63, -90.20, 0.8),
]

TOTAL_WEIGHT = sum(w for _, _, _, w in DEMAND_CITIES)

# Open-Meteo models to pull in fallback
OM_MODELS = {
    "OM_ECMWF": "ecmwf_ifs025",
    "OM_GFS":   "gfs_seamless",
    "OM_ICON":  "icon_seamless",
}


def celsius_to_f(c):
    return c * 9 / 5 + 32


def compute_tdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)


def fetch_city_temps(lat, lon, om_model_name, forecast_days=FORECAST_DAYS):
    """
    Fetch daily mean 2m temperature from Open-Meteo for one city.
    Returns dict of {date_str: temp_celsius} or None on failure.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_mean",
        "temperature_unit": "celsius",
        "forecast_days": forecast_days,
        "models": om_model_name,
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


def fetch_open_meteo(model_key, om_model_name, run_date_str):
    """
    Fetch 16-day weighted-average city temperature for all DEMAND_CITIES.
    Returns a DataFrame with date, mean_temp, tdd, model, run_id.
    """
    print(f"  Fetching Open-Meteo [{model_key}] ({om_model_name}) "
          f"across {len(DEMAND_CITIES)} demand cities...")

    # Collect per-city daily temperatures
    city_data = {}
    failed = 0
    for name, lat, lon, weight in DEMAND_CITIES:
        temps = fetch_city_temps(lat, lon, om_model_name)
        if temps:
            city_data[name] = (weight, temps)
        else:
            print(f"    [ERR] {name} failed - excluded from average")
            failed += 1

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

    active = len(DEMAND_CITIES) - failed
    print(f"  [OK] [{model_key}]: {len(rows)} days | "
          f"{active}/{len(DEMAND_CITIES)} cities | "
          f"weighted avg ({TOTAL_WEIGHT - sum(DEMAND_CITIES[i][3] for i in range(failed))} of {TOTAL_WEIGHT} weight-pts)")
    return pd.DataFrame(rows)


def fetch_all_fallback():
    """
    Fetch all configured Open-Meteo models and save combined output.
    Called when primary model fetches (ECMWF + GFS) both fail.
    Returns list of (model_key, output_path) tuples.
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
        print(f"\n[OK] Open-Meteo fallback: {len(saved)} model(s) saved.")
    else:
        print("\n[ERR] Open-Meteo fallback: all models failed.")
    return saved


if __name__ == "__main__":
    results = fetch_all_fallback()
    for model_key, path in results:
        print(f"  {model_key}: {path}")
