"""
fetch_ecmwf_ens.py

Fetches ECMWF ENS (Ensemble) 2m temperature using the Open-Meteo API.
Uses `ecmwf_ifs04` (the 0.4-degree ensemble API).
Uses the 17-city gas-demand weighted average strategy.
"""

import os
import requests
import datetime
import pandas as pd
from pathlib import Path

BASE_DIR = Path("data/ecmwf_ens")
BASE_DIR.mkdir(parents=True, exist_ok=True)
BASE_TEMP_F = 65.0
FORECAST_DAYS = 16

from demand_constants import DEMAND_CITIES

def celsius_to_f(c):
    return c * 9 / 5 + 32

def compute_tdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)

def fetch_city_temps(lat, lon, forecast_days=FORECAST_DAYS):
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_mean",
        "temperature_unit": "celsius",
        "forecast_days": forecast_days,
        "models": "ecmwf_ifs025",
        "timezone": "UTC",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        
        # Open-Meteo limits raw ECMWF member data due to licensing, but provides
        # the pre-calculated statistical ensemble mean variable freely.
        temps = daily.get("temperature_2m_mean", [])
            
        return {d: t for d, t in zip(dates, temps) if t is not None}
    except Exception as e:
        print(f"Failed to fetch {lat}, {lon}: {e}")
        return None

def fetch():
    now = datetime.datetime.now(datetime.UTC)
    
    test_lat, test_lon = DEMAND_CITIES[0][1], DEMAND_CITIES[0][2]
    test_temps = fetch_city_temps(test_lat, test_lon)
    
    if not test_temps:
        print("  [ERR] ECMWF ENS Open-Meteo fetch: API completely failed")
        return None
        
    start_date_str = sorted(test_temps.keys())[0]
    date_formatted = start_date_str.replace("-", "")
    
    # Open-Meteo Ensemble mirrors ECMWF's twice-a-day schedule
    if now.hour >= 19:
        cycle = "12"
    elif now.hour >= 7:
        cycle = "00"
    else:
        cycle = "12"
        # Since it's before 7am, the "start date" returned by API is still yesterday
        
    run_id = f"{date_formatted}_{cycle}"
    out_path = BASE_DIR / f"{run_id}_tdd.csv"
    
    if out_path.exists():
        print(f"[{run_id}Z] Already fetched fully. Skipping.")
        return run_id

    print(f"Trying ECMWF ENS: {run_id} (Open-Meteo API ecmwf_ifs04)")

    city_data = {}
    failed = 0
    for name, lat, lon, weight in DEMAND_CITIES:
        temps = fetch_city_temps(lat, lon)
        if temps:
            city_data[name] = (weight, temps)
        else:
            print(f"    [ERR] {name} failed")
            failed += 1

    if not city_data:
        print("  [ERR] ECMWF ENS Open-Meteo fetch: all cities failed")
        return None

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
            "tdd_gw":    round(compute_tdd(avg_f), 2),
            "model":     "ECMWF_ENS",
            "run_id":    run_id,
        })

    if not rows:
        print("  [ERR] ECMWF ENS: no dates computed")
        return None

    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    
    active = len(DEMAND_CITIES) - failed
    print(f"[OK] Success: {run_id} ECMWF_ENS ({len(rows)} days computed | {active}/17 cities)")
    return run_id

if __name__ == "__main__":
    fetch()
