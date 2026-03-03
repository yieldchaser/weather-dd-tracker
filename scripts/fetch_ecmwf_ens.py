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

def fetch_run(date_str, cycle):
    run_id = f"{date_str}_{cycle}"
    out_path = BASE_DIR / f"{run_id}_tdd.csv"
    
    if out_path.exists():
        print(f"  [SKIP] ECMWF ENS run {run_id}Z already fetched.")
        return True

    print(f"Syncing ECMWF ENS: {run_id} (Open-Meteo API ecmwf_ifs025)")

    city_data = {}
    failed = 0
    for name, lat, lon, weight in DEMAND_CITIES:
        # We fetch the OM ensemble mean for this city.
        # OM usually mirrors 00z and 12z cycles for ECMWF.
        temps = fetch_city_temps(lat, lon)
        if temps:
            city_data[name] = (weight, temps)
        else:
            failed += 1

    if not city_data:
        return False

    all_dates = sorted(set(d for _, temps in city_data.values() for d in temps))
    rows = []
    for dt_str in all_dates:
        total_w, weighted_temp = 0.0, 0.0
        for name, (weight, temps) in city_data.items():
            if dt_str in temps:
                weighted_temp += weight * temps[dt_str]
                total_w += weight
        if total_w > 0:
            avg_f = celsius_to_f(weighted_temp / total_w)
            rows.append({
                "date": dt_str, "mean_temp": round(avg_f, 2), "tdd": round(compute_tdd(avg_f), 2),
                "tdd_gw": round(compute_tdd(avg_f), 2), "model": "ECMWF_ENS", "run_id": run_id,
            })

    if rows:
        pd.DataFrame(rows).to_csv(out_path, index=False)
        print(f"  [OK] Success: {run_id} ECMWF_ENS ({len(rows)} days)")
        return True
    return False

def sync_all_ecmwf_ens():
    print("\n--- ECMWF ENS SYNC SERVICE ---")
    now = datetime.datetime.now(datetime.UTC)
    for day_offset in range(-1, 1):
        date_str = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        for cycle in ["00", "12"]:
            # Logic: If it's today and before 08:30 UTC, 00z probably isn't ready on OM.
            # If it's before 20:30 UTC, 12z isn't ready.
            if day_offset == 0:
                if cycle == "00" and now.hour < 8: continue
                if cycle == "12" and now.hour < 20: continue
            fetch_run(date_str, cycle)

if __name__ == "__main__":
    sync_all_ecmwf_ens()
