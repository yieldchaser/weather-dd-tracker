"""
fetch_wn2.py

Fetches Google DeepMind WeatherNext 2 (64-member AI ensemble) ensemble-mean
2m temperature using the Open-Meteo Ensemble API (`google_weathernext2_ensemble_mean`).

WeatherNext 2 is the successor to the deprecated WeatherNext Gen/Graph (GenCast)
datasets. Open-Meteo mirrors the 00Z and 12Z runs only (the 06Z/18Z runs do not
arrive in time for their update schedule). Native horizon: 15 days at 6-hourly
resolution, served as daily aggregates.

City list expanded to 79 cities (see demand_constants.py).
Fetches all cities in a single batched API call via om_batch_fetch.
"""

import os
import requests
import datetime
import pandas as pd
from pathlib import Path

BASE_DIR = Path("data/google_wn2")
BASE_DIR.mkdir(parents=True, exist_ok=True)
BASE_TEMP_F = 65.0
FORECAST_DAYS = 15

from demand_constants import DEMAND_CITIES
from om_batch_fetch import fetch_all_cities_batch

OM_ENSEMBLE_ENDPOINT = "https://ensemble-api.open-meteo.com/v1/ensemble"

# Google disseminates each run ~6h50 after init; Open-Meteo ingests shortly after.
# 00Z run is safely queryable from ~08 UTC, 12Z run from ~20 UTC.


def celsius_to_f(c):
    return c * 9 / 5 + 32


def compute_hdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)


def compute_cdd(temp_f):
    return max(temp_f - BASE_TEMP_F, 0)


def fetch_run(date_str, cycle):
    run_id = f"{date_str}_{cycle}"
    out_path = BASE_DIR / f"{run_id}_tdd.csv"

    if out_path.exists():
        try:
            existing = pd.read_csv(out_path)
            if len(existing) >= 8:
                print(f"  [SKIP] GOOGLE_WN2 run {run_id}Z already fetched ({len(existing)} days).")
                return True
            else:
                print(f"  [WARN] GOOGLE_WN2 {run_id}Z: only {len(existing)} rows — removing partial file.")
                out_path.unlink()
        except Exception:
            out_path.unlink()

    print(f"Syncing GOOGLE_WN2: {run_id} "
          f"(Open-Meteo google_weathernext2_ensemble_mean, {len(DEMAND_CITIES)}-city batch)")

    city_data = fetch_all_cities_batch(
        endpoint=OM_ENSEMBLE_ENDPOINT,
        model="google_weathernext2_ensemble_mean",
        forecast_days=FORECAST_DAYS,
    )

    if not city_data:
        print(f"  [ERR] No city data returned for {run_id}. Skipping.")
        return False

    # Weighted daily average across all cities that returned data
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
            h = compute_hdd(avg_f)
            c = compute_cdd(avg_f)
            t = h + c
            rows.append({
                "date":      dt_str,
                "mean_temp": round(avg_f, 2),
                "hdd":       round(h, 2),
                "cdd":       round(c, 2),
                "tdd":       round(t, 2),
                "mean_temp_gw": round(avg_f, 2),
                "hdd_gw":    round(h, 2),
                "cdd_gw":    round(c, 2),
                "tdd_gw":    round(t, 2),
                "model":     "GOOGLE_WN2",
                "run_id":    run_id,
            })

    if rows:
        if len(rows) < 8:
            print(f"  [WARN] GOOGLE_WN2 {run_id}: only {len(rows)} day(s) — file NOT written.")
            return False
        pd.DataFrame(rows).to_csv(out_path, index=False)

        # Save raw city data to json for map generation
        city_dir = BASE_DIR / "cities"
        city_dir.mkdir(parents=True, exist_ok=True)
        city_json_path = city_dir / f"{run_id}_cities.json"
        import json
        city_temps_f = {}
        for name, (weight, temps) in city_data.items():
            city_temps_f[name] = {d: round(celsius_to_f(t), 2) for d, t in temps.items()}
        with open(city_json_path, "w") as f:
            json.dump(city_temps_f, f)

        print(f"  [OK] {run_id} GOOGLE_WN2: {len(rows)} days, "
              f"{len(city_data)}/{len(DEMAND_CITIES)} cities active.")
        return True

    print(f"  [ERR] No rows computed for {run_id}.")
    return False


def sync_all_wn2():
    print("\n--- GOOGLE WN2 SYNC SERVICE ---")
    now = datetime.datetime.now(datetime.UTC)

    # Determine the single active cycle based on current UTC time
    if now.hour >= 20:
        date_str = now.strftime("%Y%m%d")
        cycle = "12"
    elif now.hour >= 8:
        date_str = now.strftime("%Y%m%d")
        cycle = "00"
    else:
        date_str = (now - datetime.timedelta(days=1)).strftime("%Y%m%d")
        cycle = "12"

    fetch_run(date_str, cycle)


if __name__ == "__main__":
    sync_all_wn2()
