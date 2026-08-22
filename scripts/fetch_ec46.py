"""
fetch_ec46.py

Fetches ECMWF EC46 sub-seasonal ensemble (51 members, 46-day horizon) 2m
temperature using the Open-Meteo Seasonal Forecast API (`ecmwf_ec46`).

Fills the weeks 3-6 gap beyond GEFS 35-day. Note: EC46 is ensemble-mean
guidance and is not bias-corrected — treat as anomaly signal, not absolute
temperature truth. Updated daily ~20:30 UTC.

City list expanded to 79 cities (see demand_constants.py).
Fetches all cities in a single batched API call via om_batch_fetch.
"""

import os
import requests
import datetime
import pandas as pd
from pathlib import Path

BASE_DIR = Path("data/ec46")
BASE_DIR.mkdir(parents=True, exist_ok=True)
BASE_TEMP_F = 65.0
FORECAST_DAYS = 46

from demand_constants import DEMAND_CITIES
from om_batch_fetch import fetch_all_cities_batch

OM_SEASONAL_ENDPOINT = "https://seasonal-api.open-meteo.com/v1/seasonal"


def celsius_to_f(c):
    return c * 9 / 5 + 32


def compute_hdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)


def compute_cdd(temp_f):
    return max(temp_f - BASE_TEMP_F, 0)


def fetch_run(date_str):
    run_id = f"{date_str}_00"
    out_path = BASE_DIR / f"{run_id}_tdd.csv"

    if out_path.exists():
        try:
            existing = pd.read_csv(out_path)
            if len(existing) >= 30:
                print(f"  [SKIP] EC46 run {run_id} already fetched ({len(existing)} days).")
                return True
            else:
                print(f"  [WARN] EC46 {run_id}: only {len(existing)} rows — removing partial file.")
                out_path.unlink()
        except Exception:
            out_path.unlink()

    print(f"Syncing EC46: {run_id} "
          f"(Open-Meteo seasonal ecmwf_ec46, {len(DEMAND_CITIES)}-city batch)")

    city_data = fetch_all_cities_batch(
        endpoint=OM_SEASONAL_ENDPOINT,
        model="ecmwf_ec46",
        forecast_days=FORECAST_DAYS,
    )

    if not city_data:
        print(f"  [ERR] No city data returned for {run_id}. Skipping.")
        return False

    # Weighted daily average across all cities that returned data.
    # om_batch_fetch drops null temps per city/date, so the horizon naturally
    # ends at EC46's last valid forecast day (trailing nulls are excluded).
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
                "model":     "EC46",
                "run_id":    run_id,
            })

    if rows:
        if len(rows) < 30:
            print(f"  [WARN] EC46 {run_id}: only {len(rows)} day(s) — file NOT written.")
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

        print(f"  [OK] {run_id} EC46: {len(rows)} days, "
              f"{len(city_data)}/{len(DEMAND_CITIES)} cities active.")
        return True

    print(f"  [ERR] No rows computed for {run_id}.")
    return False


def sync_all_ec46():
    print("\n--- ECMWF EC46 SUB-SEASONAL SYNC SERVICE ---")
    now = datetime.datetime.now(datetime.UTC)

    # EC46 publishes once daily ~20:30 UTC. Target today's data after 22 UTC,
    # otherwise re-check yesterday's run (SKIP guard prevents refetch churn).
    if now.hour >= 22:
        date_str = now.strftime("%Y%m%d")
    else:
        date_str = (now - datetime.timedelta(days=1)).strftime("%Y%m%d")

    fetch_run(date_str)


if __name__ == "__main__":
    sync_all_ec46()
