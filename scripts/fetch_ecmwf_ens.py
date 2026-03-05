"""
fetch_ecmwf_ens.py

Fetches ECMWF ENS (Ensemble) 2m temperature using the Open-Meteo API.
Uses `ecmwf_ifs025` (the 0.25-degree ensemble mean).

City list expanded to 79 cities (see demand_constants.py).
Fetches all cities in a single batched API call via om_batch_fetch
instead of N serial requests — reduces API calls by ~97%.
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
from om_batch_fetch import fetch_all_cities_batch

OM_ENSEMBLE_ENDPOINT = "https://ensemble-api.open-meteo.com/v1/ensemble"


def celsius_to_f(c):
    return c * 9 / 5 + 32


def compute_tdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)


def fetch_run(date_str, cycle):
    run_id = f"{date_str}_{cycle}"
    out_path = BASE_DIR / f"{run_id}_tdd.csv"

    if out_path.exists():
        # Validate the existing file isn't a partial write from a prior failed run
        try:
            existing = pd.read_csv(out_path)
            if len(existing) >= 8:  # expect at least 8 forecast days
                print(f"  [SKIP] ECMWF ENS run {run_id}Z already fetched ({len(existing)} days).")
                return True
            else:
                print(f"  [WARN] ECMWF ENS {run_id}Z existing file has only {len(existing)} rows "
                      f"— likely a partial write. Removing and re-fetching.")
                out_path.unlink()
        except Exception:
            out_path.unlink()  # corrupted CSV → remove and retry

    print(f"Syncing ECMWF ENS: {run_id} "
          f"(Open-Meteo ecmwf_ifs025, {len(DEMAND_CITIES)}-city batch)")

    city_data = fetch_all_cities_batch(
        endpoint=OM_ENSEMBLE_ENDPOINT,
        model="ecmwf_ifs025",
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
            rows.append({
                "date":     dt_str,
                "mean_temp": round(avg_f, 2),
                "tdd":      round(compute_tdd(avg_f), 2),
                "tdd_gw":   round(compute_tdd(avg_f), 2),
                "model":    "ECMWF_ENS",
                "run_id":   run_id,
            })

    if rows:
        # Guard: require at least 8 forecast days before treating as a valid fetch.
        # A run with 1-2 days is a sign that the batch was severely degraded
        # (only last few days of a prior run's data came back).
        if len(rows) < 8:
            print(f"  [WARN] ECMWF ENS {run_id}: only {len(rows)} day(s) computed — "
                  f"too few to be a valid forecast. File NOT written.")
            return False
        pd.DataFrame(rows).to_csv(out_path, index=False)
        print(f"  [OK] {run_id} ECMWF_ENS: {len(rows)} days, "
              f"{len(city_data)}/{len(DEMAND_CITIES)} cities active.")
        return True

    print(f"  [ERR] No rows computed for {run_id}.")
    return False


def sync_all_ecmwf_ens():
    print("\n--- ECMWF ENS SYNC SERVICE ---")
    now = datetime.datetime.now(datetime.UTC)
    for day_offset in range(-1, 1):
        date_str = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        for cycle in ["00", "12"]:
            # Open-Meteo mirrors 00z at ~08:30 UTC and 12z at ~20:30 UTC.
            if day_offset == 0:
                if cycle == "00" and now.hour < 8:
                    continue
                if cycle == "12" and now.hour < 20:
                    continue
            fetch_run(date_str, cycle)


if __name__ == "__main__":
    sync_all_ecmwf_ens()
