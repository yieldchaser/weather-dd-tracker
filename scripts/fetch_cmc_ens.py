"""
fetch_cmc_ens.py

Fetches CMC ENS (Canadian GEM Global Ensemble) 2m temperature using the Open-Meteo API.
Uses `gem_global_ensemble` parameter.

City list expanded to 79 cities (see demand_constants.py).
Fetches all cities in a single batched API call via om_batch_fetch.
"""

import os
import requests
import datetime
import pandas as pd
from pathlib import Path

BASE_DIR = Path("data/cmc_ens")
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
        try:
            existing = pd.read_csv(out_path)
            if len(existing) >= 8:
                print(f"  [SKIP] CMC ENS run {run_id}Z already fetched ({len(existing)} days).")
                return True
            else:
                print(f"  [WARN] CMC ENS {run_id}Z: only {len(existing)} rows — removing partial file.")
                out_path.unlink()
        except Exception:
            out_path.unlink()

    print(f"Syncing CMC ENS: {run_id} "
          f"(Open-Meteo gem_global_ensemble, {len(DEMAND_CITIES)}-city batch)")

    city_data = fetch_all_cities_batch(
        endpoint=OM_ENSEMBLE_ENDPOINT,
        model="gem_global_ensemble",
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
                "date":      dt_str,
                "mean_temp": round(avg_f, 2),
                "tdd":       round(compute_tdd(avg_f), 2),
                "tdd_gw":    round(compute_tdd(avg_f), 2),
                "model":     "CMC_ENS",
                "run_id":    run_id,
            })

    if rows:
        if len(rows) < 8:
            print(f"  [WARN] CMC ENS {run_id}: only {len(rows)} day(s) — file NOT written.")
            return False
        pd.DataFrame(rows).to_csv(out_path, index=False)
        print(f"  [OK] {run_id} CMC_ENS: {len(rows)} days, "
              f"{len(city_data)}/{len(DEMAND_CITIES)} cities active.")
        return True

    print(f"  [ERR] No rows computed for {run_id}.")
    return False


def sync_all_cmc():
    print("\n--- CMC ENS SYNC SERVICE ---")
    now = datetime.datetime.now(datetime.UTC)
    for day_offset in range(-1, 1):
        date_str = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        for cycle in ["00", "12"]:
            # Open-Meteo mirrors CMC 00z at ~07:00 UTC and 12z at ~19:00 UTC.
            if day_offset == 0:
                if cycle == "00" and now.hour < 7:
                    continue
                if cycle == "12" and now.hour < 19:
                    continue
            fetch_run(date_str, cycle)


if __name__ == "__main__":
    sync_all_cmc()
