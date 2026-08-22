"""
fetch_aifs_ens.py

Fetches ECMWF AIFS ENS v2 (51-member AI ensemble) 2m temperature using the
Open-Meteo Ensemble API (`ecmwf_aifs025_ensemble`).

AIFS ENS v2 went operational 12 May 2026 alongside IFS Cycle 50r1.
Complements the deterministic AIFS Single already ingested as ECMWF_AIFS.

City list expanded to 79 cities (see demand_constants.py).
Fetches all cities in a single batched API call via om_batch_fetch.
"""

import os
import requests
import datetime
import pandas as pd
from pathlib import Path

BASE_DIR = Path("data/aifs_ens")
BASE_DIR.mkdir(parents=True, exist_ok=True)
BASE_TEMP_F = 65.0
FORECAST_DAYS = 15

from demand_constants import DEMAND_CITIES
from om_batch_fetch import fetch_all_cities_batch

OM_ENSEMBLE_ENDPOINT = "https://ensemble-api.open-meteo.com/v1/ensemble"


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
                print(f"  [SKIP] AIFS_ENS run {run_id}Z already fetched ({len(existing)} days).")
                return True
            else:
                print(f"  [WARN] AIFS_ENS {run_id}Z: only {len(existing)} rows — removing partial file.")
                out_path.unlink()
        except Exception:
            out_path.unlink()

    print(f"Syncing AIFS_ENS: {run_id} "
          f"(Open-Meteo ecmwf_aifs025_ensemble, {len(DEMAND_CITIES)}-city batch)")

    city_data = fetch_all_cities_batch(
        endpoint=OM_ENSEMBLE_ENDPOINT,
        model="ecmwf_aifs025_ensemble",
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
                "model":     "AIFS_ENS",
                "run_id":    run_id,
            })

    if rows:
        if len(rows) < 8:
            print(f"  [WARN] AIFS_ENS {run_id}: only {len(rows)} day(s) — file NOT written.")
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

        print(f"  [OK] {run_id} AIFS_ENS: {len(rows)} days, "
              f"{len(city_data)}/{len(DEMAND_CITIES)} cities active.")
        return True

    print(f"  [ERR] No rows computed for {run_id}.")
    return False


def sync_all_aifs_ens():
    print("\n--- ECMWF AIFS ENS SYNC SERVICE ---")
    now = datetime.datetime.now(datetime.UTC)

    # Determine the single active cycle based on current UTC time.
    # ECMWF open-data dissemination lands ~4-6h after init.
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
    sync_all_aifs_ens()
