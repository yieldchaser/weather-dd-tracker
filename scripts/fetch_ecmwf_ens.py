"""
fetch_ecmwf_ens.py

Fetches ECMWF ENS (Ensemble) 2m temperature using the Open-Meteo API.
Uses `ecmwf_ifs025` (the 0.25-degree ensemble mean).

COMPATIBILITY: Insulated from direct ECMWF API changes (uses Open-Meteo proxy).
  Open-Meteo will auto-handle 50r1 / AIFS v2 backend migrations.
  No code changes required for this script; monitoring only.

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
from om_batch_fetch import fetch_all_cities_batch, is_duplicate_run

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
            h = compute_hdd(avg_f)
            c = compute_cdd(avg_f)
            t = h + c
            rows.append({
                "date":     dt_str,
                "mean_temp": round(avg_f, 2),
                "hdd":      round(h, 2),
                "cdd":      round(c, 2),
                "tdd":      round(t, 2),
                "mean_temp_gw": round(avg_f, 2),
                "hdd_gw":   round(h, 2),
                "cdd_gw":   round(c, 2),
                "tdd_gw":   round(t, 2),
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
        if is_duplicate_run(BASE_DIR, run_id, city_data):
            return False
        import om_batch_fetch as _omb
        for _r in rows:
            _r["ens_members"] = _omb.LAST_MEMBER_COUNT
        _q = _omb.member_dd_quantiles(city_data)
        for _r in rows:
            _qq = _q.get(_r["date"])
            if _qq:
                _r["hdd_gw_p10"], _r["hdd_gw_p90"] = _qq["hdd_p10"], _qq["hdd_p90"]
                _r["cdd_gw_p10"], _r["cdd_gw_p90"] = _qq["cdd_p10"], _qq["cdd_p90"]
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
            
        print(f"  [OK] {run_id} ECMWF_ENS: {len(rows)} days, "
              f"{len(city_data)}/{len(DEMAND_CITIES)} cities active.")
        return True

    print(f"  [ERR] No rows computed for {run_id}.")
    return False


def sync_all_ecmwf_ens():
    """Fetch the single active cycle, as the other Open-Meteo fetchers do.

    Open-Meteo always serves its newest run. The old loop also tried
    yesterday's cycles; per-run files are not kept between CI runs, so the
    newest forecast was written under yesterday's 00z label first and the true
    cycle labels were then skipped as duplicates of it."""
    print("\n--- ECMWF ENS SYNC SERVICE ---")
    now = datetime.datetime.now(datetime.UTC)
    # Open-Meteo mirrors 00z at ~08:30 UTC and 12z at ~20:30 UTC.
    if now.hour >= 21:
        date_str, cycle = now.strftime("%Y%m%d"), "12"
    elif now.hour >= 9:
        date_str, cycle = now.strftime("%Y%m%d"), "00"
    else:
        date_str, cycle = (now - datetime.timedelta(days=1)).strftime("%Y%m%d"), "12"
    # Exact label from Open-Meteo's own run metadata when available.
    from om_batch_fetch import om_latest_run
    _rid = om_latest_run("ecmwf_ifs025")
    if _rid:
        date_str, cycle = _rid.split("_")
    # 06z/18z on Open-Meteo run only 144 h and reuse the previous 00z/12z run
    # for days 7-15; store 00z and 12z only.
    if cycle in ("06", "18"):
        print(f"  [SKIP] Open-Meteo serves the {date_str}_{cycle} short run; keeping the 00z/12z runs only.")
        return
    fetch_run(date_str, cycle)


if __name__ == "__main__":
    sync_all_ecmwf_ens()
