"""
fetch_open_meteo_ai.py

Fetches NOAA AIGFS and HGEFS AI weather model forecasts from the Open-Meteo Single Runs API.
Supports real-time run sync and backfill mode to prevent cold starts.
Runs entirely in the cloud (no local compute/VRAM needed).
"""

import os
import sys
import argparse
import datetime
import pandas as pd
from pathlib import Path

# Add scripts directory to path to import shared modules
sys.path.insert(0, str(Path(__file__).parent))
from demand_constants import DEMAND_CITIES, TOTAL_WEIGHT
from om_batch_fetch import fetch_all_cities_batch

# Open-Meteo Single Runs API Endpoint
SINGLE_RUNS_ENDPOINT = "https://single-runs-api.open-meteo.com/v1/forecast"

# Config mapping for the two NOAA AI models
MODELS_CONFIG = {
    "AIGFS": {
        "om_name": "ncep_aigfs025",
        "forecast_days": 16,
        "cycles": ["00", "06", "12", "18"],
        "out_dir": Path("data/aigfs")
    },
    "HGEFS": {
        "om_name": "ncep_hgefs025_ensemble_mean",
        "forecast_days": 10,
        "cycles": ["00", "06", "12", "18"],
        "out_dir": Path("data/hgefs")
    }
}

def celsius_to_f(c):
    return c * 9 / 5 + 32

def hdd(temp_f):
    return max(65.0 - temp_f, 0)

def cdd(temp_f):
    return max(temp_f - 65.0, 0)

def tdd(temp_f):
    return hdd(temp_f) + cdd(temp_f)

def fetch_run(model_key, date_str, cycle):
    """
    Fetch a specific forecast run for the given model, date, and cycle.
    """
    config = MODELS_CONFIG[model_key]
    run_id = f"{date_str}_{cycle}"
    out_path = config["out_dir"] / f"{run_id}_tdd.csv"

    # Skip if already downloaded and valid
    if out_path.exists():
        try:
            df_existing = pd.read_csv(out_path)
            # Expect at least 3 days of valid data (to handle short forecasts or edge cases)
            if len(df_existing) >= 3:
                print(f"  [SKIP] {model_key} run {run_id} already exists.")
                return True
        except Exception:
            pass

    # Build ISO timestamp for the run (e.g. 2026-06-05T00:00)
    run_time_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T{cycle}:00"
    print(f"  Fetching {model_key} run {run_id} (initialized: {run_time_str} UTC)...")

    city_data = fetch_all_cities_batch(
        endpoint=SINGLE_RUNS_ENDPOINT,
        model=config["om_name"],
        forecast_days=config["forecast_days"],
        extra_params={"run": run_time_str}
    )

    if not city_data:
        print(f"  [WARN] No data returned for {model_key} run {run_id} (not published yet or API error).")
        return False

    # Aggregate weighted average across demand cities
    all_dates = sorted(set(d for _, temps in city_data.values() for d in temps))
    rows = []

    for dt_str in all_dates:
        total_w = 0.0
        weighted_temp = 0.0
        for name, (weight, temps) in city_data.items():
            if dt_str in temps:
                weighted_temp += weight * temps[dt_str]
                total_w += weight

        if total_w > 0:
            avg_c = weighted_temp / total_w
            avg_f = celsius_to_f(avg_c)
            hdd_val = hdd(avg_f)
            cdd_val = cdd(avg_f)
            tdd_val = hdd_val + cdd_val
            
            rows.append({
                "date": dt_str,
                "mean_temp": round(avg_f, 2),
                "hdd": round(hdd_val, 2),
                "cdd": round(cdd_val, 2),
                "tdd": round(tdd_val, 2),
                "mean_temp_gw": round(avg_f, 2),
                "hdd_gw": round(hdd_val, 2),
                "cdd_gw": round(cdd_val, 2),
                "tdd_gw": round(tdd_val, 2),
                "model": model_key,
                "run_id": run_id
            })

    if not rows:
        print(f"  [WARN] Empty rows calculated for {model_key} run {run_id}.")
        return False

    # Save output to CSV
    config["out_dir"].mkdir(parents=True, exist_ok=True)
    df_out = pd.DataFrame(rows)
    df_out.to_csv(out_path, index=False)
    print(f"  [OK] Saved {model_key} run {run_id} ({len(df_out)} days).")
    return True

def sync_runs(backfill_days=0):
    """
    Sync recent runs or execute historical backfill.
    """
    now = datetime.datetime.now(datetime.UTC)
    
    # If N is specified, backfill that many days, otherwise default to 3 days lookback
    lookback = backfill_days if backfill_days > 0 else 3
    print(f"Syncing runs with a {lookback}-day lookback window...")

    for day_offset in range(-lookback, 1):
        date_str = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        for model_key, config in MODELS_CONFIG.items():
            for cycle in config["cycles"]:
                # If checking today, don't request runs in the future
                if day_offset == 0:
                    cycle_hour = int(cycle)
                    # Models are typically published 3-6 hours after the cycle start
                    if now.hour < cycle_hour + 3:
                        continue
                
                try:
                    fetch_run(model_key, date_str, cycle)
                except Exception as e:
                    print(f"  [ERR] Failed to sync {model_key} run {date_str}_{cycle}: {e}")

def main():
    parser = argparse.ArgumentParser(description="NOAA AI Models Fetcher")
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=0,
        help="Number of days to backfill historically. If 0 and data directory is empty, defaults to 15."
    )
    args = parser.parse_args()

    # Create directories
    for config in MODELS_CONFIG.values():
        config["out_dir"].mkdir(parents=True, exist_ok=True)

    backfill = args.backfill_days
    
    # Check if directories are empty. If so, default backfill to 15 days to avoid cold start
    if backfill == 0:
        for model_key, config in MODELS_CONFIG.items():
            existing_files = list(config["out_dir"].glob("*_tdd.csv"))
            if not existing_files:
                print(f"No existing data files found for {model_key}. Triggering automatic 15-day backfill to prevent cold start.")
                backfill = max(backfill, 15)

    sync_runs(backfill_days=backfill)

if __name__ == "__main__":
    main()
