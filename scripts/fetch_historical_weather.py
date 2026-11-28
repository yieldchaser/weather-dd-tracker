"""
fetch_historical_weather.py

Fetches historical ERA5 weather data from Open-Meteo Archive API
to construct a daily Total Degree Day (TDD) index for the US
Gas-Weighted demand cities.

Outputs:
- outputs/historical_degree_days.csv

Incremental: the full 1991-to-date download (78 cities x 35 years) used to run
on every hourly poller pass, in parallel with the model fetchers that share
the same Open-Meteo limit, and a partly failed pass could rewrite the whole
history from 60% of the cities (2026-11-28: 916 s, then a crash). Now:
  * the existing file is kept and only the last REFRESH_DAYS are re-fetched
    (recent days settle from analysis into ERA5) and merged in
  * at most every MIN_INTERVAL_H hours (state in outputs/health/historical_weather.json)
  * nothing is written unless at least MIN_COVERAGE of the city weight returned
  * full rebuild only when the file is missing or too short
"""

import datetime
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from demand_constants import DEMAND_CITIES, TOTAL_WEIGHT
from om_batch_fetch import fetch_era5_cities_batch

import json
import sys

URL = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "1991-01-01"
REFRESH_DAYS = 60
MIN_INTERVAL_H = 6
MIN_COVERAGE = 0.97
STATE = Path("outputs/health/historical_weather.json")

def celsius_to_f(c):
    return c * 9 / 5 + 32

def main():
    print("\n--- Fetching Historical Weather (Open-Meteo ERA5) ---")
    
    # We fetch up to yesterday to ensure completeness of archive
    END_DATE = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    output_path = Path("outputs/historical_degree_days.csv")
    
    old = None
    if output_path.exists():
        try:
            old = pd.read_csv(output_path)
        except Exception:
            old = None
    full = old is None or len(old) < 3650
    now = datetime.datetime.now(datetime.timezone.utc)
    if not full and "--force" not in sys.argv:
        try:
            last = datetime.datetime.fromisoformat(json.loads(STATE.read_text())["last_ok"])
            if (now - last).total_seconds() < MIN_INTERVAL_H * 3600:
                print(f"  [SKIP] refreshed {(now - last).total_seconds() / 3600:.1f} h ago (every {MIN_INTERVAL_H} h)")
                return
        except Exception:
            pass
    start = START_DATE if full else (now - datetime.timedelta(days=REFRESH_DAYS)).strftime("%Y-%m-%d")

    print(f"  Fetching ERA5 for {len(DEMAND_CITIES)} cities ({start} to {END_DATE}, {'full rebuild' if full else 'recent refresh'})...")
    city_data_raw = fetch_era5_cities_batch(
        endpoint=URL,
        start_date=start,
        end_date=END_DATE,
        variables=["temperature_2m_mean"],
    )

    got_w = sum(w for n, _, _, w in DEMAND_CITIES if n in (city_data_raw or {}))
    if not city_data_raw or got_w < MIN_COVERAGE * TOTAL_WEIGHT:
        # a partial city set would bias the weighted series; keep the existing file
        print(f"  [ERR] ERA5 returned {got_w:.1f}/{TOTAL_WEIGHT:.1f} of the city weight; existing history kept.")
        sys.exit(1)

    # Convert from {city_name: (weight, {date_str: temp_value})} to dataframes
    city_data = {}
    for name, (weight, temps_dict) in city_data_raw.items():
        dates = sorted(temps_dict.keys())
        temps = [temps_dict[d] for d in dates]
        temps_f = [celsius_to_f(t) if t is not None else np.nan for t in temps]
        df = pd.DataFrame({"date": pd.to_datetime(dates), f"temp_{name}": temps_f})
        city_data[name] = df
        
    if not city_data:
        print("  [ERR] All ERA5 fetches failed.")
        return
        
    # Merge all city dataframes
    print("  Calculating Gas Weights...")
    all_dates = sorted({d for df in city_data.values() for d in df["date"]})
    master_df = pd.DataFrame({"date": pd.to_datetime(all_dates)})
    
    for name, df in city_data.items():
        master_df = master_df.merge(df, on="date", how="left")
        
    # Calculate Simple Average (mean of 17 cities)
    temp_cols = [f"temp_{n}" for n, _, _, _ in DEMAND_CITIES if n in city_data]
    master_df["mean_temp_simple"] = master_df[temp_cols].mean(axis=1)
    
    # Calculate Gas-Weighted Average
    gw_sum = np.zeros(len(master_df))
    w_sum_arr = np.zeros(len(master_df))
    for name, _, _, weight in DEMAND_CITIES:
        if name in city_data:
            col = master_df[f"temp_{name}"]
            valid = col.notna()
            gw_sum += np.where(valid, col * weight, 0.0)
            w_sum_arr += np.where(valid, weight, 0.0)
            
    master_df["mean_temp_gw"] = np.where(w_sum_arr > 0, gw_sum / w_sum_arr, np.nan)
    
    # Calculate Daily HDDs and CDDs
    base = 65.0
    master_df["hdd_simple"] = np.maximum(base - master_df["mean_temp_simple"], 0).round(2)
    master_df["cdd_simple"] = np.maximum(master_df["mean_temp_simple"] - base, 0).round(2)
    master_df["hdd_gw"] = np.maximum(base - master_df["mean_temp_gw"], 0).round(2)
    master_df["cdd_gw"] = np.maximum(master_df["mean_temp_gw"] - base, 0).round(2)
    
    master_df["tdd_gw"] = (master_df["hdd_gw"] + master_df["cdd_gw"]).round(2)
    master_df["mean_temp_simple"] = master_df["mean_temp_simple"].round(2)
    master_df["mean_temp_gw"] = master_df["mean_temp_gw"].round(2)
    
    # Only keep the final summary columns
    final_cols = ["date", "mean_temp_simple", "hdd_simple", "cdd_simple", "mean_temp_gw", "hdd_gw", "cdd_gw", "tdd_gw"]
    final_df = master_df[final_cols].copy()
    
    # Convert date back to string format for simple CSV usage
    final_df["date"] = final_df["date"].dt.strftime('%Y-%m-%d')
    
    final_df = final_df.dropna(subset=["mean_temp_gw"])
    if not full:
        # replace the refreshed window, keep everything older untouched
        kept = old[old["date"] < final_df["date"].min()]
        final_df = pd.concat([kept, final_df], ignore_index=True)
    final_df = final_df.drop_duplicates("date", keep="last").sort_values("date")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(output_path, index=False)
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps({"last_ok": now.isoformat(), "mode": "full" if full else "refresh",
                                 "last_date": final_df["date"].max(), "rows": int(len(final_df))}))

    print(f"  [OK] Saved {len(final_df)} days of historical data to {output_path} (last {final_df['date'].max()})")

if __name__ == "__main__":
    main()
