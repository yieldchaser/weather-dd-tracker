"""
fetch_live_grid.py

Uses gridstatus to query live ERCOT and PJM fuel mix grids.
Implements a local caching system strictly to avoid pinging historical endpoints
for 30 days of data on every daily GitHub Action cycle.
"""

import os
import sys
import time
import pandas as pd
import gridstatus
import datetime
import pytz
from pathlib import Path

# Caching locations
DATA_DIR = Path("data")
OUTPUT_DIR = Path("outputs")
CACHE_FILE = DATA_DIR / "grid_history_cache.csv"
OUTPUT_FILE = OUTPUT_DIR / "live_grid_generation.csv"

def get_ercot_daily_mix(dt):
    """Fetches ERCOT detailed fuel mix for a specific date and returns daily aversge MW"""
    
    try:
        iso = gridstatus.Ercot()
    except AttributeError:
        iso = gridstatus.ercot()
        
    for attempt in range(3):
        try:
            # get_fuel_mix_detailed is strictly required to get Gas out of ERCOT
            df = iso.get_fuel_mix_detailed(date=dt)
            if df.empty:
                 return None
                 
            # gridstatus returns 5 min intervals. Average them across the day.
            # Columns natively include: 'Natural Gas', 'Wind', 'Solar', 'Coal', etc
            cols_to_keep = ["Natural Gas", "Wind", "Solar", "Coal", "Nuclear"]
            means = {c: df[c].mean() if c in df.columns else 0.0 for c in cols_to_keep}
            means["iso"] = "ERCOT"
            means["date"] = dt.strftime("%Y-%m-%d")
            return means
        except Exception as e:
            print(f"  [WARN] ERCOT detailed fuel fetch failed for {dt.strftime('%m-%d')} (Attempt {attempt+1}/3): {e}")
            time.sleep(2)
            
    return None

def fetch_live_grid():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # We use US Central time as standard ISO alignment
    tz = pytz.timezone("US/Central")
    today = datetime.datetime.now(tz).date()
    
    print(f"--- Fetching Live ISO Grid Generation (ERCOT) ---")
    
    # 1. Load Cache
    if CACHE_FILE.exists():
        cache_df = pd.read_csv(CACHE_FILE)
        cache_df["date"] = pd.to_datetime(cache_df["date"]).dt.date
    else:
        cache_df = pd.DataFrame()
        
    dates_to_fetch = []
    
    # We need to maintain a 30-day rolling window in the cache
    start_date = today - datetime.timedelta(days=30)
    current_dt = start_date
    
    while current_dt <= today:
        if cache_df.empty or current_dt not in cache_df["date"].values:
            dates_to_fetch.append(current_dt)
        current_dt += datetime.timedelta(days=1)
        
    # 2. Fetch missing days (including today)
    new_rows = []
    if dates_to_fetch:
        print(f"  [INFO] Cache miss for {len(dates_to_fetch)} day(s). Fetching via API...")
        for dt in dates_to_fetch:
            print(f"    -> Querying {dt}...")
            ercot = get_ercot_daily_mix(dt)
            if ercot: new_rows.append(ercot)
            
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        new_df["date"] = pd.to_datetime(new_df["date"]).dt.date
        cache_df = pd.concat([cache_df, new_df], ignore_index=True).drop_duplicates(["iso", "date"], keep="last")
        
    # 3. Clean Cache (drop anything older than 30 days entirely)
    if not cache_df.empty and "date" in cache_df.columns:
        cache_df["date"] = pd.to_datetime(cache_df["date"])
        cache_df = cache_df[cache_df["date"].dt.date >= start_date]
        cache_df.sort_values(["iso", "date"], inplace=True)
        cache_df.to_csv(CACHE_FILE, index=False)
        
        # 4. Process Today's Output and calculate Anomaly
        out_rows = []
        today_str = pd.to_datetime(today).strftime("%Y-%m-%d")
        
        for iso_str in ["ERCOT"]:
            iso_data = cache_df[cache_df["iso"] == iso_str].copy()
            
            # We need historical days to compute the 30d baseline
            hist_wind = iso_data[iso_data["date"].dt.strftime("%Y-%m-%d") != today_str]["Wind"].mean()
            
            # Today's live snapshot
            today_data = iso_data[iso_data["date"].dt.strftime("%Y-%m-%d") == today_str]
            
            if not today_data.empty and pd.notna(hist_wind):
                row = today_data.iloc[0].to_dict()
                live_wind = row["Wind"]
                anomaly = live_wind - hist_wind
                
                # Formatting for UI
                row["natural_gas_mw"] = round(row["Natural Gas"])
                row["wind_mw"] = round(live_wind)
                row["solar_mw"] = round(row["Solar"])
                row["coal_mw"] = round(row["Coal"])
                row["wind_30d_avg_mw"] = round(hist_wind)
                row["wind_anomaly_mw"] = round(anomaly)
                
                # Simple heuristic: heavily negative anomaly means wind died -> burns more gas
                if anomaly < -1000:
                    impact = "BULLISH (Wind Drought)"
                elif anomaly > 1500:
                    impact = "BEARISH (Strong Wind)"
                else:
                    impact = "NEUTRAL"
                
                row["gas_burn_impact"] = impact
                
                # Formatting
                row["date"] = row["date"].strftime("%Y-%m-%d")
                
                # Drop the raw columns that we duplicated with better names
                for col in ["Natural Gas", "Wind", "Solar", "Coal", "Nuclear"]:
                    if col in row: del row[col]
                    
                out_rows.append(row)
                
        if out_rows:
            out_df = pd.DataFrame(out_rows)
            # Ensure column order
            cols = ["date", "iso", "natural_gas_mw", "wind_mw", "solar_mw", "coal_mw", "wind_30d_avg_mw", "wind_anomaly_mw", "gas_burn_impact"]
            out_df = out_df[[c for c in cols if c in out_df.columns]]
            out_df.to_csv(OUTPUT_FILE, index=False)
            print(f"\n  [OK] Saved Live Grid Generation -> {OUTPUT_FILE}")
            print(out_df.to_string(index=False))
        else:
            print("\n  [ERR] Failed to compute live grid snapshot. Incomplete data.")
            cols = ["date", "iso", "natural_gas_mw", "wind_mw", "solar_mw", "coal_mw", "wind_30d_avg_mw", "wind_anomaly_mw", "gas_burn_impact"]
            fallback_df = pd.DataFrame([{c: float('nan') for c in cols}])
            fallback_df["date"] = today_str
            fallback_df["iso"] = "ERCOT"
            fallback_df["gas_burn_impact"] = "NEUTRAL"
            fallback_df.to_csv(OUTPUT_FILE, index=False)
            print(f"  [WARN] Saved NaN Fallback -> {OUTPUT_FILE}")
            sys.exit(1)
    else:
        print("\n  [ERR] Cache is completely empty and APIs failed. Exiting gracefully.")
        cols = ["date", "iso", "natural_gas_mw", "wind_mw", "solar_mw", "coal_mw", "wind_30d_avg_mw", "wind_anomaly_mw", "gas_burn_impact"]
        fallback_df = pd.DataFrame([{c: float('nan') for c in cols}])
        fallback_df["date"] = pd.to_datetime(today).strftime("%Y-%m-%d")
        fallback_df["iso"] = "ERCOT"
        fallback_df["gas_burn_impact"] = "NEUTRAL"
        fallback_df.to_csv(OUTPUT_FILE, index=False)
        print(f"  [WARN] Saved NaN Fallback -> {OUTPUT_FILE}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_live_grid()
