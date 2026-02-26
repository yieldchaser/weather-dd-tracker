"""
build_historical_normals.py

Purpose:
- Dynamically queries Open-Meteo ERA5 Reanalysis API to build high-accuracy
  30-Year (1991-2020) and 10-Year (2016-2025) historical normals.
- Uses the 17 "Demand Cities" representing US Henry Hub gas consumption.
- Generates both simple (unweighted) and gas-weighted (population/demand weighted)
  curves for all 365 days of the year (plus Feb 29).

Outputs:
- data/normals/us_daily_normals.csv
- data/normals/us_gas_weighted_normals.csv
"""

import datetime
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from demand_constants import DEMAND_CITIES, TOTAL_WEIGHT

URL = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "1991-01-01"
END_DATE = "2025-12-31"

def celsius_to_f(c):
    return c * 9 / 5 + 32

def run():
    print(f"\n--- Synthesizing 10-Year & 30-Year Normals from ERA5 ---")
    
    lats = [c[1] for c in DEMAND_CITIES]
    lons = [c[2] for c in DEMAND_CITIES]
    
    params = {
        "latitude": ",".join(map(str, lats)),
        "longitude": ",".join(map(str, lons)),
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": "temperature_2m_mean",
        "timezone": "UTC"
    }
    
    print(f"  Fetching ERA5 for {len(DEMAND_CITIES)} cities ({START_DATE} to {END_DATE})...")
    r = requests.get(URL, params=params, timeout=60)
    if r.status_code != 200:
        print(f"  [ERR] Failed to fetch: {r.status_code}")
        print(r.text)
        return
        
    responses = r.json()
    if isinstance(responses, dict):
        responses = [responses]
        
    city_data = {}
    for i, res in enumerate(responses):
        name = DEMAND_CITIES[i][0]
        data = res.get("daily", {})
        times = data.get("time", [])
        temps = data.get("temperature_2m_mean", [])
        temps_f = [celsius_to_f(t) if t is not None else np.nan for t in temps]
        df = pd.DataFrame({"date": pd.to_datetime(times), f"temp_{name}": temps_f})
        city_data[name] = df
        
    if not city_data:
        print("  [ERR] All ERA5 fetches failed.")
        return
        
    # Merge all city dataframes
    print("\n  Merging 12,000+ days and calculating Gas Weights...")
    master_df = city_data[DEMAND_CITIES[0][0]][["date"]].copy()
    
    for name, df in city_data.items():
        master_df = master_df.merge(df, on="date", how="left")
        
    # Calculate Simple Average (mean of 17 cities)
    temp_cols = [f"temp_{n}" for n, _, _, _ in DEMAND_CITIES if n in city_data]
    master_df["mean_temp_simple"] = master_df[temp_cols].mean(axis=1)
    
    # Calculate Gas-Weighted Average
    gw_sum = np.zeros(len(master_df))
    w_sum = 0
    for name, _, _, weight in DEMAND_CITIES:
        if name in city_data:
            gw_sum += master_df[f"temp_{name}"].fillna(0) * weight
            w_sum += weight
            
    master_df["mean_temp_gw"] = gw_sum / w_sum
    
    # Calculate Daily HDDs and CDDs
    base = 65.0
    master_df["hdd_simple"] = np.maximum(base - master_df["mean_temp_simple"], 0)
    master_df["cdd_simple"] = np.maximum(master_df["mean_temp_simple"] - base, 0)
    master_df["hdd_gw"] = np.maximum(base - master_df["mean_temp_gw"], 0)
    master_df["cdd_gw"] = np.maximum(master_df["mean_temp_gw"] - base, 0)
    
    # Feature engineering for grouping
    master_df["year"] = master_df["date"].dt.year
    master_df["month"] = master_df["date"].dt.month
    master_df["day"] = master_df["date"].dt.day
    
    # Separate the 30-Year Epoch and 10-Year Epoch
    df_30 = master_df[(master_df["year"] >= 1991) & (master_df["year"] <= 2020)]
    df_10 = master_df[(master_df["year"] >= 2016) & (master_df["year"] <= 2025)]
    
    print("  Aggregating 30-Year (1991-2020) grouping...")
    grp_30 = df_30.groupby(["month", "day"]).agg({
        "mean_temp_simple": "mean",
        "hdd_simple": "mean",
        "cdd_simple": "mean",
        "mean_temp_gw": "mean",
        "hdd_gw": "mean",
        "cdd_gw": "mean"
    }).reset_index().round(1)
    
    print("  Aggregating 10-Year (2016-2025) grouping...")
    grp_10 = df_10.groupby(["month", "day"]).agg({
        "mean_temp_simple": "mean",
        "hdd_simple": "mean",
        "cdd_simple": "mean",
        "mean_temp_gw": "mean",
        "hdd_gw": "mean",
        "cdd_gw": "mean"
    }).reset_index().round(1)
    
    # Rename for clarity
    grp_30.columns = ["month", "day", "mean_temp_f", "hdd_normal", "cdd_normal", "mean_temp_gw", "hdd_normal_gw", "cdd_normal_gw"]
    grp_10.columns = ["month", "day", "mean_temp_f_10yr", "hdd_normal_10yr", "cdd_normal_10yr", "mean_temp_gw_10yr", "hdd_normal_gw_10yr", "cdd_normal_gw_10yr"]
    
    # Merge both
    final_df = grp_30.merge(grp_10, on=["month", "day"], how="left")
    
    # Split into the two files expected by the user architecture
    # 1. us_daily_normals.csv (Simple only, keeping backwards compatibility)
    daily_simple = final_df[["month", "day", "mean_temp_f", "hdd_normal", "cdd_normal", 
                             "mean_temp_f_10yr", "hdd_normal_10yr", "cdd_normal_10yr"]]
                             
    # 2. us_gas_weighted_normals.csv (Everything)
    daily_gw = final_df.copy()
    
    Path("data/normals").mkdir(parents=True, exist_ok=True)
    daily_simple.to_csv("data/normals/us_daily_normals.csv", index=False)
    daily_gw.to_csv("data/normals/us_gas_weighted_normals.csv", index=False)
    
    print("  [OK] Successfully rewrote data/normals/us_daily_normals.csv")
    print("  [OK] Successfully rewrote data/normals/us_gas_weighted_normals.csv")
    print("\n--- Summary ---")
    print(f"30-Yr Epoch (1991-2020) total GW HDD: {grp_30['hdd_normal_gw'].sum():.1f}")
    print(f"10-Yr Epoch (2016-2025) total GW HDD: {grp_10['hdd_normal_gw_10yr'].sum():.1f}")
    print(f"Warming Delta: {grp_10['hdd_normal_gw_10yr'].sum() - grp_30['hdd_normal_gw'].sum():.1f} HDDs (10-Yr is warmer)")

if __name__ == "__main__":
    run()
