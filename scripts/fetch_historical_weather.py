"""
fetch_historical_weather.py

Fetches historical ERA5 weather data from Open-Meteo Archive API
to construct a daily Total Degree Day (TDD) index for the US
Gas-Weighted demand cities.

Outputs:
- outputs/historical_degree_days.csv
"""

import datetime
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from demand_constants import DEMAND_CITIES, TOTAL_WEIGHT

URL = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "1991-01-01"

def celsius_to_f(c):
    return c * 9 / 5 + 32

def main():
    print("\n--- Fetching Historical Weather (Open-Meteo ERA5) ---")
    
    # We fetch up to yesterday to ensure completeness of archive
    END_DATE = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    output_path = Path("outputs/historical_degree_days.csv")
    
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
    print("  Calculating Gas Weights...")
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
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(output_path, index=False)
    
    print(f"  [OK] Saved {len(final_df)} days of historical data to {output_path}")

if __name__ == "__main__":
    main()
