"""
fetch_historical_eia_normals.py

Purpose:
- Fetches the TRUE historical Population-Weighted Degree Days (HDD/CDD) from the US Energy Information Administration (EIA).
- This provides an accurate historical baseline going back to 1949, matching the Skylar Capital style charts.
- Saves the data to `data/normals/eia_historical_hdd_cdd.csv`

Requirements:
- Needs an `EIA_KEY` environment variable. (Get free at: https://www.eia.gov/opendata/register.php)
"""

import os
import requests
import pandas as pd
from pathlib import Path

# EIA API v2 endpoint for Total Energy Data
EIA_API_URL = "https://api.eia.gov/v2/total-energy/data/"

# ZWHDUS = Population-Weighted Heating Degree Days
# ZWCDUS = Population-Weighted Cooling Degree Days
SERIES_IDS = ["ZWHDPUS", "ZWCDPUS"]

OUTPUT_FILE = Path("data/normals/eia_historical_hdd_cdd.csv")

def run():
    print("\n--- Fetching Historical EIA Degree Days ---")
    
    api_key = os.environ.get("EIA_KEY")
    if not api_key or api_key.strip() == "":
        print("  [WARN] Missing EIA_KEY environment variable. Cannot update historical baseline.")
        print("         Returning early. The charting script will rely on previously downloaded data if it exists.")
        return False
        
    params = {
        "api_key": api_key,
        "frequency": "monthly",
        "data[0]": "value",
        "facets[msn][]": SERIES_IDS,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000 # Max allowed per page
    }
    
    try:
        r = requests.get(EIA_API_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  [ERR] Failed to fetch data from EIA API: {e}")
        return False
        
    records = data.get("response", {}).get("data", [])
    if not records:
        print("  [WARN] EIA API returned no data.")
        return False
        
    df = pd.DataFrame(records)
    
    # Clean up the dataframe
    if "msn" not in df.columns or "period" not in df.columns or "value" not in df.columns:
         print("  [ERR] Unrecognized EIA data format.")
         return False

    df = df[["period", "msn", "value", "unit"]].copy()
    
    # period is in YYYY-MM format
    df["date"] = pd.to_datetime(df["period"] + "-01")
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["value"] = pd.to_numeric(df["value"], errors='coerce')
    
    # Label the degree day type
    df["type"] = df["msn"].map({"ZWHDPUS": "HDD", "ZWCDPUS": "CDD"})
    
    # Drop NAs
    df = df.dropna(subset=["value"])
    
    # Sort for cleanliness
    df = df.sort_values(["type", "year", "month"]).reset_index(drop=True)
    
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"  [OK] Saved {len(df)} historical monthly degree day records.")
    print(f"       File -> {OUTPUT_FILE}")
    
    # Show a quick summary
    print(f"       Data Range: {df['year'].min()} to {df['year'].max()}")
    return True

if __name__ == "__main__":
    run()
