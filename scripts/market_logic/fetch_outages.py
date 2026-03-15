"""
fetch_outages.py

Aggregates national daily generator outages for Nuclear, Coal, and Gas.
"""
import os
import requests
import pandas as pd
from pathlib import Path

OUTPUT_FILE = Path("outputs/grid_outages.csv")
EIA_API_KEY = os.environ.get("EIA_KEY")

def fetch_grid_outages():
    if not EIA_API_KEY:
        print("[ERR] EIA_KEY not set.")
        return

    url = "https://api.eia.gov/v2/electricity/outages/generators/data/"
    fuel_types = ["NG", "NUC", "COL"]
    all_records = []
    
    for fuel in fuel_types:
        params = {
            "api_key": EIA_API_KEY,
            "frequency": "daily",
            "data[0]": "capacity",
            "facets[fuelTypeCode][]": fuel,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 500
        }
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            res_data = r.json()
            if "response" in res_data and "data" in res_data["response"]:
                recs = res_data["response"]["data"]
                all_records.extend(recs)
                print(f"  [OUTAGES] {fuel}: {len(recs)} records")
        except Exception as e:
            print(f"  [WARN] Outages fetch failed for {fuel}: {e}")

    if not all_records:
        return
        
    df = pd.DataFrame(all_records)
    df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce")
    
    # Pivot: Rows = date (period), Cols = fuelTypeCode
    pivot = df.pivot_table(index="period", columns="fuelTypeCode", values="capacity", aggfunc="sum").reset_index()
    
    # Rename to schema
    outage_map = {"NG": "gas_outage_mw", "NUC": "nuclear_outage_mw", "COL": "coal_outage_mw"}
    pivot = pivot.rename(columns=outage_map)
    
    # Ensure all columns exist
    for col in outage_map.values():
        if col not in pivot.columns: pivot[col] = 0.0
    
    # Approximate national fleet capacities (GW -> MW)
    FLEET_NUC = 95000.0 
    FLEET_COL = 180000.0
    
    pivot["date"] = pivot["period"]
    pivot["iso"] = "NATIONAL"
    pivot["nuclear_capacity_mw"] = FLEET_NUC
    pivot["coal_capacity_mw"] = FLEET_COL
    pivot["total_outage_mw"] = pivot["nuclear_outage_mw"] + pivot["coal_outage_mw"] + pivot["gas_outage_mw"]
    
    pivot["nuclear_availability_pct"] = round((FLEET_NUC - pivot["nuclear_outage_mw"]) / FLEET_NUC * 100, 1)
    
    out_df = pivot[["date", "iso", "nuclear_outage_mw", "coal_outage_mw", "total_outage_mw", "nuclear_capacity_mw", "coal_capacity_mw", "nuclear_availability_pct"]]
    
    if OUTPUT_FILE.exists():
        old_df = pd.read_csv(OUTPUT_FILE)
        combined = pd.concat([old_df, out_df]).drop_duplicates(subset=["date", "iso"], keep="first")
        combined.to_csv(OUTPUT_FILE, index=False)
    else:
        out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"[OK] Saved outages to {OUTPUT_FILE}")

if __name__ == "__main__":
    fetch_grid_outages()
