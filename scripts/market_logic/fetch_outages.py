"""
fetch_outages.py

Aggregates national daily generator outages for Nuclear, Coal, and Gas.
"""
import os
import requests
import pandas as pd
from pathlib import Path
import json
import sys
import datetime

def safe_write_csv(df, path, min_rows=1):
    """Only write if dataframe has meaningful data."""
    if df is None or len(df) < min_rows:
        print(f"[SKIP] {path} — insufficient data ({len(df) if df is not None else 0} rows), preserving last state")
        return False
    df.to_csv(path, index=False)
    print(f"[OK] Written {path} ({len(df)} rows)")
    return True

def safe_write_json(data, path, required_keys=None):
    """Only write if data has required keys and is non-empty."""
    if not data:
        print(f"[SKIP] {path} — empty data, preserving last state")
        return False
    if required_keys:
        missing = [k for k in required_keys if k not in data]
        if missing:
            print(f"[SKIP] {path} — missing keys {missing}, preserving last state")
            return False
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"[OK] Written {path}")
    return True

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
        combined = pd.concat([old_df, out_df]).drop_duplicates(subset=["date", "iso"], keep="last")
        safe_write_csv(combined, OUTPUT_FILE)
    else:
        safe_write_csv(out_df, OUTPUT_FILE)
    print(f"[OK] Saved outages to {OUTPUT_FILE}")

if __name__ == "__main__":
    script_name = Path(__file__).stem
    try:
        fetch_grid_outages()
        health = {"script": __file__, "status": "ok", "timestamp": datetime.datetime.utcnow().isoformat() + "Z"}
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
    except Exception as e:
        print(f"[CRITICAL] {__file__} failed: {e}")
        import traceback
        traceback.print_exc()
        health = {
            "script": __file__,
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
        }
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
