"""
fetch_live_grid.py

Uses official EIA v2 API to query live fuel mix grid generation for 7 ISOs.
Now includes LOAD (demand) data and ISO-level row persistence in outputs.
"""

import os
import sys
import time
import requests
import pandas as pd
import datetime
import pytz
import json

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
from pathlib import Path

# Outputs
OUTPUT_DIR = Path("outputs")
OUTPUT_FILE = OUTPUT_DIR / "live_grid_generation.csv"
HISTORY_FILE = Path("outputs/wind/wind_actuals_history.csv")

# Constants
TOTAL_INSTALLED_GW = 110.0
EIA_API_KEY = os.environ.get("EIA_KEY")

ISO_LIST = ["ERCO", "PJM", "MISO", "SWPP", "CISO", "ISNE", "NYIS"]

ISO_DISPLAY = {
    "ERCO": "ERCOT",
    "PJM":  "PJM",
    "MISO": "MISO",
    "SWPP": "SPP",
    "CISO": "CAISO",
    "ISNE": "ISONE",
    "NYIS": "NYISO",
}

def get_eia_data(endpoint, iso_code, start_dt, today, data_type=None):
    """
    Generic EIA v2 API caller.
    """
    start_str = start_dt.strftime("%Y-%m-%dT%H")
    end_str = today.strftime("%Y-%m-%dT%H")
    
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": iso_code,
        "start": start_str,
        "end": end_str,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000
    }
    
    if data_type:
        params["facets[type][]"] = data_type
        
    for attempt in range(3):
        try:
            r = requests.get(endpoint, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if "response" in data and "data" in data["response"]:
                return pd.DataFrame(data["response"]["data"])
        except Exception as e:
            print(f"  [ERR] EIA Fetch failed ({iso_code}, {data_type}): {e}")
        time.sleep(2)
    return pd.DataFrame()

def fetch_live_grid():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if not EIA_API_KEY:
        print("[ERR] EIA_KEY not set.")
        return

    tz = pytz.timezone("US/Central")
    today = datetime.datetime.now(tz)
    start_dt = today - datetime.timedelta(days=35)
    
    fuel_map = {"NG": "natural_gas_mw", "COL": "coal_mw", "NUC": "nuclear_mw", "WND": "wind_mw", "SUN": "solar_mw"}
    
    gen_url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
    load_url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    
    all_iso_output_rows = []
    hourly_records = []
    
    for iso_code in ISO_LIST:
        print(f"Processing {ISO_DISPLAY[iso_code]}...")
        # 1. Fetch Generation
        gen_df = get_eia_data(gen_url, iso_code, start_dt, today)
        # 2. Fetch Load
        load_df = get_eia_data(load_url, iso_code, start_dt, today, data_type="D")
        
        if gen_df.empty or load_df.empty:
            continue
            
        # Process Gen
        if "fueltype" in gen_df.columns: gen_df["fuel-mapped"] = gen_df["fueltype"].map(fuel_map)
        else: gen_df["fuel-mapped"] = gen_df["type-name"].str.upper().map(fuel_map)
        gen_df = gen_df.dropna(subset=["fuel-mapped", "value"])
        gen_df["value"] = pd.to_numeric(gen_df["value"], errors='coerce')
        
        # Hourly Pivot for ISO
        gen_hourly = gen_df.pivot_table(index="period", columns="fuel-mapped", values="value", aggfunc="mean").reset_index()
        
        # Process Load
        load_df["load_mw"] = pd.to_numeric(load_df["value"], errors='coerce')
        # Log the latest available non-null load for this ISO
        if not latest_load.empty:
            print(f"  [LOAD] {ISO_DISPLAY[iso_code]}: {round(latest_load['load_mw'].iloc[0])} MW")
        else:
            print(f"  [LOAD] {ISO_DISPLAY[iso_code]}: NO DATA RECEIVED")
        load_hourly = load_df[["period", "load_mw"]]
        
        # Merge Hourly
        merged_hourly = pd.merge(gen_hourly, load_hourly, on="period", how="left")
        merged_hourly["iso"] = ISO_DISPLAY[iso_code]
        hourly_records.append(merged_hourly)
        
        # Daily Aggregation
        merged_hourly["date_only"] = pd.to_datetime(merged_hourly["period"]).dt.strftime("%Y-%m-%d")
        daily = merged_hourly.groupby("date_only").mean(numeric_only=True).reset_index()
        
        # Get Latest Date (Anomaly Logic)
        latest_date = daily["date_only"].max()
        matching_rows = daily[daily["date_only"] == latest_date]
        if matching_rows.empty:
            print(f"  [WARN] No data for {latest_date} in {iso_code} — skipping")
            continue
        today_row = matching_rows.iloc[0].to_dict()
        
        # Anomaly logic (30d trailing)
        hist_wind = daily[daily["date_only"] != latest_date]["wind_mw"].mean()
        
        # Per-ISO impact logic
        anomaly = today_row.get("wind_mw", 0) - hist_wind if pd.notna(hist_wind) else 0
        if anomaly < -1000: impact = "BULLISH (Wind Drought)"
        elif anomaly > 1500: impact = "BEARISH (Strong Wind)"
        else: impact = "NEUTRAL"

        out_row = {
            "date": latest_date,
            "iso": ISO_DISPLAY[iso_code],
            "natural_gas_mw": round(today_row.get("natural_gas_mw", 0)) if pd.notna(today_row.get("natural_gas_mw")) else None,
            "wind_mw": round(today_row.get("wind_mw", 0)) if pd.notna(today_row.get("wind_mw")) else None,
            "solar_mw": round(today_row.get("solar_mw", 0)) if pd.notna(today_row.get("solar_mw")) else None,
            "coal_mw": round(today_row.get("coal_mw", 0)) if pd.notna(today_row.get("coal_mw")) else None,
            "nuclear_mw": round(today_row.get("nuclear_mw", 0)) if pd.notna(today_row.get("nuclear_mw")) else None,
            "load_mw": round(today_row.get("load_mw", 0)) if pd.notna(today_row.get("load_mw")) else None,
            "wind_30d_avg_mw": round(hist_wind) if pd.notna(hist_wind) else None,
            "wind_anomaly_mw": round(anomaly) if pd.notna(hist_wind) else None,
            "gas_burn_impact": impact
        }
        all_iso_output_rows.append(out_row)

    if not all_iso_output_rows:
        return

    # --- NATIONAL AGGREGATION ---
    hourly_all = pd.concat(hourly_records)
    # Filter to periods where we have majority coverage
    national_hourly = hourly_all.groupby("period").sum(numeric_only=True).reset_index()
    national_hourly["date_only"] = pd.to_datetime(national_hourly["period"]).dt.strftime("%Y-%m-%d")
    
    # Save hourly data for peaker script
    safe_write_csv(hourly_all, "outputs/hourly_grid_data.csv")
    
    nat_daily = national_hourly.groupby("date_only").mean(numeric_only=True).reset_index()
    latest_date_nat = nat_daily["date_only"].max()
    matching_nat = nat_daily[nat_daily["date_only"] == latest_date_nat]
    if matching_nat.empty:
        print(f"  [WARN] No national aggregate available for {latest_date_nat}")
        return
    nat_today = matching_nat.iloc[0].to_dict()
    
    # Trailing 30d for NATIONAL anomaly
    hist_wind_nat = nat_daily[nat_daily["date_only"] != latest_date_nat]["wind_mw"].mean()
    
    nat_row = {
        "date": latest_date_nat,
        "iso": "NATIONAL",
        "natural_gas_mw": round(nat_today["natural_gas_mw"]),
        "wind_mw": round(nat_today["wind_mw"]),
        "solar_mw": round(nat_today["solar_mw"]),
        "coal_mw": round(nat_today["coal_mw"]),
        "nuclear_mw": round(nat_today["nuclear_mw"]),
        "load_mw": round(nat_today["load_mw"]),
        "wind_30d_avg_mw": round(hist_wind_nat) if pd.notna(hist_wind_nat) else None,
        "wind_anomaly_mw": round(nat_today["wind_mw"] - hist_wind_nat) if pd.notna(hist_wind_nat) else None
    }
    
    # Impact logic
    anom = nat_row["wind_anomaly_mw"] or 0
    if anom < -3000: impact_nat = "BULLISH (Wind Drought)"
    elif anom > 4000: impact_nat = "BEARISH (Strong Wind)"
    else: impact_nat = "NEUTRAL"
    nat_row["gas_burn_impact"] = impact_nat

    # Thermal & Load Metrics
    nat_row["total_thermal_mw"] = (nat_row["natural_gas_mw"] or 0) + (nat_row["coal_mw"] or 0) + (nat_row["nuclear_mw"] or 0)
    nat_row["gas_pct_thermal"] = round(nat_row["natural_gas_mw"] / nat_row["total_thermal_mw"] * 100, 1) if nat_row["total_thermal_mw"] > 0 else None
    nat_row["gas_pct_load"] = round(nat_row["natural_gas_mw"] / nat_row["load_mw"] * 100, 1) if nat_row["load_mw"] > 0 else None
    
    # Add National to top
    all_iso_output_rows.insert(0, nat_row)
    
    # --- APPEND AND ROLL ---
    new_df = pd.DataFrame(all_iso_output_rows)
    cols = ["date", "iso", "natural_gas_mw", "wind_mw", "solar_mw", "coal_mw", "nuclear_mw", "load_mw", "total_thermal_mw", "gas_pct_thermal", "gas_pct_load", "wind_30d_avg_mw", "wind_anomaly_mw", "gas_burn_impact"]
    new_df = new_df[[c for c in cols if c in new_df.columns]]
    
    if OUTPUT_FILE.exists():
        try:
            old_df = pd.read_csv(OUTPUT_FILE)
            # Merge and Deduplicate by date + iso
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["date", "iso"], keep="last")
            
            # Rolling 35-day window per ISO (National included)
            # Convert to datetime for sorting and filtering
            combined["date_dt"] = pd.to_datetime(combined["date"])
            cutoff = combined["date_dt"].max() - datetime.timedelta(days=35)
            combined = combined[combined["date_dt"] > cutoff].sort_values(["date_dt", "iso"])
            
            # Drop helper column and save
            safe_write_csv(combined.drop(columns=["date_dt"]), OUTPUT_FILE)
            print(f"[OK] Appended/Cleaned {OUTPUT_FILE} (35-day rolling window)")
        except Exception as e:
            print(f"[ERR] Grid append failed: {e}")
            new_df.to_csv(OUTPUT_FILE, index=False)
    else:
        safe_write_csv(new_df, OUTPUT_FILE)
        print(f"[OK] Created initial {OUTPUT_FILE}")

    # Update history CSV
    update_wind_history(nat_row, all_iso_output_rows)

def update_wind_history(nat_row, out_rows):
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    national_mw = nat_row.get("wind_mw", 0)
    nat_cf_pct = (national_mw / (TOTAL_INSTALLED_GW * 1000) * 100) if national_mw else 0.0
    iso_map = {r["iso"]: r["wind_mw"] for r in out_rows}
    new_data = {
        "date": nat_row["date"],
        "national_wind_mw": int(round(national_mw)) if pd.notna(national_mw) else 0,
        "national_wind_cf_pct": round(nat_cf_pct, 1),
        "ercot_wind_mw": int(round(iso_map.get("ERCOT", 0))) if pd.notna(iso_map.get("ERCOT")) else 0,
        "pjm_wind_mw": int(round(iso_map.get("PJM", 0))) if pd.notna(iso_map.get("PJM")) else 0,
        "miso_wind_mw": int(round(iso_map.get("MISO", 0))) if pd.notna(iso_map.get("MISO")) else 0,
        "spp_wind_mw": int(round(iso_map.get("SPP", 0))) if pd.notna(iso_map.get("SPP")) else 0
    }
    new_df = pd.DataFrame([new_data])
    if HISTORY_FILE.exists():
        try:
            old_df = pd.read_csv(HISTORY_FILE)
            if new_data["date"] in old_df["date"].astype(str).values: return
            combined = pd.concat([old_df, new_df], ignore_index=True)
            safe_write_csv(combined, HISTORY_FILE)
        except Exception as e: print(f"[ERR] History update failed: {e}")
    else: safe_write_csv(new_df, HISTORY_FILE)

if __name__ == "__main__":
    import pathlib
    script_name = pathlib.Path(__file__).stem
    try:
        fetch_live_grid()
        health = {"script": __file__, "status": "ok", "timestamp": datetime.datetime.now(pytz.UTC).isoformat() + "Z"}
        pathlib.Path("outputs/health").mkdir(exist_ok=True, parents=True)
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
            "timestamp": datetime.datetime.now(pytz.UTC).isoformat() + "Z"
        }
        pathlib.Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
