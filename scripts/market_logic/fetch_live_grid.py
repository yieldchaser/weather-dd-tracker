"""
fetch_live_grid.py

Uses official EIA v2 API to query live fuel mix grid generation for Big 4 ISOs.
Calculates 30-day historical averages strictly to compute anomaly metrics
for Wind vs Natural Gas.

Synthesizes a 5th 'NATIONAL' row containing the aggregate sums across ISOs, 
propaganea NaNs, and logs historical wind actuals to a persistent CSV.
"""

import os
import sys
import time
import requests
import pandas as pd
import datetime
import pytz
from pathlib import Path

# Outputs
OUTPUT_DIR = Path("outputs")
OUTPUT_FILE = OUTPUT_DIR / "live_grid_generation.csv"
HISTORY_FILE = Path("outputs/wind/wind_actuals_history.csv")

# Constants
TOTAL_INSTALLED_GW = 110.0
EIA_API_KEY = os.environ.get("EIA_KEY")
ISO_LIST = ["ERCO", "PJM", "MISO", "SWPP"]

def get_eia_fuel_mix(iso_code, start_dt, today):
    """
    Queries EIA v2 API for the last 30 days of hourly electricity generation
    by fuel type within a specific ISO / Balancing Authority.
    """
    start_str = start_dt.strftime("%Y-%m-%dT%H")
    end_str = today.strftime("%Y-%m-%dT%H")
    
    url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
    
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
    
    if not EIA_API_KEY:
        print("  [ERR] EIA_KEY env var not set. Skipping EIA fetch.")
        return pd.DataFrame()

    print(f"--- Fetching Live Grid Generation ({iso_code} via EIA) ---")
    
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            if "response" in data and "data" in data["response"]:
                records = data["response"]["data"]
                if records:
                    return pd.DataFrame(records)
            
            print(f"  [WARN] Unexpected EIA json framework: {data.keys()}")
            
        except requests.exceptions.HTTPError as e:
            print(f"  [ERR] EIA API Fetch failed HTTP (Attempt {attempt+1}/3): {e}")
            if r.status_code == 403:
                print("  [ERR] EIA API Key rejected. Ensure EIA_KEY secret is set correctly.")
                break
        except Exception as e:
            print(f"  [ERR] EIA API Fetch failed (Attempt {attempt+1}/3): {e}")
            
        time.sleep(2)
        
    return pd.DataFrame()

def fetch_live_grid():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    tz = pytz.timezone("US/Central")
    today = datetime.datetime.now(tz)
    today_date = today.date()
    today_str = pd.to_datetime(today_date).strftime("%Y-%m-%d")
    start_dt = today - datetime.timedelta(days=35)
    
    out_rows = []
    
    fuel_map = {
        "natural gas": "Natural Gas",
        "wind": "Wind",
        "solar": "Solar",
        "coal": "Coal",
        "nuclear": "Nuclear"
    }

    # ISO labels for output
    iso_labels = {
        "ERCO": "ERCOT",
        "PJM": "PJM",
        "MISO": "MISO",
        "SWPP": "SWPP"
    }
    
    for iso_code in ISO_LIST:
        df = get_eia_fuel_mix(iso_code, start_dt, today)
        
        if df.empty:
            print(f"  [WARN] No data for {iso_code}. Appending NaN row.")
            out_rows.append(_create_nan_row(today_str, iso_labels[iso_code]))
            continue
            
        df["date"] = pd.to_datetime(df["period"]).dt.date
        df["type-name"] = df["type-name"].str.lower().str.strip().map(fuel_map)
        df = df.dropna(subset=["type-name", "value"])
        
        df["value"] = pd.to_numeric(df["value"], errors='coerce')
        df = df.dropna(subset=["value"])
        
        daily_avg = df.groupby(["date", "type-name"])["value"].mean().reset_index()
        daily_pivot = daily_avg.pivot(index="date", columns="type-name", values="value").reset_index()
        daily_pivot["date"] = pd.to_datetime(daily_pivot["date"])
        
        for col in fuel_map.values():
            if col not in daily_pivot.columns:
                daily_pivot[col] = float('nan')

        daily_pivot.sort_values("date", inplace=True)
        
        start_date_filter = pd.to_datetime(today_date - datetime.timedelta(days=30))
        daily_pivot = daily_pivot[daily_pivot["date"] >= start_date_filter]
        
        if daily_pivot.empty:
            print(f"  [WARN] {iso_code} pivot is empty. Appending NaN row.")
            out_rows.append(_create_nan_row(today_str, iso_labels[iso_code]))
            continue

        latest_date_str = daily_pivot["date"].max().strftime("%Y-%m-%d")
        
        historical = daily_pivot[daily_pivot["date"].dt.strftime("%Y-%m-%d") != latest_date_str]
        hist_wind = historical["Wind"].mean() if not historical.empty else float('nan')
        
        today_data = daily_pivot[daily_pivot["date"].dt.strftime("%Y-%m-%d") == latest_date_str]
        
        if not today_data.empty:
            row = today_data.iloc[-1].to_dict()
            live_wind = row["Wind"]
            
            out_row = {
                "date": row["date"].strftime("%Y-%m-%d"),
                "iso": iso_labels[iso_code],
                "natural_gas_mw": round(row["Natural Gas"]) if pd.notna(row["Natural Gas"]) else float('nan'),
                "wind_mw": round(live_wind) if pd.notna(live_wind) else float('nan'),
                "solar_mw": round(row["Solar"]) if pd.notna(row["Solar"]) else float('nan'),
                "coal_mw": round(row["Coal"]) if pd.notna(row["Coal"]) else float('nan')
            }
            
            if pd.notna(hist_wind) and pd.notna(live_wind):
                anomaly = live_wind - hist_wind
                out_row["wind_30d_avg_mw"] = round(hist_wind)
                out_row["wind_anomaly_mw"] = round(anomaly)
                
                if anomaly < -1000:
                    impact = "BULLISH (Wind Drought)"
                elif anomaly > 1500:
                    impact = "BEARISH (Strong Wind)"
                else:
                    impact = "NEUTRAL"
            else:
                out_row["wind_30d_avg_mw"] = float('nan')
                out_row["wind_anomaly_mw"] = float('nan')
                impact = "CALCULATING BASELINE"
                
            out_row["gas_burn_impact"] = impact
            out_rows.append(out_row)
        else:
            print(f"  [WARN] No today_data for {iso_code}. Appending NaN row.")
            out_rows.append(_create_nan_row(today_str, iso_labels[iso_code]))
            
    # --- NATIONAL AGGREGATION ---
    nat_gas_mw_sum = 0.0
    wind_mw_sum = 0.0
    wind_anomaly_sum = 0.0
    solar_mw_sum = 0.0
    coal_mw_sum = 0.0
    wind_30d_sum = 0.0
    gas_isos = 0
    wind_isos = 0
    anomaly_isos = 0

    for r in out_rows:
        if pd.notna(r["natural_gas_mw"]): nat_gas_mw_sum += r["natural_gas_mw"]; gas_isos += 1
        if pd.notna(r["wind_mw"]): wind_mw_sum += r["wind_mw"]; wind_isos += 1
        if pd.notna(r["wind_anomaly_mw"]): wind_anomaly_sum += r["wind_anomaly_mw"]; anomaly_isos += 1
        if pd.notna(r["solar_mw"]): solar_mw_sum += r["solar_mw"]
        if pd.notna(r["coal_mw"]): coal_mw_sum += r["coal_mw"]
        if pd.notna(r["wind_30d_avg_mw"]): wind_30d_sum += r["wind_30d_avg_mw"]

    total_isos = len(out_rows)
    partial = gas_isos < total_isos

    nat_row = {
        "date": today_str,
        "iso": "NATIONAL",
        "natural_gas_mw": int(round(nat_gas_mw_sum)) if gas_isos > 0 else float('nan'),
        "wind_mw": int(round(wind_mw_sum)) if wind_isos > 0 else float('nan'),
        "solar_mw": int(round(solar_mw_sum)),
        "coal_mw": int(round(coal_mw_sum)),
        "wind_30d_avg_mw": int(round(wind_30d_sum)) if anomaly_isos > 0 else float('nan'),
        "wind_anomaly_mw": int(round(wind_anomaly_sum)) if anomaly_isos > 0 else float('nan'),
    }
    
    if pd.notna(nat_row["wind_anomaly_mw"]):
        if nat_row["wind_anomaly_mw"] < -3000:
            impact = "BULLISH (Wind Drought)"
        elif nat_row["wind_anomaly_mw"] > 4000:
            impact = "BEARISH (Strong Wind)"
        else:
            impact = "NEUTRAL"
        if partial:
            impact += f" ({gas_isos}/{total_isos} ISOs)"
    else:
        impact = "CALCULATING BASELINE"
        
    nat_row["gas_burn_impact"] = impact
    out_rows.append(nat_row)
    
    out_df = pd.DataFrame(out_rows)
    cols = ["date", "iso", "natural_gas_mw", "wind_mw", "solar_mw", "coal_mw", "wind_30d_avg_mw", "wind_anomaly_mw", "gas_burn_impact"]
    out_df = out_df[[c for c in cols if c in out_df.columns]]
    # Guard: Only overwrite if we have non-null national generation data
    if pd.notna(nat_row.get("natural_gas_mw")) and pd.notna(nat_row.get("wind_mw")):
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n  [OK] Saved Live Grid Generation -> {OUTPUT_FILE}")
    else:
        print("\n  [WARN] Fetched data contains nulls for core metrics. Skipping overwrite of live_grid_generation.csv to preserve last good state.")
    
    # Update historical record
    update_wind_history(nat_row, out_rows)
    print(out_df.to_string(index=False))

def _create_nan_row(today_str, iso_label):
    return {
        "date": today_str,
        "iso": iso_label,
        "natural_gas_mw": float('nan'),
        "wind_mw": float('nan'),
        "solar_mw": float('nan'),
        "coal_mw": float('nan'),
        "wind_30d_avg_mw": float('nan'),
        "wind_anomaly_mw": float('nan'),
        "gas_burn_impact": "NEUTRAL"
    }

def update_wind_history(nat_row, out_rows):
    """
    Appends today's national and ISO-level wind MW to the history CSV if not already present.
    Ensures long-term data depth even if the snapshot file is overwritten.
    """
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Calculate CF %: (MW / (GW * 1000)) * 100
    national_mw = nat_row.get("wind_mw", 0)
    nat_cf_pct = (national_mw / (TOTAL_INSTALLED_GW * 1000) * 100) if national_mw else 0.0
    
    # Map ISOs for cleaner column extraction
    iso_map = {r["iso"]: r["wind_mw"] for r in out_rows}
    
    new_data = {
        "date": nat_row["date"],
        "national_wind_mw": int(round(national_mw)) if pd.notna(national_mw) else 0,
        "national_wind_cf_pct": round(nat_cf_pct, 1),
        "ercot_wind_mw": int(round(iso_map.get("ERCOT", 0))) if pd.notna(iso_map.get("ERCOT")) else 0,
        "pjm_wind_mw": int(round(iso_map.get("PJM", 0))) if pd.notna(iso_map.get("PJM")) else 0,
        "miso_wind_mw": int(round(iso_map.get("MISO", 0))) if pd.notna(iso_map.get("MISO")) else 0,
        "spp_wind_mw": int(round(iso_map.get("SWPP", 0))) if pd.notna(iso_map.get("SWPP")) else 0
    }
    
    new_df = pd.DataFrame([new_data])
    
    if HISTORY_FILE.exists():
        try:
            old_df = pd.read_csv(HISTORY_FILE)
            # Deduplicate by date
            if new_data["date"] in old_df["date"].astype(str).values:
                print(f"  [INFO] Data for {new_data['date']} already exists in history. Skipping append.")
                return
            
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined.to_csv(HISTORY_FILE, index=False)
            print(f"  [OK] Appended {new_data['date']} to {HISTORY_FILE}")
        except Exception as e:
            print(f"  [ERR] Failed to update history: {e}")
    else:
        new_df.to_csv(HISTORY_FILE, index=False)
        print(f"  [OK] Created new history file: {HISTORY_FILE}")

if __name__ == "__main__":
    fetch_live_grid()
