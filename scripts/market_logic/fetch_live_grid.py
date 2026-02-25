"""
fetch_live_grid.py

Uses official EIA v2 API to query live ERCOT fuel mix grid generation.
Calculates 30-day historical averages strictly to compute anomaly metrics
for Wind vs Natural Gas.

Maintains the exact output schema from previous proxy versions
to seamlessly map to index.html without breaking downstream UX.
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

# Configuration
EIA_API_KEY = os.environ.get("EIA_API_KEY", "aV619Ak6xj07qNmW2f3sTYIL9Eb3ow486baGknRy")

def get_eia_fuel_mix_ercot():
    """
    Queries EIA v2 API for the last 30 days of hourly electricity generation
    by fuel type within the ERCOT (ERCO) Balancing Authority.
    """
    
    # We use US Central time for ERCOT alignment
    tz = pytz.timezone("US/Central")
    today = datetime.datetime.now(tz)
    
    # Pull the last 35 days from EIA just to ensure we have a full 30-day historical baseline
    start_dt = today - datetime.timedelta(days=35)
    
    start_str = start_dt.strftime("%Y-%m-%dT%H")
    end_str = today.strftime("%Y-%m-%dT%H")
    
    # EIA v2 Endpoint for hourly fuel-type generation
    url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
    
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": "ERCO",  # ERCOT Balancing Authority
        "start": start_str,
        "end": end_str,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000  # Usually ~800 hours * 5-6 fuel types
    }
    
    print(f"--- Fetching Live ISO Grid Generation (ERCOT via EIA) ---")
    
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=15)
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
                print("  [ERR] EIA API Key rejected. Ensure it is active.")
                break
        except Exception as e:
            print(f"  [ERR] EIA API Fetch failed (Attempt {attempt+1}/3): {e}")
            
        time.sleep(2)
        
    return pd.DataFrame()


def fetch_live_grid():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    df = get_eia_fuel_mix_ercot()
    
    tz = pytz.timezone("US/Central")
    today = datetime.datetime.now(tz).date()
    today_str = pd.to_datetime(today).strftime("%Y-%m-%d")

    # If API fails, issue NaN fallback schema
    if df.empty:
        print("\n  [ERR] EIA API failed. Generating NaN fallback.")
        _write_fallback(today_str)
        sys.exit(1)
        
    # Example EIA Columns: 'period', 'type-name', 'value'
    # Fuel types: 'Natural gas', 'Wind', 'Solar', 'Coal', 'Nuclear', etc.
    
    # 1. Standardize date grouping
    # period format is often 'YYYY-MM-DDTHH'
    df["date"] = pd.to_datetime(df["period"]).dt.date
    
    # Mapping EIA exact fuel strings to our expected UI strings
    fuel_map = {
        "Natural gas": "Natural Gas",
        "Wind": "Wind",
        "Solar": "Solar",
        "Coal": "Coal",
        "Nuclear": "Nuclear"
    }
    
    df["type-name"] = df["type-name"].map(fuel_map)
    df = df.dropna(subset=["type-name", "value"])
    
    # Cast EIA API strings back to numeric for math operations
    df["value"] = pd.to_numeric(df["value"], errors='coerce')
    df = df.dropna(subset=["value"])
    
    # 2. Daily Averages
    # We want the average MW per day for each fuel type
    daily_avg = df.groupby(["date", "type-name"])["value"].mean().reset_index()
    
    # Pivot so rows are Dates and columns are Fuel Types
    daily_pivot = daily_avg.pivot(index="date", columns="type-name", values="value").reset_index()
    daily_pivot["date"] = pd.to_datetime(daily_pivot["date"])
    
    # Ensure all required columns exist in pivot just in case EIA dropped one for an entire month
    for col in fuel_map.values():
        if col not in daily_pivot.columns:
            daily_pivot[col] = float('nan')

    daily_pivot.sort_values("date", inplace=True)
    
    # Filter strictly to the last 30 days up to today
    start_date = pd.to_datetime(today - datetime.timedelta(days=30))
    daily_pivot = daily_pivot[daily_pivot["date"] >= start_date]
    
    if daily_pivot.empty:
        print("\n  [ERR] EIA parser produced empty pivot table. Exiting.")
        _write_fallback(today_str)
        sys.exit(1)

    # 3. Calculate Anomaly + UI Output
    
    out_rows = []
    
    # Calculate historical 30-day wind
    historical = daily_pivot[daily_pivot["date"].dt.strftime("%Y-%m-%d") != today_str]
    hist_wind = historical["Wind"].mean() if not historical.empty else float('nan')
    
    # Isolate Today
    today_data = daily_pivot[daily_pivot["date"].dt.strftime("%Y-%m-%d") == today_str]
    
    if not today_data.empty:
        row = today_data.iloc[-1].to_dict()
        live_wind = row["Wind"]
        
        # Formatting UI Math
        # Only round numerical values, allow NaNs to propagate natively to UI
        out_row = {
            "date": row["date"].strftime("%Y-%m-%d"),
            "iso": "ERCOT",
            "natural_gas_mw": round(row["Natural Gas"]) if pd.notna(row["Natural Gas"]) else float('nan'),
            "wind_mw": round(live_wind) if pd.notna(live_wind) else float('nan'),
            "solar_mw": round(row["Solar"]) if pd.notna(row["Solar"]) else float('nan'),
            "coal_mw": round(row["Coal"]) if pd.notna(row["Coal"]) else float('nan')
        }
        
        if pd.notna(hist_wind):
            anomaly = live_wind - hist_wind
            out_row["wind_30d_avg_mw"] = round(hist_wind)
            out_row["wind_anomaly_mw"] = round(anomaly)
            
            # Simple heuristic
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
        
    if out_rows:
        out_df = pd.DataFrame(out_rows)
        cols = ["date", "iso", "natural_gas_mw", "wind_mw", "solar_mw", "coal_mw", "wind_30d_avg_mw", "wind_anomaly_mw", "gas_burn_impact"]
        out_df = out_df[[c for c in cols if c in out_df.columns]]
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n  [OK] Saved Live Grid Generation -> {OUTPUT_FILE}")
        print(out_df.to_string(index=False))
    else:
        print("\n  [ERR] Failed to compute live EIA grid snapshot.")
        _write_fallback(today_str)
        sys.exit(1)


def _write_fallback(today_str):
    cols = ["date", "iso", "natural_gas_mw", "wind_mw", "solar_mw", "coal_mw", "wind_30d_avg_mw", "wind_anomaly_mw", "gas_burn_impact"]
    fallback_df = pd.DataFrame([{c: float('nan') for c in cols}])
    fallback_df["date"] = today_str
    fallback_df["iso"] = "ERCOT"
    fallback_df["gas_burn_impact"] = "NEUTRAL"
    fallback_df.to_csv(OUTPUT_FILE, index=False)
    print(f"  [WARN] Saved NaN Fallback -> {OUTPUT_FILE}")


if __name__ == "__main__":
    fetch_live_grid()
