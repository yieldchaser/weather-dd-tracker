"""
fetch_live_grid.py

Uses official EIA v2 API to query live fuel mix grid generation for Big 4 ISOs.
Calculates 30-day historical averages strictly to compute anomaly metrics
for Wind vs Natural Gas.

Synthesizes a 5th 'NATIONAL' row containing the aggregate sums across ISOs, 
propagating NaN values to ensure no partial data is displayed.
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
    
    print(f"--- Fetching Live Grid Generation ({iso_code} via EIA) ---")
    
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
    
    tz = pytz.timezone("US/Central")
    today = datetime.datetime.now(tz)
    today_date = today.date()
    today_str = pd.to_datetime(today_date).strftime("%Y-%m-%d")
    start_dt = today - datetime.timedelta(days=35)
    
    out_rows = []
    
    fuel_map = {
        "Natural gas": "Natural Gas",
        "Wind": "Wind",
        "Solar": "Solar",
        "Coal": "Coal",
        "Nuclear": "Nuclear"
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
        df["type-name"] = df["type-name"].map(fuel_map)
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

        historical = daily_pivot[daily_pivot["date"].dt.strftime("%Y-%m-%d") != today_str]
        hist_wind = historical["Wind"].mean() if not historical.empty else float('nan')
        
        today_data = daily_pivot[daily_pivot["date"].dt.strftime("%Y-%m-%d") == today_str]
        
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
    has_nan_gas = False
    has_nan_wind = False
    has_nan_anomaly = False
    solar_mw_sum = 0.0
    coal_mw_sum = 0.0
    wind_30d_sum = 0.0
    
    for r in out_rows:
        if pd.isna(r["natural_gas_mw"]): has_nan_gas = True
        else: nat_gas_mw_sum += r["natural_gas_mw"]
            
        if pd.isna(r["wind_mw"]): has_nan_wind = True
        else: wind_mw_sum += r["wind_mw"]
            
        if pd.isna(r["wind_anomaly_mw"]): has_nan_anomaly = True
        else: wind_anomaly_sum += r["wind_anomaly_mw"]
            
        if pd.notna(r["solar_mw"]): solar_mw_sum += r["solar_mw"]
        if pd.notna(r["coal_mw"]): coal_mw_sum += r["coal_mw"]
        if pd.notna(r["wind_30d_avg_mw"]): wind_30d_sum += r["wind_30d_avg_mw"]
        
    nat_row = {
        "date": today_str,
        "iso": "NATIONAL",
        "natural_gas_mw": float('nan') if has_nan_gas else int(round(nat_gas_mw_sum)),
        "wind_mw": float('nan') if has_nan_wind else int(round(wind_mw_sum)),
        "solar_mw": int(round(solar_mw_sum)),
        "coal_mw": int(round(coal_mw_sum)),
        "wind_30d_avg_mw": float('nan') if has_nan_anomaly else int(round(wind_30d_sum)),
        "wind_anomaly_mw": float('nan') if has_nan_anomaly else int(round(wind_anomaly_sum)),
    }
    
    if pd.notna(nat_row["wind_anomaly_mw"]):
        if nat_row["wind_anomaly_mw"] < -3000:
            impact = "BULLISH (Wind Drought)"
        elif nat_row["wind_anomaly_mw"] > 4000:
            impact = "BEARISH (Strong Wind)"
        else:
            impact = "NEUTRAL"
    else:
        impact = "CALCULATING BASELINE"
        
    nat_row["gas_burn_impact"] = impact
    out_rows.append(nat_row)
    
    out_df = pd.DataFrame(out_rows)
    cols = ["date", "iso", "natural_gas_mw", "wind_mw", "solar_mw", "coal_mw", "wind_30d_avg_mw", "wind_anomaly_mw", "gas_burn_impact"]
    out_df = out_df[[c for c in cols if c in out_df.columns]]
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n  [OK] Saved Live Grid Generation -> {OUTPUT_FILE}")
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

if __name__ == "__main__":
    fetch_live_grid()
