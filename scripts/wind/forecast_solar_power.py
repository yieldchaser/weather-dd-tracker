"""
forecast_solar_power.py

Fetches solar irradiance (GHI) from Open-Meteo GFS/ECMWF for 12 nodes (~48 GW).
Converts GHI to Capacity Factor using a PVWatts-style efficiency model.
Calculates sub-daily sub-buckets (Peak/Off-Peak/Shoulder).
Generates solar_power_forecast.csv and a combined_drought.json signal.
"""

import os
import json
import logging
import time
import requests
import pandas as pd
from datetime import datetime, date, timedelta, UTC
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

SOLAR_NODES = [
    # (name, lat, lon, region, installed_gw)
    ("S_TX_PERMIAN",    31.5, -102.5, "ERCOT",  4.0),
    ("S_TX_SOUTH",      26.5,  -98.0, "ERCOT",  5.0),
    ("S_CA_MOJAVE",     35.0, -117.5, "WECC",   8.0),
    ("S_CA_SAN_JOAQUIN",36.5, -119.5, "WECC",   6.0),
    ("S_AZ_PHOENIX",    33.5, -112.0, "WECC",   5.0),
    ("S_NV_LAS_VEGAS",  36.0, -115.0, "WECC",   3.0),
    ("S_NM_CENTRAL",    34.5, -106.5, "WECC",   3.0),
    ("S_NC_CENTRAL",    35.5,  -79.5, "PJM",    4.0),
    ("S_VA_CENTRAL",    37.5,  -78.5, "PJM",    3.0),
    ("S_IN_CENTRAL",    40.0,  -86.5, "MISO",   2.5),
    ("S_MN_CENTRAL",    44.5,  -94.0, "MISO",   2.5),
    ("S_KS_CENTRAL",    38.5,  -98.0, "SPP",    2.0),
]

TOTAL_SOLAR_GW = sum(n[4] for n in SOLAR_NODES)  # ~48 GW

SOLAR_MODELS = {
    "GFS":        {"om_name": "gfs_seamless",  "horizon_days": 16},
    "ECMWF":      {"om_name": "ecmwf_ifs025",  "horizon_days": 10},
    "ECMWF_AIFS": {"om_name": "ecmwf_aifs025", "horizon_days": 10},
}

PERFORMANCE_RATIO = 0.75     # system losses (inverter, wiring, temp)
PEAK_HOURS    = list(range(13, 21))   # 13:00–20:00 UTC

BASE_URL = "https://api.open-meteo.com/v1/forecast"
HISTORICAL_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
CLIMO_PATH = "outputs/wind/solar_climo_30d.json"
WIND_DROUGHT_JSON = "outputs/wind/drought.json"
OUTPUT_CSV = "outputs/wind/solar_power_forecast.csv"
OUTPUT_JSON = "outputs/wind/combined_drought.json"

def irradiance_to_cf(ghi_wm2: float) -> float:
    """
    Convert GHI (W/m²) to capacity factor.
    Efficiency is already in the nameplate rating (STC = 1000 W/m²).
    CF = (GHI / 1000) * PERFORMANCE_RATIO
    """
    return min((ghi_wm2 / 1000.0) * PERFORMANCE_RATIO, 1.0)

def get_solar_drought_threshold(month: int) -> float:
    """
    Solar peak-hour CF drought threshold varies by season.
    Summer (Jun-Aug): solar potential is high, drought = below 35%
    Winter (Nov-Feb): solar potential is low, drought = below 15%
    Spring/Fall: 25%
    """
    if 6 <= month <= 8:
        return 0.35
    elif month >= 11 or month <= 2:
        return 0.15
    else:
        return 0.25

def build_solar_climatology():
    logging.info("Solar Climatology not found — bootstrapping via GFS Historical API...")
    end_date = date.today() - timedelta(days=5)
    start_date = end_date - timedelta(days=365 * 2) 
    
    params = {
        "latitude":  ",".join(str(n[1]) for n in SOLAR_NODES),
        "longitude": ",".join(str(n[2]) for n in SOLAR_NODES),
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date":   end_date.strftime("%Y-%m-%d"),
        "hourly":     "direct_radiation,diffuse_radiation",
        "models":     "gfs_seamless",
        "timezone":   "UTC"
    }

    try:
        resp = requests.get(HISTORICAL_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.error(f"Failed to fetch historical solar data: {e}")
        return {}

    if isinstance(data, dict): data = [data]
    
    dfs = []
    for i, node in enumerate(SOLAR_NODES):
        if i >= len(data): break
        h = data[i].get("hourly", {})
        if not h: continue
        
        times = pd.to_datetime(h["time"])
        ghi = pd.Series(h.get("direct_radiation", [])) + pd.Series(h.get("diffuse_radiation", []))
        
        df = pd.DataFrame({"time": times, "ghi": ghi})
        df["cf"] = df["ghi"].apply(irradiance_to_cf)
        df["gw"] = df["cf"] * node[4]
        dfs.append(df[["time", "gw"]])

    if not dfs: return {}
    
    all_data = pd.concat(dfs, ignore_index=True)
    national = all_data.groupby("time")["gw"].sum().reset_index()
    national["cf"] = national["gw"] / TOTAL_SOLAR_GW
    national["mm_dd"] = national["time"].dt.strftime("%m-%d")
    national["hour"] = national["time"].dt.hour
    
    climo = {}
    for mm_dd, group in national.groupby("mm_dd"):
        climo[mm_dd] = {
            "all":  float(group["cf"].mean()),
            "peak": float(group[group["hour"].isin(PEAK_HOURS)]["cf"].mean())
        }
        
    os.makedirs(os.path.dirname(CLIMO_PATH), exist_ok=True)
    with open(CLIMO_PATH, "w") as f:
        json.dump(climo, f, indent=2)
    logging.info(f"Rebuilt climo with {len(climo)} days.")
    return climo

def run_forecast():
    # Load climo
    if os.path.exists(CLIMO_PATH):
        try:
            with open(CLIMO_PATH, "r") as f: climo = json.load(f)
        except: climo = build_solar_climatology()
    else:
        climo = build_solar_climatology()
        
    all_rows = []
    utc_now = datetime.now(UTC)
    current_month = utc_now.month
    
    for model, config in SOLAR_MODELS.items():
        logging.info(f"Fetching {model} solar...")
        params = {
            "latitude":      ",".join(str(n[1]) for n in SOLAR_NODES),
            "longitude":     ",".join(str(n[2]) for n in SOLAR_NODES),
            "hourly":         "direct_radiation,diffuse_radiation",
            "past_days":      1,
            "forecast_days":  config["horizon_days"],
            "models":         config["om_name"],
            "timezone":       "UTC"
        }
        
        try:
            resp = requests.get(BASE_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logging.error(f"Failed to fetch {model}: {e}")
            continue
            
        if isinstance(data, dict): data = [data]
        
        node_dfs = []
        for i, node in enumerate(SOLAR_NODES):
            if i >= len(data): break
            h = data[i].get("hourly", {})
            if not h: continue
            
            times = pd.to_datetime(h["time"])
            ghi = pd.Series(h.get("direct_radiation", [])) + pd.Series(h.get("diffuse_radiation", []))
            df = pd.DataFrame({"time": times, "ghi": ghi})
            df["cf"] = df["ghi"].apply(irradiance_to_cf)
            df["gw"] = df["cf"] * node[4]
            node_dfs.append(df)
            
        if not node_dfs: continue
        
        all_model = pd.concat(node_dfs, ignore_index=True)
        national = all_model.groupby("time")["gw"].sum().reset_index()
        national["cf"] = national["gw"] / TOTAL_SOLAR_GW
        national["date"] = national["time"].dt.date
        national["hour"] = national["time"].dt.hour
        
        # Daily aggregate
        daily = national.groupby("date").agg({"gw": "mean", "cf": "mean"}).reset_index()
        peak_daily = national[national["hour"].isin(PEAK_HOURS)].groupby("date")["cf"].mean()
        
        for _, row in daily.iterrows():
            d = row["date"]
            d_str = d.strftime("%Y-%m-%d")
            
            mm_dd = d.strftime("%m-%d")
            c_entry = climo.get(mm_dd, {"all": 0.09, "peak": 0.28})
            
            peak_cf = peak_daily.get(d, 0)
            
            all_rows.append({
                "date": d_str,
                "model": model,
                "total_solar_gw": round(row["gw"], 2),
                "national_cf_pct": round(row["cf"] * 100, 1),
                "national_cf_peak_pct": round(peak_cf * 100, 1),
                "climo_cf_pct": round(c_entry["peak"] * 100, 1), 
                "anomaly_cf_pct": round((peak_cf - c_entry["peak"]) * 100, 1), 
                "drought_flag": 1 if (peak_cf < get_solar_drought_threshold(current_month)) else 0 
            })
            
    if not all_rows: return
    
    df_out = pd.DataFrame(all_rows)
    df_out = df_out[df_out["date"] >= utc_now.date().strftime("%Y-%m-%d")]
    df_out.to_csv(OUTPUT_CSV, index=False)
    
    generate_combined_drought(df_out)

def generate_combined_drought(solar_df):
    """
    Synthesizes wind and solar data into a combined signal.
    """
    utc_now = datetime.now(UTC)
    today_str = utc_now.date().strftime("%Y-%m-%d")
    
    combined = {
        "wind_drought_prob_16d": 0.0,
        "solar_drought_prob_10d": 0.0,
        "combined_drought_today": False,
        "combined_drought_prob_7d": 0.0,
        "combined_drought_days_7d": 0,
        "worst_combined_day": today_str,
        "worst_combined_renewable_cf_pct": 0.0,
        "gas_displacement_loss_gw": 0.0,
        "signal": "NEUTRAL",
        "solar_drought_threshold_cf_pct": round(get_solar_drought_threshold(datetime.now(UTC).month) * 100, 1),
        "timestamp": utc_now.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    try:
        with open(WIND_DROUGHT_JSON, "r") as f:
            wind_json = json.load(f)
        
        wind_csv = pd.read_csv("outputs/wind/wind_power_forecast.csv")
        wind_csv = wind_csv[wind_csv["model"] == "GFS"]
        
        solar_gfs = solar_df[solar_df["model"] == "GFS"]
        merged = pd.merge(wind_csv, solar_gfs, on="date", suffixes=('_w', '_s'))
        merged["combined_cf"] = (merged["total_wind_gw"] + merged["total_solar_gw"]) / (110.0 + TOTAL_SOLAR_GW)
        
        # Drought check: Wind < 35% AND Solar consensus < 25% (2 of 3 models)
        # First, pivot solar to get all models for consensus
        solar_pivoted = solar_df.pivot(index="date", columns="model", values="national_cf_peak_pct")
        def check_solar_consensus(d):
            if d not in solar_pivoted.index: return False
            row = solar_pivoted.loc[d]
            return sum(1 for m in ["GFS", "ECMWF", "ECMWF_AIFS"] if m in row and row[m] < 25.0) >= 2

        merged["solar_consensus"] = merged["date"].apply(check_solar_consensus)
        merged["both_drought"] = (merged["national_cf_pct_w"] < 35.0) & (merged["solar_consensus"])
        
        near_term = merged[merged["date"] <= (utc_now.date() + timedelta(days=6)).strftime("%Y-%m-%d")]
        prob_7d = near_term["both_drought"].mean() if not near_term.empty else 0.0
        days_7d = int(near_term["both_drought"].sum()) if not near_term.empty else 0
        
        today_row = merged[merged["date"] == today_str]
        combined_drought_today = False
        gas_loss = 0.0
        if not today_row.empty:
            combined_drought_today = bool(today_row.iloc[0]["both_drought"])
            w_loss = (today_row.iloc[0]["climo_cf_pct_w"] - today_row.iloc[0]["national_cf_pct_w"]) * 110.0 / 100.0
            
            with open(CLIMO_PATH, "r") as f: c_full = json.load(f)
            mm_dd = utc_now.strftime("%m-%d")
            c_today = c_full.get(mm_dd, {"all": 0.09, "peak": 0.28})
            
            s_loss = (c_today["all"] - (today_row.iloc[0]["national_cf_pct_s"]/100)) * TOTAL_SOLAR_GW
            gas_loss = round(w_loss + s_loss, 1)

        signal = "NEUTRAL"
        if combined_drought_today and prob_7d >= 0.40: 
            signal = "STRONG BULL"
        elif gas_loss > 5.0: 
            signal = "MODERATE BULL"
        elif gas_loss < -5.0: 
            signal = "MODERATE BEAR"
        elif combined_drought_today or prob_7d >= 0.25: 
            signal = "MODERATE BULL"
        
        worst = merged.loc[merged["combined_cf"].idxmin()]
        
        combined.update({
            "wind_drought_prob_16d": wind_json["drought_prob_16d"],
            "solar_drought_prob_10d": round(solar_gfs["drought_flag"].mean(), 2),
            "combined_drought_today": combined_drought_today,
            "combined_drought_prob_7d": round(prob_7d, 2),
            "combined_drought_days_7d": days_7d,
            "worst_combined_day": worst["date"],
            "worst_combined_renewable_cf_pct": round(worst["combined_cf"] * 100, 1),
            "gas_displacement_loss_gw": gas_loss,
            "signal": signal
        })

    except Exception as e:
        logging.error(f"Fallback triggered for combined_drought.json: {e}")
        try:
            with open(WIND_DROUGHT_JSON, "r") as f:
                wj = json.load(f)
                combined["wind_drought_prob_16d"] = wj["drought_prob_16d"]
                combined["solar_drought_prob_10d"] = None
        except: pass

    with open(OUTPUT_JSON, "w") as f:
        json.dump(combined, f, indent=2)

if __name__ == "__main__":
    run_forecast()
