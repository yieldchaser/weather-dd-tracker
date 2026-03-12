import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime, date, timedelta, UTC

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

WIND_NODES = [
    # (name, iso, lat, lon, installed_gw)
    ("W_TX_PERMIAN",    "ERCOT",  31.5, -102.5, 18.0),
    ("W_TX_PANHANDLE",  "ERCOT",  35.2, -101.8, 14.0),
    ("TX_COAST",        "ERCOT",  27.8,  -97.4,  6.0),
    ("OK_CENTRAL",      "SPP",    35.8,  -97.5, 12.0),
    ("KS_PLAINS",       "SPP",    38.3,  -98.2,  9.0),
    ("NE_PLAINS",       "SPP",    41.5,  -99.0,  5.0),
    ("IA_CENTRAL",      "MISO",   42.0,  -93.5, 12.0),
    ("MN_SOUTHWEST",    "MISO",   44.2,  -95.8,  5.5),
    ("SD_CENTRAL",      "MISO",   44.0, -100.2,  4.5),
    ("IN_NORTH",        "MISO",   40.8,  -86.5,  4.0),
    ("IL_CENTRAL",      "MISO",   40.5,  -89.0,  3.5),
    ("WY_SOUTH",        "WECC",   41.8, -106.5,  4.0),
    ("CA_ALTAMONT",     "WECC",   37.7, -121.6,  2.5),
    ("CA_TEHACHAPI",    "WECC",   35.1, -118.4,  3.0),
    ("PJM_OHIO",        "PJM",    40.5,  -83.0,  3.0),
]

TOTAL_INSTALLED_GW = sum(n[4] for n in WIND_NODES)  # ~110 GW represented

MODELS = {
    "GFS":   {"om_name": "gfs_seamless",      "horizon_days": 16, "api": "forecast"},
    "GEFS":  {"om_name": "gfs_seamless",      "horizon_days": 16, "api": "ensemble"},
    "ECMWF": {"om_name": "ecmwf_ifs025",      "horizon_days": 10, "api": "forecast"},
    "ICON":  {"om_name": "icon_seamless",     "horizon_days": 7,  "api": "forecast"},
}

BASE_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
CLIMO_PATH = "outputs/wind/wind_climo_30d.json"
OUTPUT_CSV = "outputs/wind/wind_power_forecast.csv"
OUTPUT_JSON = "outputs/wind/drought.json"

def wind_power_curve(ws_ms):
    """
    Returns capacity factor (0.0–1.0) for wind speed in m/s.
    IEC Class II: cut-in 3 m/s, rated 12.5 m/s, cut-out 25 m/s.
    Cubic interpolation between cut-in and rated.
    """
    CUT_IN  = 3.0
    RATED   = 12.5
    CUT_OUT = 25.0
    if ws_ms < CUT_IN or ws_ms >= CUT_OUT:
        return 0.0
    if ws_ms >= RATED:
        return 1.0
    return ((ws_ms - CUT_IN) / (RATED - CUT_IN)) ** 3

def build_wind_climatology():
    logging.info("Climatology not found or outdated — starting ERA5 bootstrap (one-time, ~2 min)")
    end_date = date.today() - timedelta(days=5)
    start_date = end_date - timedelta(days=365 * 5)
    
    params = {
        "latitude":        ",".join(str(n[2]) for n in WIND_NODES),
        "longitude":       ",".join(str(n[3]) for n in WIND_NODES),
        "start_date":      start_date.strftime("%Y-%m-%d"),
        "end_date":        end_date.strftime("%Y-%m-%d"),
        "hourly":          "wind_speed_100m",
        "wind_speed_unit": "ms",
        "timezone":        "UTC"
    }

    try:
        resp = requests.get(ARCHIVE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.error(f"Failed to fetch ERA5 data: {e}")
        return {}

    if isinstance(data, dict):
        if "hourly" in data:
            data = [data]
            
    dfs = []
    for i, node in enumerate(WIND_NODES):
        if i >= len(data): break
        node_data = data[i]
        if "hourly" not in node_data:
            continue
            
        times = pd.to_datetime(node_data["hourly"]["time"])
        ws = node_data["hourly"].get("wind_speed_100m", [])
        
        df = pd.DataFrame({"time": times, "ws": ws})
        df = df.dropna()
        if df.empty:
            continue
        
        df["cf"] = df["ws"].apply(wind_power_curve)
        df["gw"] = df["cf"] * node[4]
        df["date"] = df["time"].dt.date
        
        daily = df.groupby("date")["gw"].mean().reset_index()
        dfs.append(daily)
        
    if not dfs:
        logging.error("No valid ERA5 data parsed.")
        return {}
        
    all_daily = pd.concat(dfs, ignore_index=True)
    national_gw = all_daily.groupby("date")["gw"].sum().reset_index()
    national_gw["cf"] = national_gw["gw"] / TOTAL_INSTALLED_GW
    
    national_gw["mm_dd"] = pd.to_datetime(national_gw["date"]).dt.strftime("%m-%d")
    doy_climo = national_gw.groupby("mm_dd")["cf"].mean().to_dict()
    
    os.makedirs(os.path.dirname(CLIMO_PATH), exist_ok=True)
    try:
        with open(CLIMO_PATH, "w") as f:
            json.dump(doy_climo, f, indent=2)
        logging.info(f"Successfully wrote ERA5 Climatology to {CLIMO_PATH}")
    except Exception as e:
        logging.error(f"Failed to write Climatology file: {e}")
        
    return doy_climo

def fetch_forecasts():
    if not os.path.exists(CLIMO_PATH):
        climo_data = build_wind_climatology()
    else:
        try:
            with open(CLIMO_PATH, "r") as f:
                climo_data = json.load(f)
        except Exception:
            climo_data = build_wind_climatology()
            
    if not climo_data:
        logging.error("No climatology data. Exiting.")
        return

    all_rows = []
    
    for model, config in MODELS.items():
        logging.info(f"Fetching {model}...")
        
        api_base = "https://ensemble-api.open-meteo.com/v1/ensemble" if config.get("api") == "ensemble" else BASE_URL
        target_var = "wind_speed_80m" if model == "ICON" else "wind_speed_100m"
        
        node_dfs = []
        
        if model == "GEFS":
            # User specifically requested sequential single-location fetching for GEFS
            # due to multi-location batching issues/limits
            import time
            for i, node in enumerate(WIND_NODES):
                params = {
                    "latitude":        node[2],
                    "longitude":       node[3],
                    "hourly":          target_var,
                    "wind_speed_unit": "ms",
                    "forecast_days":   16,
                    "models":          config["om_name"],
                    "timezone":        "UTC"
                }
                try:
                    resp = requests.get(api_base, params=params)
                    resp.raise_for_status()
                    node_data = resp.json()
                    
                    if "hourly" not in node_data:
                        continue
                        
                    hourly_data = node_data["hourly"]
                    ws = hourly_data.get(target_var, [])
                    if not ws or all(v is None for v in ws):
                        continue
                        
                    times = pd.to_datetime(hourly_data["time"])
                    df = pd.DataFrame({"time": times, "ws": ws})
                    df = df.dropna()
                    if df.empty: continue
                        
                    df["cf"] = df["ws"].apply(wind_power_curve)
                    df["gw"] = df["cf"] * node[4]
                    df["date"] = df["time"].dt.date
                    daily = df.groupby("date")["gw"].mean().reset_index()
                    node_dfs.append(daily)
                    time.sleep(0.5)  # Small delay between calls
                except Exception as e:
                    logging.info(f"GEFS sequential fetch failed for node {node[0]}: {e}")
                    
        else:
            # Batch fetch for all other models
            params = {
                "latitude":        ",".join(str(n[2]) for n in WIND_NODES),
                "longitude":       ",".join(str(n[3]) for n in WIND_NODES),
                "hourly":          target_var,
                "wind_speed_unit": "ms",
                "forecast_days":   16,
                "models":          config["om_name"],
                "timezone":        "UTC"
            }
            
            try:
                resp = requests.get(api_base, params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                if resp and resp.status_code == 400:
                    logging.info(f"Model {model} dropped gracefully or encountered provider limit. {e}")
                else:
                    logging.error(f"Error fetching {model}: {e}")
                continue
                
            if isinstance(data, dict):
                if "hourly" in data:
                    data = [data]
                    
            for i, node in enumerate(WIND_NODES):
                if i >= len(data): break
                node_data = data[i]
                if "hourly" not in node_data:
                    continue
                    
                hourly_data = node_data["hourly"]
                ws = hourly_data.get(target_var, [])
                if not ws or all(v is None for v in ws):
                    continue
                
                times = pd.to_datetime(hourly_data["time"])
                df = pd.DataFrame({"time": times, "ws": ws})
                df = df.dropna()
                
                if df.empty:
                    continue
                    
                df["cf"] = df["ws"].apply(wind_power_curve)
                df["gw"] = df["cf"] * node[4]
                df["date"] = df["time"].dt.date
                
                daily = df.groupby("date")["gw"].mean().reset_index()
                node_dfs.append(daily)
            
        if not node_dfs:
            logging.info(f"No valid forecast data for {model}.")
            continue
            
        all_daily = pd.concat(node_dfs, ignore_index=True)
        national = all_daily.groupby("date")["gw"].sum().reset_index()
        
        for _, row in national.iterrows():
            d_str = row["date"].strftime("%Y-%m-%d")
            total_wind_gw = row["gw"]
            national_cf_pct = total_wind_gw / TOTAL_INSTALLED_GW
            mm_dd = row["date"].strftime("%m-%d")
            
            climo_cf = climo_data.get(mm_dd, climo_data.get("02-28", 0.40))
            
            anomaly_cf = national_cf_pct - climo_cf
            drought_flag = 1 if national_cf_pct < 0.35 else 0
            
            all_rows.append({
                "date": d_str,
                "model": model,
                "total_wind_gw": round(total_wind_gw, 2),
                "national_cf_pct": round(national_cf_pct * 100, 1),
                "climo_cf_pct": round(climo_cf * 100, 1),
                "anomaly_cf_pct": round(anomaly_cf * 100, 1),
                "drought_flag": drought_flag
            })
            
    if not all_rows:
        logging.warning("No forecast data generated.")
        return
        
    df_out = pd.DataFrame(all_rows)
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df_out.to_csv(OUTPUT_CSV, index=False)
    
    today_str = datetime.now(UTC).date().strftime("%Y-%m-%d")
    df_future = df_out[df_out["date"] >= today_str]
    
    if df_future.empty:
        logging.warning("No future days found for drought logic.")
        return
    
    daily_flags = df_future.groupby("date")["drought_flag"].mean()
    drought_days_all = daily_flags[daily_flags >= 0.5].index.tolist()
    
    end_16d = (datetime.now(UTC).date() + timedelta(days=15)).strftime("%Y-%m-%d")
    end_7d  = (datetime.now(UTC).date() + timedelta(days=6)).strftime("%Y-%m-%d")
    
    drought_days_16d = len([d for d in drought_days_all if d <= end_16d])
    drought_days_7d  = len([d for d in drought_days_all if d <= end_7d])
    
    drought_prob_16d = round(drought_days_16d / 16.0, 2)
    drought_prob_7d  = round(drought_days_7d / 7.0, 2)
    
    worst_idx = df_future["national_cf_pct"].idxmin()
    worst_row = df_future.loc[worst_idx]
    
    df_today = df_future[df_future["date"] == today_str]
    if not df_today.empty:
        anomaly_today = round(df_today["anomaly_cf_pct"].mean(), 1)
        models_in_drought_today = df_today[df_today["drought_flag"] == 1]["model"].tolist()
    else:
        anomaly_today = 0.0
        models_in_drought_today = []
        
    model_horizons = {k: v["horizon_days"] for k, v in MODELS.items()}
    
    out_json = {
        "drought_prob_16d": drought_prob_16d,
        "drought_prob_7d":  drought_prob_7d,
        "drought_days_16d": drought_days_16d,
        "drought_days_7d":  drought_days_7d,
        "worst_day":        worst_row["date"],
        "worst_cf_pct":     float(worst_row["national_cf_pct"]),
        "worst_anomaly_cf_pct": float(worst_row["anomaly_cf_pct"]),
        "worst_model":      worst_row["model"],
        "anomaly_today":    float(anomaly_today),
        "models_in_drought_today": models_in_drought_today,
        "model_horizons":   model_horizons,
        "timestamp":        datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    
    with open(OUTPUT_JSON, "w") as f:
        json.dump(out_json, f, indent=2)

if __name__ == "__main__":
    fetch_forecasts()
