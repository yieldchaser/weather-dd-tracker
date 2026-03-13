import os
import json
import logging
import time
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
    "GFS":   {"om_name": "gfs_seamless",        "horizon_days": 16, "wind_var": "wind_speed_100m"},
    "ECMWF": {"om_name": "ecmwf_ifs025",        "horizon_days": 10, "wind_var": "wind_speed_100m"},
    "ICON":  {"om_name": "icon_seamless",        "horizon_days": 7,  "wind_var": "wind_speed_80m"},
    "GFS_CFS": {
        "om_name":      "gfs_seamless",
        "endpoint":     "https://ensemble-api.open-meteo.com/v1/ensemble",
        "horizon_days": 35,
        "wind_var":     "wind_speed_100m"
    },
}

PEAK_HOURS    = list(range(13, 21))   # 13:00–20:00 UTC = 8am–3pm Central (demand peak)
OFFPEAK_HOURS = list(range(21, 24)) + list(range(0, 6))  # overnight

BASE_URL = "https://api.open-meteo.com/v1/forecast"
HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
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

def get_wind_drought_threshold(month: int) -> float:
    """
    Wind CF drought threshold varies by season.
    Winter: high wind season, 35% is meaningful low
    Summer: structurally lower wind, 25% is meaningful low
    Shoulder: interpolate
    """
    # Heating season (Nov-Mar): 35%
    if month >= 11 or month <= 3:
        return 0.35
    # Cooling season (Jun-Aug): 25%
    elif 6 <= month <= 8:
        return 0.25
    # Shoulder (Apr-May, Sep-Oct): 30%
    else:
        return 0.30

def get_peak_wind_drought_threshold(month: int) -> float:
    if month >= 11 or month <= 3:
        return 0.30
    elif 6 <= month <= 8:
        return 0.20
    else:
        return 0.25

def build_wind_climatology():
    logging.info("Climatology not found or outdated — starting GFS Historical bootstrap (one-time, ~2 min)")
    end_date = date.today() - timedelta(days=5)
    start_date = end_date - timedelta(days=365 * 5)
    
    params = {
        "latitude":        ",".join(str(n[2]) for n in WIND_NODES),
        "longitude":       ",".join(str(n[3]) for n in WIND_NODES),
        "start_date":      start_date.strftime("%Y-%m-%d"),
        "end_date":        end_date.strftime("%Y-%m-%d"),
        "hourly":          "wind_speed_100m",
        "models":          "gfs_seamless",
        "wind_speed_unit": "ms",
        "timezone":        "UTC"
    }

    try:
        resp = requests.get(HISTORICAL_FORECAST_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.error(f"Failed to fetch historical GFS data: {e}")
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
        
        dfs.append(df[["time", "gw"]])
        
    if not dfs:
        logging.error("No valid ERA5 data parsed.")
        return {}
        
    all_daily = pd.concat(dfs, ignore_index=True)
    national = all_daily.groupby("time")["gw"].sum().reset_index()
    national["cf"] = national["gw"] / TOTAL_INSTALLED_GW
    
    national["date"] = national["time"].dt.date
    national["hour"] = national["time"].dt.hour
    national["mm_dd"] = national["time"].dt.strftime("%m-%d")
    national["period"] = national["hour"].apply(
        lambda h: "peak" if h in PEAK_HOURS else ("offpeak" if h in OFFPEAK_HOURS else "shoulder")
    )

    # Compute climatology for each period
    doy_climo = {}
    for mm_dd, group in national.groupby("mm_dd"):
        doy_climo[mm_dd] = {
            "all":      float(group["cf"].mean()),
            "peak":     float(group[group["period"] == "peak"]["cf"].mean()),
            "offpeak":  float(group[group["period"] == "offpeak"]["cf"].mean()),
            "shoulder": float(group[group["period"] == "shoulder"]["cf"].mean())
        }
    
    os.makedirs(os.path.dirname(CLIMO_PATH), exist_ok=True)
    try:
        with open(CLIMO_PATH, "w") as f:
            json.dump(doy_climo, f, indent=2)
        logging.info(f"Successfully wrote ERA5 Climatology to {CLIMO_PATH}")
    except Exception as e:
        logging.error(f"Failed to write Climatology file: {e}")
        
    return doy_climo

def fetch_forecasts():
    if os.path.exists(CLIMO_PATH):
        try:
            with open(CLIMO_PATH, "r") as f:
                climo_data = json.load(f)
            # Migration check: if old flat schema (MM-DD: float), rebuild
            if climo_data and isinstance(list(climo_data.values())[0], (float, int)):
                logging.info("Old flat climatology detected — rebuilding with peak/offpeak schema")
                os.remove(CLIMO_PATH)
                climo_data = build_wind_climatology()
        except Exception:
            climo_data = build_wind_climatology()
    else:
        climo_data = build_wind_climatology()
            
    if not climo_data:
        logging.error("No climatology data. Exiting.")
        return

    all_rows = []
    gfs_daily_node_gw = {} # {date: [gw1, gw2, ...]} for GFS spread calculation
    current_month = datetime.now().month

    for model, config in MODELS.items():
        logging.info(f"Fetching {model}...")
        
        node_dfs = []
        model_id = config["om_name"]
        target_var = config["wind_var"]
        endpoint = config.get("endpoint", BASE_URL)

        # Batch fetch for all models
        params = {
            "latitude":        ",".join(str(n[2]) for n in WIND_NODES),
            "longitude":       ",".join(str(n[3]) for n in WIND_NODES),
            "hourly":          target_var,
            "wind_speed_unit": "ms",
            "forecast_days":   config["horizon_days"],
            "models":          model_id,
            "timezone":        "UTC"
        }
        
        try:
            resp = requests.get(endpoint, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            if 'resp' in locals() and resp is not None and resp.status_code == 400:
                logging.warning(f"Model {model} fetch failed (400): {resp.text}")
            else:
                logging.error(f"Error fetching {model} ({model_id}): {e}")
            continue
            
        if isinstance(data, dict):
            data = [data]
                
        for i, node in enumerate(WIND_NODES):
            if i >= len(data): break
            node_data = data[i]
            if "hourly" not in node_data:
                if i == 0: logging.warning(f"{model} node {node[0]} missing 'hourly'. {node_data}")
                continue
                
            hourly_data = node_data["hourly"]
            ws = hourly_data.get(target_var, [])
            if not ws or all(v is None for v in ws):
                if i == 0: logging.warning(f"{model} node {node[0]} variable {target_var} empty/null. Available: {list(hourly_data.keys())}")
                continue
            
            times = pd.to_datetime(hourly_data["time"])
            df = pd.DataFrame({"time": times, "ws": ws})
            df = df.dropna()
            
            if df.empty:
                continue
                
            df["cf"] = df["ws"].apply(wind_power_curve)
            df["gw"] = df["cf"] * node[4]
            node_dfs.append(df)

            # Store GFS node data for spatial spread proxy (daily mean)
            if model == "GFS":
                df["date"] = df["time"].dt.date
                daily = df.groupby("date")["gw"].mean()
                for d, gw in daily.items():
                    if d not in gfs_daily_node_gw: gfs_daily_node_gw[d] = []
                    gfs_daily_node_gw[d].append(gw)
            
        if not node_dfs:
            logging.info(f"No valid forecast data for {model} after filtering.")
            continue
        
        logging.info(f"Model {model} successfully parsed for {len(node_dfs)} nodes.")
            
        all_model_data = pd.concat(node_dfs, ignore_index=True)
        national_hourly = all_model_data.groupby("time")["gw"].sum().reset_index()
        national_hourly["cf"] = national_hourly["gw"] / TOTAL_INSTALLED_GW
        national_hourly["date"] = national_hourly["time"].dt.date
        national_hourly["hour"] = national_hourly["time"].dt.hour
        national_hourly["period"] = national_hourly["hour"].apply(
            lambda h: "peak" if h in PEAK_HOURS else ("offpeak" if h in OFFPEAK_HOURS else "shoulder")
        )

        # Aggregate metrics
        daily_sum = national_hourly.groupby("date").agg({
            "gw": "mean",
            "cf": "mean"
        }).reset_index()

        period_metrics = national_hourly.groupby(["date", "period"])["cf"].mean().unstack(fill_value=0)
        
        for _, row in daily_sum.iterrows():
            d = row["date"]
            d_str = d.strftime("%Y-%m-%d")
            mm_dd = d.strftime("%m-%d")
            
            climo_entry = climo_data.get(mm_dd, climo_data.get("02-28", {"all": 0.40}))
            climo_cf = climo_entry.get("all", 0.40)
            
            total_wind_gw = row["gw"]
            national_cf_pct = row["cf"]
            
            # Period-specific CFs
            cf_peak     = period_metrics.loc[d, "peak"] if "peak" in period_metrics.columns else 0
            cf_offpeak  = period_metrics.loc[d, "offpeak"] if "offpeak" in period_metrics.columns else 0
            cf_shoulder = period_metrics.loc[d, "shoulder"] if "shoulder" in period_metrics.columns else 0
            
            anomaly_cf = national_cf_pct - climo_cf
            drought_threshold = get_wind_drought_threshold(current_month)
            drought_flag = 1 if national_cf_pct < drought_threshold else 0
            
            all_rows.append({
                "date": d_str,
                "model": model,
                "total_wind_gw": round(total_wind_gw, 2),
                "national_cf_pct": round(national_cf_pct * 100, 1),
                "national_cf_peak_pct": round(cf_peak * 100, 1),
                "national_cf_offpeak_pct": round(cf_offpeak * 100, 1),
                "national_cf_shoulder_pct": round(cf_shoulder * 100, 1),
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
    
    # Calculate GFS spatial spread
    gfs_spread = {}
    for d, gws in gfs_daily_node_gw.items():
        if len(gws) > 1:
            gfs_spread[d.strftime("%Y-%m-%d")] = round(pd.Series(gws).std(), 2)

    # Drought logic: require >= 2 models consensus for daily flag (exclude GFS_CFS from 2-model consensus for stability if desired, but here we include it)
    daily_model_counts = df_future[df_future["model"] != "GFS_CFS"].groupby("date")["drought_flag"].sum()
    drought_days_all = daily_model_counts[daily_model_counts >= 2].index.tolist()
    
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
        anomaly_today_peak = round(df_today["national_cf_peak_pct"].mean() - (df_today["climo_cf_pct"].mean()), 1) # Simple proxy
        # Better anomaly today peak:
        # We need peak climo.
        mm_dd_today = datetime.now(UTC).date().strftime("%m-%d")
        climo_today = climo_data.get(mm_dd_today, climo_data.get("02-28", {"all": 0.4, "peak": 0.4}))
        peak_climo_pct = climo_today.get("peak", 0.4) * 100
        offpeak_climo_pct = climo_today.get("offpeak", 0.4) * 100
        
        anomaly_today_peak = round(df_today["national_cf_peak_pct"].mean() - peak_climo_pct, 1)
        anomaly_today_offpeak = round(df_today["national_cf_offpeak_pct"].mean() - offpeak_climo_pct, 1)
        
        models_in_drought_today = df_today[df_today["drought_flag"] == 1]["model"].tolist()
        peak_cf_avg = df_today["national_cf_peak_pct"].mean()
        peak_threshold = get_peak_wind_drought_threshold(current_month)
        peak_drought_today = bool(peak_cf_avg < peak_threshold * 100)
    else:
        anomaly_today = 0.0
        anomaly_today_peak = 0.0
        anomaly_today_offpeak = 0.0
        models_in_drought_today = []
        peak_drought_today = False
        
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
        "anomaly_today_peak": float(anomaly_today_peak),
        "anomaly_today_offpeak": float(anomaly_today_offpeak),
        "peak_drought_today": peak_drought_today,
        "models_in_drought_today": models_in_drought_today,
        "model_horizons":   model_horizons,
        "drought_threshold_cf_pct": round(get_wind_drought_threshold(current_month) * 100, 1),
        "peak_drought_threshold_cf_pct": round(get_peak_wind_drought_threshold(current_month) * 100, 1),
        "gfs_spatial_spread": gfs_spread,
        "timestamp":        datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    
    with open(OUTPUT_JSON, "w") as f:
        json.dump(out_json, f, indent=2)

if __name__ == "__main__":
    fetch_forecasts()
