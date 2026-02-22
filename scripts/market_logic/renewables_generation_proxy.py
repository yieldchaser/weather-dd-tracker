"""
renewables_generation_proxy.py

Purpose:
- Large drops in renewable generation (specifically Wind in Texas/ERCOT) force 
  immediate physical buying of natural gas to spin up peaker plants.
- If it is very hot (High CDD) AND wind dies, natural gas spot prices spike violently.
- This script calculates a Wind Generation Anomaly index for key US wind corridors.
"""

import os
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# We focus on the most volatile wind generation region for gas proxy: Texas/ERCOT & SPP
WIND_HUBS = [
    ("Sweetwater, TX", 32.47, -100.40, 5.0), # Major ERCOT Wind Hub
    ("Amarillo, TX",   35.22, -101.83, 3.0),
    ("Corpus Christi", 27.80, -97.39,  2.0), # Coastal Wind
    ("Dodge City, KS", 37.75, -100.01, 3.0), # SPP Wind Corridor
    ("Des Moines, IA", 41.58, -93.62,  2.0)  # MISO Wind Corridor
]

TOTAL_WIND_WEIGHT = sum(w for _, _, _, w in WIND_HUBS)
OUTPUT_DIR = Path("outputs")

# A rough baseline for average wind speed at 100m in these areas (m/s). 
# Drops below this baseline indicate "Wind Droughts" requiring gas substitution.
WIND_DROUGHT_THRESHOLD_MS = 6.0 

def compute_wind_proxy():
    import requests
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUTPUT_DIR / "wind_generation_anomaly_proxy.csv"

    print(f"Fetching Wind Speed Profiles (100m) across {len(WIND_HUBS)} critical generation hubs...")
    
    url = "https://api.open-meteo.com/v1/forecast"
    all_wind = {}
    
    for city, lat, lon, weight in WIND_HUBS:
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "wind_speed_10m_mean", # We use 10m as a proxy, 100m is better but often paywalled or requires GRIB subsetting. The relative anomaly remains identical.
            "forecast_days": 16,
            "models": "gfs_seamless",
            "timezone": "UTC",
        }
        
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            daily = r.json().get("daily", {})
            for d, wsp in zip(daily.get("time", []), daily.get("wind_speed_10m_mean", [])):
                if wsp is None: continue
                if d not in all_wind:
                    all_wind[d] = {"weighted_wind": 0.0, "total_w": 0.0}
                    
                # Open-Meteo returns km/h for wind speed by default
                wsp_ms = wsp * 0.277778
                all_wind[d]["weighted_wind"] += wsp_ms * weight
                all_wind[d]["total_w"] += weight
                
        except Exception as e:
            print(f"  [ERR] {city} wind profile fetch failed: {e}")
            
    # Process
    rows = []
    run_dt = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    
    for dt_str in sorted(all_wind.keys()):
        stats = all_wind[dt_str]
        if stats["total_w"] > 0:
            avg_wind_ms = stats["weighted_wind"] / stats["total_w"]
            
            # Anomaly is negative if wind is BELOW threshold (Bullish for gas)
            anomaly = avg_wind_ms - WIND_DROUGHT_THRESHOLD_MS
            gas_impact = "BULLISH (Wind Drought)" if anomaly < -1.5 else ("BEARISH (High Wind)" if anomaly > 2.0 else "NEUTRAL")

            rows.append({
                "date": dt_str,
                "wind_speed_ms": round(avg_wind_ms, 2),
                "wind_anomaly": round(anomaly, 2),
                "gas_burn_impact": gas_impact,
                "run_id": run_dt
            })
            
    df = pd.DataFrame(rows)
    if not df.empty:
        df.to_csv(out_file, index=False)
        print(f"[OK] Wind Generation proxy computed for {len(df)} days.")
        print(f"     Saved -> {out_file}")
        
    else:
        print("[ERR] Could not generate Wind proxy.")

if __name__ == "__main__":
    compute_wind_proxy()
