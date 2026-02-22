"""
power_burn_proxy.py

Purpose:
- Natural Gas demand in summer is driven strictly by "Power Burn" (electricity generation for A/C).
- Traditional population-based CDD weights underestimate the outsized impact of 
  industrial/heavy cooling regions like Texas (ERCOT) or the Southeast (SERC) which 
  rely heavily on gas-fired peaker plants.
- This script calculates a specialized "Power Burn TDD" directly geared toward 
  physical spot market gas pricing.
"""

import os
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# We establish custom Power Burn Cooling weights.
# Rather than raw population, these represent the percentage of marginal 
# gas-fired electricity deployment that happens per region for every +1 Cooling Degree Day.
POWER_BURN_WEIGHTS = [
    # South Central (ERCOT) - The absolute driver of summer gas volatility
    ("Dallas",       32.78, -96.80, 25.0),
    ("Houston",      29.76, -95.36, 18.0),
    
    # Southeast (SERC) - Heavy prolonged heat, high gas reliance
    ("Atlanta",      33.75, -84.39, 12.0),
    ("Charlotte",    35.23, -80.84,  8.0),
    
    # Mid-Atlantic/Northeast (PJM)
    ("Philadelphia", 39.95, -75.16, 10.0),
    ("New York",     40.71, -74.01,  8.0),
    
    # Midwest (MISO)
    ("Chicago",      41.85, -87.65, 12.0),
    
    # West Coast (CAISO) - Notable but offset by high renewable/solar penetration
    ("Los Angeles",  34.05, -118.24, 7.0),
]

TOTAL_PB_WEIGHT = sum(w for _, _, _, w in POWER_BURN_WEIGHTS)

OUTPUT_DIR = Path("outputs")

def compute_power_burn():
    """
    Since we don't fetch CAISO/Houston explicitly in the general scripts, 
    we will rely on fetching this directly via Open-Meteo for the Power Burn proxy 
    to assure high precision without bogging down the main physics grid parsers.
    """
    import requests
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUTPUT_DIR / "power_burn_cdd_proxy.csv"

    print(f"Fetching specialized Power Burn CDD across {len(POWER_BURN_WEIGHTS)} critical generation hubs...")
    
    url = "https://api.open-meteo.com/v1/forecast"
    all_temps = {}
    
    # Calculate Custom Power Burn CDD (Cooling Degree Days) proxy. Base temp = 65F.
    def celsius_to_f(c): return c * 9 / 5 + 32
    def compute_cdd(f): return max(f - 65.0, 0)

    for city, lat, lon, weight in POWER_BURN_WEIGHTS:
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_mean",
            "temperature_unit": "celsius",
            "forecast_days": 16,
            "models": "gfs_seamless", # Use GFS as standard for broad cooling
            "timezone": "UTC",
        }
        
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            daily = r.json().get("daily", {})
            for d, tc in zip(daily.get("time", []), daily.get("temperature_2m_mean", [])):
                if tc is None: continue
                if d not in all_temps:
                    all_temps[d] = {"weighted_cdd": 0.0, "total_w": 0.0}
                    
                tf = celsius_to_f(tc)
                cdd = compute_cdd(tf)
                all_temps[d]["weighted_cdd"] += cdd * weight
                all_temps[d]["total_w"] += weight
                
        except Exception as e:
            print(f"  [ERR] {city} power burn fetch failed: {e}")
            
    # Process
    rows = []
    run_dt = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    
    for dt_str in sorted(all_temps.keys()):
        stats = all_temps[dt_str]
        if stats["total_w"] > 0:
            avg_cdd = stats["weighted_cdd"] / stats["total_w"]
            rows.append({
                "date": dt_str,
                "power_burn_cdd": round(avg_cdd, 2),
                "run_id": run_dt
            })
            
    df = pd.DataFrame(rows)
    if not df.empty:
        df.to_csv(out_file, index=False)
        print(f"[OK] Power Burn proxy computed for {len(df)} days.")
        print(f"     Saved -> {out_file}")
    else:
        print("[ERR] Could not generate Power Burn proxy.")

if __name__ == "__main__":
    compute_power_burn()
