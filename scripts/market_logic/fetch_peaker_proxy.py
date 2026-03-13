"""
fetch_peaker_proxy.py

Calculates gas peaker utilization proxy based on peak vs off-peak hourly ratios.
"""
import pandas as pd
from pathlib import Path

INPUT_FILE = Path("outputs/hourly_grid_data.csv")
OUTPUT_FILE = Path("outputs/peaker_history.csv")

def calculate_peaker_proxy():
    if not INPUT_FILE.exists():
        print(f"[WARN] No hourly data found at {INPUT_FILE}")
        return
        
    try:
        df = pd.read_csv(INPUT_FILE)
        df["period"] = pd.to_datetime(df["period"])
        df["hour"] = df["period"].dt.hour
        df["date"] = df["period"].dt.strftime("%Y-%m-%d")
        
        # Filter to NATIONAL aggregate (sum of all ISOs for that hour)
        # Assuming fetch_live_grid already aggregated this or we sum here.
        # Logic: sum all ISOs per period
        nat = df.groupby(["period", "date", "hour"]).sum(numeric_only=True).reset_index()
        
        PEAK_HOURS = range(7, 23) # 7am-11pm local (coarse approximation across timezones)
        
        daily_rows = []
        for date, group in nat.groupby("date"):
            # Need at least some hours to be valid
            if len(group) < 12: continue
            
            peak = group[group["hour"].isin(PEAK_HOURS)]
            offpeak = group[~group["hour"].isin(PEAK_HOURS)]
            
            if peak.empty or offpeak.empty: continue
            
            p_gas = peak["natural_gas_mw"].mean()
            o_gas = offpeak["natural_gas_mw"].mean()
            p_load = peak["load_mw"].mean()
            o_load = offpeak["load_mw"].mean()
            
            ratio = p_gas / o_gas if o_gas > 0 else 1.0
            proxy = ((p_gas - o_gas) / p_gas * 100) if p_gas > 0 else 0.0
            
            daily_rows.append({
                "date": date,
                "peak_gas_mw": round(p_gas),
                "offpeak_gas_mw": round(o_gas),
                "peak_offpeak_ratio": round(ratio, 2),
                "peak_load_mw": round(p_load),
                "offpeak_load_mw": round(o_load),
                "peaker_proxy_pct": round(proxy, 1)
            })
            
        new_df = pd.DataFrame(daily_rows)
        
        if OUTPUT_FILE.exists():
            old_df = pd.read_csv(OUTPUT_FILE)
            combined = pd.concat([old_df, new_df]).drop_duplicates(subset=["date"], keep="first")
            combined.sort_values("date", inplace=True)
            combined.to_csv(OUTPUT_FILE, index=False)
        else:
            new_df.to_csv(OUTPUT_FILE, index=False)
            
        print(f"[OK] Saved peaker history -> {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"[ERR] peaker_proxy calculation failed: {e}")

if __name__ == "__main__":
    calculate_peaker_proxy()
