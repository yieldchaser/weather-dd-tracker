import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime

# Define standard colors for charts
COLORS = {
    "10Y_CDD": "#d62728", # red
    "30Y_CDD": "#ff7f0e", # orange
    "10Y_HDD": "#1f77b4", # blue
    "30Y_HDD": "#aec7e8", # light blue
}

import requests
from demand_constants import DEMAND_CITIES, TOTAL_WEIGHT
from om_batch_fetch import fetch_era5_cities_batch

CACHE_PATH = Path("data/normals/era5_10yr_normals.csv")

def get_10yr_normals():
    """10-year normal (last 10 complete years) from the shared normals file,
    which build_normals_from_history.py keeps on the live 79-city index and
    rolls each January. Returns month, day, mean_temp_gw, hdd_10yr, cdd_10yr.
    The cached ERA5 download (fixed at 2016-2025) is only a fallback."""
    gw_path = Path("data/normals/us_gas_weighted_normals.csv")
    if gw_path.exists():
        df = pd.read_csv(gw_path)
        if {"mean_temp_gw_10yr", "hdd_normal_gw_10yr", "cdd_normal_gw_10yr"} <= set(df.columns):
            return df[["month", "day"]].assign(mean_temp_gw=df["mean_temp_gw_10yr"],
                                               hdd_10yr=df["hdd_normal_gw_10yr"], cdd_10yr=df["cdd_normal_gw_10yr"])
    if CACHE_PATH.exists():
        print("  [WARN] Using cached 10Y normals (fallback).")
        return pd.read_csv(CACHE_PATH)
    return None


def load_normals():
    gw_path = Path("data/normals/us_gas_weighted_normals.csv")
    if gw_path.exists():
        df = pd.read_csv(gw_path)
        df["30yr_hdd"] = df["hdd_normal_gw"]
        df["30yr_cdd"] = df["cdd_normal_gw"]
        df["10yr_hdd"] = df.get("hdd_normal_gw_10yr", df["hdd_normal_gw"])
        df["10yr_cdd"] = df.get("cdd_normal_gw_10yr", df["cdd_normal_gw"])
        return df

    normals_path = Path("data/normals/us_daily_normals.csv")
    if not normals_path.exists():
        print(" [ERR] Normal file missing.")
        return None
        
    df = pd.read_csv(normals_path)
    
    # Get real 10Y data
    df_10y = get_10yr_normals()
    if df_10y is not None:
        # Merge real 10Y data into the 30Y dataframe
        df = df.merge(df_10y[["month", "day", "hdd_10yr", "cdd_10yr"]], on=["month", "day"], how="left")
        df["10yr_hdd"] = df["hdd_10yr"]
        df["10yr_cdd"] = df["cdd_10yr"]
    else:
        # Fallback to 5% approximation if fetch fails
        print("  [WARN] Falling back to 5% approximation for 10Y normals.")
        df["10yr_hdd"] = (df["hdd_normal"] * 0.95).round(1)
        df["10yr_cdd"] = (df["cdd_normal"] * 1.05).round(1)
    
    df["30yr_hdd"] = df["hdd_normal"]
    df["30yr_cdd"] = df["cdd_normal"]
    
    return df

def main():
    print("\n--- Generating Seasonal Crossover Tracker ---")
    df = load_normals()
    if df is None: return
    
    # Use current calendar year dynamically for Fall crossover window (Sep 20 to Oct 15)
    fall_df = df[(df["month"] == 9) | (df["month"] == 10)].copy()
    current_year = datetime.now().year
    
    # Add a date column for the current year
    fall_df["date"] = pd.to_datetime(fall_df.apply(lambda r: f"{current_year}-{int(r['month']):02d}-{int(r['day']):02d}", axis=1))
    
    # Filter bounds to match reference (Sep 20 to Oct 15)
    target_start = pd.to_datetime(f"{current_year}-09-20")
    target_end = pd.to_datetime(f"{current_year}-10-15")
    
    plot_df = fall_df[(fall_df["date"] >= target_start) & (fall_df["date"] <= target_end)].copy()
    plot_df = plot_df.set_index("date")
    
    # Output CSV Data Structure
    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)
    csv_path = out_dir / "seasonal_crossover.csv"
    
    plot_df[["30yr_hdd", "10yr_hdd", "30yr_cdd", "10yr_cdd"]].to_csv(csv_path)
    print(f"  [OK] Saved Crossover Matrix -> {csv_path}")
    
    # Now Chart It (Phase 5)
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.plot(plot_df.index, plot_df["30yr_hdd"], color=COLORS["30Y_HDD"], linewidth=2.5, label="ng_hdd_30yr")
    ax.plot(plot_df.index, plot_df["10yr_hdd"], color=COLORS["10Y_HDD"], linewidth=2.5, label="ng_hdd_10yr")
    
    ax.plot(plot_df.index, plot_df["30yr_cdd"], color=COLORS["30Y_CDD"], linewidth=2.5, label="pop_cdd_30yr")
    ax.plot(plot_df.index, plot_df["10yr_cdd"], color=COLORS["10Y_CDD"], linewidth=2.5, label="pop_cdd_10yr")
    
    ax.set_title("Seasonal Degree Day Crossover", fontsize=18, fontweight='bold', pad=20)
    
    # Formatting X Axis
    import matplotlib.dates as mdates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    plt.xticks(rotation=45)
    
    ax.set_ylim(0, 8)
    
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.2), ncol=4, frameon=False)
    plt.tight_layout()
    
    chart_path = out_dir / "crossover_chart.png"
    plt.savefig(chart_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  [OK] Saved Crossover Chart -> {chart_path}")

if __name__ == "__main__":
    main()
