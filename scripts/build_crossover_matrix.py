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

CACHE_PATH = Path("data/normals/era5_10yr_normals.csv")

def get_10yr_normals():
    """Fetches or loads real 10Y normals (2016-2025) from Open-Meteo Archive."""
    if CACHE_PATH.exists():
        # Check if cache is older than 90 days
        mtime = datetime.fromtimestamp(CACHE_PATH.stat().st_mtime)
        if (datetime.now() - mtime).days < 90:
            print("  [OK] Using cached 10Y normals.")
            return pd.read_csv(CACHE_PATH)

    print("  [INFO] Fetching real 10Y normals (2016-2025) from Open-Meteo...")
    URL = "https://archive-api.open-meteo.com/v1/archive"
    lats = [c[1] for c in DEMAND_CITIES]
    lons = [c[2] for c in DEMAND_CITIES]
    
    params = {
        "latitude": ",".join(map(str, lats)),
        "longitude": ",".join(map(str, lons)),
        "start_date": "2016-01-01",
        "end_date": "2025-12-31",
        "daily": "temperature_2m_mean",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York"
    }

    r = requests.get(URL, params=params, timeout=60)
    if r.status_code != 200:
        print(f"  [ERR] Failed to fetch 10Y data: {r.status_code}")
        return None

    res = r.json()
    if isinstance(res, dict): res = [res]
    
    # Process each city's data
    city_dfs = []
    for i, city_res in enumerate(res):
        name = DEMAND_CITIES[i][0]
        weight = DEMAND_CITIES[i][3]
        daily = city_res.get("daily", {})
        df = pd.DataFrame({
            "date": pd.to_datetime(daily.get("time", [])),
            "temp": daily.get("temperature_2m_mean", []),
            "weight": weight
        })
        city_dfs.append(df)

    if not city_dfs: return None

    # Merge and weight
    master_df = city_dfs[0][["date"]].copy()
    weighted_temp_sum = np.zeros(len(master_df))
    for df in city_dfs:
        weighted_temp_sum += df["temp"].fillna(df["temp"].mean()) * df["weight"].iloc[0]
    
    master_df["mean_temp_gw"] = weighted_temp_sum / TOTAL_WEIGHT
    
    # Group by MM-DD to get daily normals
    master_df["month"] = master_df["date"].dt.month
    master_df["day"] = master_df["date"].dt.day
    
    # Mean temperature per calendar day across 2016-2025
    normals_10y = master_df.groupby(["month", "day"])["mean_temp_gw"].mean().reset_index()
    
    # Compute HDD/CDD on the 10Y normals
    base = 65.0
    normals_10y["hdd_10yr"] = np.maximum(base - normals_10y["mean_temp_gw"], 0).round(2)
    normals_10y["cdd_10yr"] = np.maximum(normals_10y["mean_temp_gw"] - base, 0).round(2)
    
    # Save to cache
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    normals_10y.to_csv(CACHE_PATH, index=False)
    print(f"  [OK] Cached 10Y normals to {CACHE_PATH}")
    return normals_10y

def load_normals():
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
    
    # Create fake dates for plotting (usually we look at Sep-Oct for Fall crossover)
    # The reference image is from Sep 20 to Oct 10 
    fall_df = df[(df["month"] == 9) | (df["month"] == 10)].copy()
    
    # Add a pseudo date column for the current leap year to make plotting easy
    fall_df["date"] = pd.to_datetime(f"2024-" + fall_df["month"].astype(str) + "-" + fall_df["day"].astype(str))
    
    # Filter bounds to match reference
    target_start = pd.to_datetime("2024-09-20")
    target_end = pd.to_datetime("2024-10-15")
    
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
    
    ax.plot(plot_df.index, plot_df["30yr_hdd"], color=COLORS["10Y_HDD"], linewidth=2.5, label="ng_hdd_30yr")
    ax.plot(plot_df.index, plot_df["10yr_hdd"], color=COLORS["30Y_HDD"], linewidth=2.5, label="ng_hdd_10yr")
    
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
