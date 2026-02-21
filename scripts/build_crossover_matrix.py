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

def load_normals():
    normals_path = Path("data/normals/us_daily_normals.csv")
    if not normals_path.exists():
        print(" [ERR] Normal file missing.")
        return None
        
    df = pd.read_csv(normals_path)
    
    # We will build a dummy 10-year curve that represents modern warming.
    # Typically 10-yr HDDs are lower than 30-yr HDDs (warmer winters).
    # Typically 10-yr CDDs are higher than 30-yr CDDs (hotter summers).
    # Since we lack precise 10-year data, we apply a 5% penalty to heating, 5% boost to cooling.
    # This simulates climate shift until real 10Y data is provided.
    df["10yr_hdd"] = (df["hdd_normal"] * 0.95).round(1)
    df["30yr_hdd"] = df["hdd_normal"]
    
    df["10yr_cdd"] = (df["cdd_normal"] * 1.05).round(1)
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
