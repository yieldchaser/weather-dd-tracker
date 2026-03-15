"""
build_historical_monthly_charts.py

Purpose:
- Generates Skylar Capital style historical bar charts (e.g., "June PWCDDs - Historical & Forecast")
- Uses EIA historical data for the blue bars (years 1950 to Current Year - 1).
- Uses the live `ecmwf_latest.csv` forecast + daily normals (to pad missing days) for the red "Current Year" bar.
- Automatically generates charts for the Current Month and Next Month.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import date, timedelta
import calendar

# Directories
EIA_DATA_PATH = Path("data/normals/eia_historical_hdd_cdd.csv")
DAILY_NORMALS_PATH = Path("data/normals/us_gas_weighted_normals.csv")
FORECAST_PATH = Path("outputs/ecmwf_latest.csv")
OUTPUT_DIR = Path("outputs/historical_monthly_charts")

def get_season_type(month):
    # Determines if we should chart HDD or CDD based on the month
    if month in [5, 6, 7, 8, 9]:
        return "CDD"
    return "HDD"

def get_historical_normals(df_eia, target_month, target_type, current_year):
    # Filters to the specific month and type, excluding the current year from averages
    df_hist = df_eia[(df_eia["month"] == target_month) & (df_eia["type"] == target_type) & (df_eia["year"] < current_year)].copy()
    if df_hist.empty:
        return df_hist, 0.0, 0.0
        
    df_hist = df_hist.sort_values("year")
    
    # Calculate 10-year and 30-year normal (from the most recent historical years available)
    last_10 = df_hist.tail(10)["value"].mean()
    last_30 = df_hist.tail(30)["value"].mean()
    
    return df_hist, last_10, last_30

def get_current_year_estimate(target_month, target_type, current_year):
    # We build the "Red Bar" by starting with 0.
    # We find all days in the target month.
    # For each day:
    #   if it exists in the active forecast (outputs/ecmwf_latest.csv), use that TDD_GW value
    #   if it does not exist (beyond 15-day range), use the 30-year daily normal (data/normals/us_gas_weighted_normals.csv)
    
    forecast_df = pd.DataFrame()
    if FORECAST_PATH.exists():
        forecast_df = pd.read_csv(FORECAST_PATH)
        forecast_df["date"] = pd.to_datetime(forecast_df["date"]).dt.date
        # Ensure we have the appropriate value column, we prefer tdd_gw or tdd
        val_col = "tdd_gw" if "tdd_gw" in forecast_df.columns else "tdd"
        forecast_dict = dict(zip(forecast_df["date"], forecast_df[val_col]))
    else:
        forecast_dict = {}
        
    normals_df = pd.DataFrame()
    if DAILY_NORMALS_PATH.exists():
        normals_df = pd.read_csv(DAILY_NORMALS_PATH)
        from season_utils import active_metric as _active_metric
        _season = _active_metric(target_month)
        if _season == "CDD":
            val_col_norm = "cdd_normal_gw" if "cdd_normal_gw" in normals_df.columns else "cdd_normal"
        elif _season == "BOTH":
            normals_df = normals_df.copy()
            normals_df["tdd_normal_gw"] = normals_df.get("hdd_normal_gw", 0) + normals_df.get("cdd_normal_gw", 0)
            val_col_norm = "tdd_normal_gw"
        else:
            val_col_norm = "hdd_normal_gw" if "hdd_normal_gw" in normals_df.columns else "hdd_normal"
        norm_dict = dict(zip(zip(normals_df["month"], normals_df["day"]), normals_df[val_col_norm]))
    else:
        norm_dict = {}

    total_value = 0.0
    days_in_month = calendar.monthrange(current_year, target_month)[1]
    
    for d in range(1, days_in_month + 1):
        target_date = date(current_year, target_month, d)
        
        if target_date in forecast_dict and pd.notna(forecast_dict[target_date]):
            val = forecast_dict[target_date]
        else:
            # Fallback to normal
            val = norm_dict.get((target_month, d), 0.0)
            
        total_value += val
        
    return total_value

def build_chart(target_month, current_year, df_eia):
    month_name = calendar.month_name[target_month]
    target_type = get_season_type(target_month)
    
    print(f"\n  Building {month_name} PW{target_type} Chart...")
    
    # Get Historical
    df_hist, norm_10y, norm_30y = get_historical_normals(df_eia, target_month, target_type, current_year)
    
    # Get Current Estimate (Forecast + Normal blending)
    cur_val = get_current_year_estimate(target_month, target_type, current_year)
    
    if df_hist.empty and cur_val == 0:
        print(f"    [WARN] No data available to plot for {month_name}.")
        return
        
    # Combine data for plotting
    years = list(df_hist["year"]) + [current_year]
    values = list(df_hist["value"]) + [cur_val]
    
    # Plotting
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 4))
    
    # Bars
    bar_colors = ['blue'] * len(df_hist) + ['red']
    ax.bar(years, values, color=bar_colors, width=0.5)
    
    # Lines
    if norm_10y > 0:
        ax.axhline(norm_10y, color='gray', linestyle='-', linewidth=2.5, label='10y Normal')
    if norm_30y > 0:
        ax.axhline(norm_30y, color='black', linestyle='-', linewidth=2.5, label='30y Normal')
        
    # Formatting
    ax.set_title(f"{month_name} PW{target_type}s - Historical & Forecast", fontsize=14, pad=15)
    
    # Create proxy artists for the legend
    import matplotlib.patches as mpatches
    blue_patch = mpatches.Patch(color='blue', label=month_name)
    red_patch = mpatches.Patch(color='red', label=f"{current_year} (Live)")
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles=[blue_patch, red_patch] + handles, loc='upper left', ncol=4, frameon=False, bbox_to_anchor=(0.02, 0.98))
    
    # Set x-ticks to be every 3 years like the Skylar chart, inverted
    ax.set_xlim(current_year + 1, min(years) - 1)
    
    tick_years = np.arange(current_year, min(years) - 1, -3)
    ax.set_xticks(tick_years)
    ax.set_xticklabels(tick_years)
    
    # Y-limit dynamic
    max_val = max(values + [norm_10y, norm_30y])
    min_val = min(values + [norm_10y, norm_30y])
    ax.set_ylim(max(0, min_val - 20), max_val + 20)
    
    # Remove borders
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    
    plt.tight_layout()
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{month_name.lower()}_historical_chart.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"    [OK] Saved -> {out_path}")

def main():
    print("\n--- Generating Historical Monthly Charts ---")
    
    if not EIA_DATA_PATH.exists():
         print("  [WARN] EIA Historical Data not found. Run `fetch_historical_eia_normals.py` first.")
         # Create a dummy blank DF so we can test the framework without the API key
         df_eia = pd.DataFrame(columns=["year", "month", "type", "value"])
    else:
         df_eia = pd.read_csv(EIA_DATA_PATH)
         
    # Generate for this month and next month
    today = date.today()
    current_year = today.year
    
    months_to_run = [today.month]
    next_month = today.month + 1 if today.month < 12 else 1
    months_to_run.append(next_month)
    
    for m in months_to_run:
        build_chart(m, current_year, df_eia)

if __name__ == "__main__":
    main()
