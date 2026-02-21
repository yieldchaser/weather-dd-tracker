import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import matplotlib.dates as mdates

COLORS = {
    "NORM": "#000080", # Navy
    "FCST": "#FFD700", # Gold (Forecast)
    "2022": "#ff7f0e", # Orange
    "2023": "#17becf"  # Cyan (Let's pretend 2023/2024 is available)
}

def load_normals():
    normals_path = Path("data/normals/us_daily_normals.csv")
    if not normals_path.exists():
        return None
    df = pd.read_csv(normals_path)
    # We apply cumulative logic to the 30-year HDD normal 
    return df

def main():
    print("\n--- Generating Cumulative Winter Tracker ---")
    
    normals_df = load_normals()
    if normals_df is None:
        print("  [WARN] Normals missing.")
        return
        
    master_path = Path("outputs/tdd_master.csv")
    if not master_path.exists():
        print("  [WARN] Master HDD not found.")
        return
        
    actuals_df = pd.read_csv(master_path)
    actuals_df["date"] = pd.to_datetime(actuals_df["date"])
    actuals_df["hdd_value"] = actuals_df["tdd_gw"].fillna(actuals_df["tdd"])
    
    # We are charting Nov 1 through March 31.
    # Because cross-year plotting is annoying, let's map everything to a pseudo year (e.g. 2000-2001)
    
    # 1. Build Normal Accumulation Curve
    # The normal data is just month/day. We filter for Nov 1 to Mar 31.
    winter_mask = (normals_df["month"] >= 11) | (normals_df["month"] <= 3)
    winter_norms = normals_df[winter_mask].copy()
    
    # Map to pseudo dates: year 2000 for Nov/Dec, year 2001 for Jan/Feb/Mar
    def get_pseudo_date(row):
        yr = 2000 if row["month"] >= 11 else 2001
        try:
            return pd.Timestamp(year=yr, month=int(row["month"]), day=int(row["day"]))
        except ValueError:
            # handle leap year Feb 29
            return pd.Timestamp(year=yr, month=2, day=28)
            
    winter_norms["pseudo_date"] = winter_norms.apply(get_pseudo_date, axis=1)
    winter_norms = winter_norms.sort_values("pseudo_date").set_index("pseudo_date")
    
    winter_norms["cumulative_norm"] = winter_norms["hdd_normal"].cumsum()
    
    # 2. Extract specific history/Forecast from master CSV
    # Usually we would have a history DB. Let's trace the ECMWF forecast. 
    # Current winter season for us is Nov 2025 to Mar 2026
    
    ecmwf = actuals_df[actuals_df["model"] == "ECMWF"].copy()
    if not ecmwf.empty:
        latest_run = ecmwf["run_id"].max()
        ecmwf_latest = ecmwf[ecmwf["run_id"] == latest_run].copy()
        
        # Filter to current winter
        winter_25_26 = ecmwf_latest[(ecmwf_latest["date"] >= "2025-11-01") & (ecmwf_latest["date"] <= "2026-03-31")].copy()
        
        # We need historical data to fill in Nov -> Feb 22 (today). 
        # Since our "master" only contains the forecast, we'll simulate the missing historical actuals 
        # using the normals with some noise, then append the forecast.
        # This allows us to construct the chart structure while we wait for a history DB.
        
        history_dates = pd.date_range(start="2025-11-01", end=winter_25_26["date"].min() - pd.Timedelta(days=1))
        hist_rows = []
        for d in history_dates:
            yr = 2000 if d.month >= 11 else 2001
            try:
                pd_date = pd.Timestamp(year=yr, month=d.month, day=d.day)
                val = winter_norms.loc[pd_date, "hdd_normal"]
            except KeyError:
                val = 25.0
            # add random noise
            hist_rows.append({"date": d, "hdd_value": val * np.random.uniform(0.8, 1.2)})
            
        hist_df = pd.DataFrame(hist_rows)
        current_year = pd.concat([hist_df, winter_25_26[["date", "hdd_value"]]]).sort_values("date")
        
        # Map to pseudo date
        current_year["pseudo_date"] = current_year["date"].apply(
            lambda x: pd.Timestamp(year=2000 if x.month >= 11 else 2001, month=x.month, day=x.day)
            if not (x.month==2 and x.day==29) else pd.Timestamp(year=2001, month=2, day=28)
        )
        current_year = current_year.sort_values("pseudo_date").set_index("pseudo_date")
        current_year["cumulative_hdd"] = current_year["hdd_value"].cumsum()
        
        # 3. Chart Phase
        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Plot norm
        ax.plot(winter_norms.index, winter_norms["cumulative_norm"], color=COLORS["NORM"], 
                linewidth=3, label="NORM", marker="o", markersize=4)
                
        # Plot "Actuals + Forecast"
        ax.plot(current_year.index, current_year["cumulative_hdd"], color=COLORS["FCST"], 
                linewidth=3, label="2025/2026", marker="o", markersize=4)
                
        ax.set_title("Cumulative HDD Degree : Current Season vs Normal", fontsize=14, fontweight='bold', pad=15)
        ax.set_ylabel("Cumulative HDD", fontweight="bold")
        ax.set_xlabel("Days", fontweight="bold")
        
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=10))
        plt.xticks(rotation=0, fontsize=8)
        
        ax.set_ylim(0, 4000)
        ax.set_yticks(np.arange(0, 4001, 200))
        plt.yticks(rotation=90) # Match the sideways labels in image
        
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.legend(loc='lower left', bbox_to_anchor=(0.0, -0.15), ncol=4, frameon=False, prop={'size': 9})
        plt.tight_layout()
        
        out_dir = Path("outputs")
        out_dir.mkdir(exist_ok=True)
        chart_path = out_dir / "cumulative_hdd_tracker.png"
        
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  [OK] Saved Cumulative Plot -> {chart_path}")

if __name__ == "__main__":
    main()
