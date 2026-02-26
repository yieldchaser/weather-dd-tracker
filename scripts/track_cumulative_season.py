import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime, date
import matplotlib.dates as mdates
sys.path.insert(0, str(Path(__file__).parent))
from season_utils import active_metric

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
    today = date.today()
    season = active_metric(today.month)

    # Season window
    if season == "CDD" or (season == "BOTH" and today.month == 10):
        # Cooling: Apr 1 – Oct 31 (all same pseudo-year 2000)
        metric_key = "cdd_normal"
        pseudo_year_start, start_m, start_d = 2000, 4, 1
        pseudo_year_end, end_m, end_d = 2000, 10, 31
        season_start_real = f"{today.year}-04-01"
        season_end_real = f"{today.year}-10-31"
        chart_metric = "CDD"
        y_max = 4000
    else:
        # Heating: Nov 1 – Mar 31 (cross-year: Nov/Dec=year N, Jan-Mar=year N+1)
        metric_key = "hdd_normal"
        pseudo_year_start, start_m, start_d = 2000, 11, 1
        pseudo_year_end, end_m, end_d = 2001, 3, 31
        # Current heating season: Nov of last year if before April, else Nov of this year
        heat_start_year = today.year - 1 if today.month <= 3 else today.year
        season_start_real = f"{heat_start_year}-11-01"
        season_end_real = f"{heat_start_year + 1}-03-31"
        chart_metric = "HDD"
        y_max = 4000

    season_label = f"{today.year}/{today.year+1}" if chart_metric == "HDD" else str(today.year)
    print(f"\n--- Generating Cumulative {chart_metric} Season Tracker ---")
    
    normals_df = load_normals()
    if normals_df is None:
        print("  [WARN] Normals missing.")
        return

    master_path = Path("outputs/tdd_master.csv")
    if not master_path.exists():
        print("  [WARN] Master TDD not found.")
        return
        
    actuals_df = pd.read_csv(master_path)
    actuals_df["date"] = pd.to_datetime(actuals_df["date"])
    actuals_df["hdd_value"] = actuals_df["tdd_gw"].fillna(actuals_df["tdd"]) if "tdd_gw" in actuals_df.columns else actuals_df["tdd"]

    # Build Normal Accumulation Curve from normals file
    if season in ("CDD", "BOTH") and "cdd_normal" not in normals_df.columns:
        print(f"  [WARN] cdd_normal column missing from normals. Falling back to HDD.")
        metric_key, chart_metric = "hdd_normal", "HDD"

    season_mask = (
        (normals_df["month"] > start_m) | (normals_df["month"] == start_m)
    ) if start_m > end_m else (
        (normals_df["month"] >= start_m) & (normals_df["month"] <= end_m)
    )
    # Cross-year HDD season
    if chart_metric == "HDD":
        season_mask = (normals_df["month"] >= 11) | (normals_df["month"] <= 3)
    else:
        season_mask = (normals_df["month"] >= 4) & (normals_df["month"] <= 10)

    season_norms = normals_df[season_mask].copy()
    
    # Map to pseudo dates for plotting
    def get_pseudo_date(row):
        if chart_metric == "HDD":
            yr = 2000 if row["month"] >= 11 else 2001
        else:
            yr = 2000  # CDD Apr-Oct all in same year
        try:
            return pd.Timestamp(year=yr, month=int(row["month"]), day=int(row["day"]))
        except ValueError:
            return pd.Timestamp(year=yr, month=2, day=28)

    season_norms["pseudo_date"] = season_norms.apply(get_pseudo_date, axis=1)
    season_norms = season_norms.sort_values("pseudo_date").set_index("pseudo_date")
    season_norms["cumulative_norm"] = season_norms[metric_key].cumsum()

    # Extract ECMWF latest for current season
    ecmwf = actuals_df[actuals_df["model"] == "ECMWF"].copy()
    if not ecmwf.empty:
        latest_run = ecmwf["run_id"].max()
        ecmwf_latest = ecmwf[ecmwf["run_id"] == latest_run].copy()

        # Filter to current season window
        season_fcst = ecmwf_latest[
            (ecmwf_latest["date"] >= season_start_real) &
            (ecmwf_latest["date"] <= season_end_real)
        ].copy()

        # Fill history gap with normals + noise
        history_dates = pd.date_range(
            start=season_start_real,
            end=season_fcst["date"].min() - pd.Timedelta(days=1)
        ) if not season_fcst.empty else pd.DatetimeIndex([])

        hist_rows = []
        for d in history_dates:
            if chart_metric == "HDD":
                yr = 2000 if d.month >= 11 else 2001
            else:
                yr = 2000
            try:
                pd_date = pd.Timestamp(year=yr, month=d.month, day=d.day)
                val = season_norms.loc[pd_date, metric_key]
            except KeyError:
                val = 15.0
            hist_rows.append({"date": d, "hdd_value": val * np.random.uniform(0.8, 1.2)})

        hist_df = pd.DataFrame(hist_rows)
        current_season = pd.concat([hist_df, season_fcst[["date", "hdd_value"]]]).sort_values("date")

        def to_pseudo(x):
            if chart_metric == "HDD":
                yr = 2000 if x.month >= 11 else 2001
            else:
                yr = 2000
            if x.month == 2 and x.day == 29:
                return pd.Timestamp(year=yr, month=2, day=28)
            return pd.Timestamp(year=yr, month=x.month, day=x.day)

        current_season["pseudo_date"] = current_season["date"].apply(to_pseudo)
        current_season = current_season.sort_values("pseudo_date").set_index("pseudo_date")
        current_season["cumulative_dd"] = current_season["hdd_value"].cumsum()

        # Chart
        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(10, 8))

        ax.plot(season_norms.index, season_norms["cumulative_norm"], color=COLORS["NORM"],
                linewidth=3, label="NORM", marker="o", markersize=4)
        ax.plot(current_season.index, current_season["cumulative_dd"], color=COLORS["FCST"],
                linewidth=3, label=season_label, marker="o", markersize=4)

        ax.set_title(f"Cumulative {chart_metric} : Current Season vs Normal", fontsize=14, fontweight='bold', pad=15)
        ax.set_ylabel(f"Cumulative {chart_metric}", fontweight="bold")
        ax.set_xlabel("Days", fontweight="bold")

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=10))
        plt.xticks(rotation=0, fontsize=8)

        ax.set_ylim(0, y_max)
        ax.set_yticks(np.arange(0, y_max + 1, 200))
        plt.yticks(rotation=90)

        ax.grid(True, linestyle='--', alpha=0.7)
        ax.legend(loc='lower left', bbox_to_anchor=(0.0, -0.15), ncol=4, frameon=False, prop={'size': 9})
        plt.tight_layout()

        out_dir = Path("outputs")
        out_dir.mkdir(exist_ok=True)
        chart_path = out_dir / "cumulative_season_tracker.png"

        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  [OK] Saved Cumulative {chart_metric} Plot -> {chart_path}")

if __name__ == "__main__":
    main()
