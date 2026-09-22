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
    from season_utils import dominant_metric_from_master
    today = date.today()
    season = active_metric(today)
    dom = dominant_metric_from_master()
    if dom in ("HDD", "CDD"):
        chart_metric = dom
    elif season == "CDD":
        chart_metric = "CDD"
    elif season == "HDD":
        chart_metric = "HDD"
    elif today.month in [4, 5, 6, 7, 8, 9] and today.day < 16:
        chart_metric = "CDD"
    else:
        chart_metric = "HDD"

    # Season window
    if chart_metric == "CDD":
        # Cooling: Apr 1 – Oct 31 (all same pseudo-year 2000)
        metric_key = "cdd_normal"
        pseudo_year_start, start_m, start_d = 2000, 4, 1
        pseudo_year_end, end_m, end_d = 2000, 10, 31
        season_start_real = f"{today.year}-04-01"
        season_end_real = f"{today.year}-10-31"
        y_max = 4000
        season_label = str(today.year)
    else:
        # Heating: Sep 1 – Apr 30 (cross-year: Sep-Dec=year N, Jan-Apr=year N+1)
        metric_key = "hdd_normal"
        pseudo_year_start, start_m, start_d = 2000, 9, 1
        pseudo_year_end, end_m, end_d = 2001, 4, 30
        heat_start_year = today.year - 1 if today.month <= 4 else today.year
        season_start_real = f"{heat_start_year}-09-01"
        season_end_real = f"{heat_start_year + 1}-04-30"
        y_max = 4500
        season_label = f"{heat_start_year}/{heat_start_year + 1}"

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
    
    # Use season-appropriate metric
    if chart_metric == "CDD":
        actuals_df["hdd_value"] = actuals_df["cdd_gw"].fillna(actuals_df["forecast_cdd"] if "forecast_cdd" in actuals_df.columns else 0) if "cdd_gw" in actuals_df.columns else actuals_df["tdd"]
    else:
        actuals_df["hdd_value"] = actuals_df["hdd_gw"].fillna(actuals_df["tdd_gw"] if "tdd_gw" in actuals_df.columns else actuals_df["tdd"]) if "hdd_gw" in actuals_df.columns else actuals_df["tdd"]

    # Build Normal Accumulation Curve from normals file
    if chart_metric == "CDD" and "cdd_normal" not in normals_df.columns:
        print("  [WARN] cdd_normal column missing from normals. Falling back to HDD.")
        metric_key, chart_metric = "hdd_normal", "HDD"

    # Cross-year HDD season (Sep 1 to Apr 30) vs CDD season (Apr 1 to Oct 31)
    if chart_metric == "HDD":
        season_mask = (normals_df["month"] >= 9) | (normals_df["month"] <= 4)
    else:
        season_mask = (normals_df["month"] >= 4) & (normals_df["month"] <= 10)

    season_norms = normals_df[season_mask].copy()
    
    # Map to pseudo dates for plotting
    def get_pseudo_date(row):
        if chart_metric == "HDD":
            yr = 2000 if int(row["month"]) >= 9 else 2001
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

        # Pre-forecast gap: fill with the normal curve but keep it HONEST —
        # no random noise, and rendered as a distinct dashed segment so it
        # is never mistaken for observed weather or forecast output.
        history_dates = pd.date_range(
            start=season_start_real,
            end=season_fcst["date"].min() - pd.Timedelta(days=1)
        ) if not season_fcst.empty else pd.DatetimeIndex([])

        hist_rows = []
        for d in history_dates:
            if chart_metric == "HDD":
                yr = 2000 if d.month >= 9 else 2001
            else:
                yr = 2000
            try:
                pd_date = pd.Timestamp(year=yr, month=d.month, day=d.day)
                val = season_norms.loc[pd_date, metric_key]
            except KeyError:
                continue
            hist_rows.append({"date": d, "hdd_value": val, "kind": "norm_fill"})

        hist_df = pd.DataFrame(hist_rows)
        fcst_part = season_fcst[["date", "hdd_value"]].copy()
        fcst_part["kind"] = "fcst"
        current_season = pd.concat([hist_df, fcst_part]).sort_values("date")

        def to_pseudo(x):
            if chart_metric == "HDD":
                yr = 2000 if x.month >= 9 else 2001
            else:
                yr = 2000
            if x.month == 2 and x.day == 29:
                return pd.Timestamp(year=yr, month=2, day=28)
            return pd.Timestamp(year=yr, month=x.month, day=x.day)

        current_season["pseudo_date"] = current_season["date"].apply(to_pseudo)
        current_season = current_season.sort_values("pseudo_date").set_index("pseudo_date")
        current_season["cumulative_dd"] = current_season["hdd_value"].cumsum()
        fill_part = current_season[current_season["kind"] == "norm_fill"]
        fcst_only = current_season[current_season["kind"] == "fcst"]

        # Chart
        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(10, 8))

        ax.plot(season_norms.index, season_norms["cumulative_norm"], color=COLORS["NORM"],
                linewidth=3, label="NORM", marker="o", markersize=4)
        if not fill_part.empty:
            ax.plot(fill_part.index, fill_part["cumulative_dd"], color="#888888",
                    linestyle="--", linewidth=2, label="Normal fill (pre-forecast)")
        ax.plot(fcst_only.index, fcst_only["cumulative_dd"], color=COLORS["FCST"],
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
