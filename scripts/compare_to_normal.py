import pandas as pd
from pathlib import Path

NORMALS_FILE = Path("data/normals/us_daily_normals.csv")
MASTER_FILE  = Path("outputs/tdd_master.csv")
OUTPUT_FILE  = Path("outputs/vs_normal.csv")

def compare():
    if not MASTER_FILE.exists():
        print("Master file not found, skipping normals comparison.")
        return

    df = pd.read_csv(MASTER_FILE, parse_dates=["date"])
    normals = pd.read_csv(NORMALS_FILE)

    df["month"] = df["date"].dt.month
    df["day"]   = df["date"].dt.day

    merged = df.merge(normals[["month","day","hdd_normal","mean_temp_f"]],
                      on=["month","day"], how="left")

    merged["tdd_anomaly"] = merged["tdd"] - merged["hdd_normal"]

    # Per run summary using AVERAGES not sums
    summary = (
        merged.groupby(["model","run_id"])
        .agg(
            forecast_hdd_avg=("tdd","mean"),
            normal_hdd_avg=("tdd_normal","mean"),
            days=("tdd","count")
        )
        .reset_index()
    )
    summary["vs_normal"] = summary["forecast_hdd_avg"] - summary["normal_hdd_avg"]
    summary["signal"] = summary["vs_normal"].apply(
        lambda x: "BULLISH" if x > 0.5 else ("BEARISH" if x < -0.5 else "NEUTRAL")
    )

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)

    print("\n--- FORECAST vs NORMAL ---")
    for _, row in summary.iterrows():
        print(f"{row['model']:6} {row['run_id']}  |  "
              f"Avg: {row['forecast_hdd_avg']:.1f} HDD/day  |  "
              f"Normal: {row['normal_hdd_avg']:.1f} HDD/day  |  "
              f"{row['vs_normal']:+.1f}  →  {row['signal']}")
    print("--------------------------\n")

    return summary

if __name__ == "__main__":
    compare()
