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

    merged = df.merge(normals[["month","day","tdd_normal","mean_temp_f"]],
                      on=["month","day"], how="left")

    merged["tdd_anomaly"] = merged["tdd"] - merged["tdd_normal"]

    # Per run summary
    summary = (
        merged.groupby(["model","run_id"])
        .agg(forecast_hdd=("tdd","sum"), normal_hdd=("tdd_normal","sum"))
        .reset_index()
    )
    summary["vs_normal"]  = summary["forecast_hdd"] - summary["normal_hdd"]
    summary["signal"] = summary["vs_normal"].apply(
        lambda x: "BULLISH" if x > 2 else ("BEARISH" if x < -2 else "NEUTRAL")
    )

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)

    print("\n--- FORECAST vs NORMAL ---")
    for _, row in summary.iterrows():
        print(f"{row['model']:6} {row['run_id']}  |  "
              f"Forecast: {row['forecast_hdd']:.1f} HDD  |  "
              f"Normal: {row['normal_hdd']:.1f} HDD  |  "
              f"{row['vs_normal']:+.1f}  →  {row['signal']}")
    print("--------------------------\n")

    return summary

if __name__ == "__main__":
    compare()
