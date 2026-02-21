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

    # Merge both hdd_normal and cdd_normal
    merged = df.merge(
        normals[["month", "day", "hdd_normal", "cdd_normal", "mean_temp_f"]],
        on=["month", "day"],
        how="left"
    )

    # HDD anomaly: forecast HDD vs normal HDD
    merged["hdd_anomaly"] = merged["tdd"] - merged["hdd_normal"]

    # CDD anomaly: compute forecast CDD from mean_temp, compare to cdd_normal
    # CDD = max(mean_temp - 65, 0)
    merged["forecast_cdd"] = merged["mean_temp"].apply(lambda t: max(t - 65, 0))
    merged["cdd_anomaly"]  = merged["forecast_cdd"] - merged["cdd_normal"]

    # Dominant anomaly: use CDD in summer (Jun–Aug), HDD otherwise
    def dominant_anomaly(row):
        if row["month"] in [6, 7, 8]:
            return row["cdd_anomaly"]
        return row["hdd_anomaly"]

    merged["anomaly"] = merged.apply(dominant_anomaly, axis=1)

    # Per run summary using AVERAGES not sums
    summary = (
        merged.groupby(["model", "run_id"])
        .agg(
            forecast_hdd_avg=("tdd", "mean"),
            normal_hdd_avg=("hdd_normal", "mean"),
            forecast_cdd_avg=("forecast_cdd", "mean"),
            normal_cdd_avg=("cdd_normal", "mean"),
            days=("tdd", "count")
        )
        .reset_index()
    )

    summary["vs_normal_hdd"] = summary["forecast_hdd_avg"] - summary["normal_hdd_avg"]
    summary["vs_normal_cdd"] = summary["forecast_cdd_avg"] - summary["normal_cdd_avg"]

    # Signal: BULLISH = more heating demand than normal, BEARISH = less
    summary["signal"] = summary["vs_normal_hdd"].apply(
        lambda x: "BULLISH" if x > 0.5 else ("BEARISH" if x < -0.5 else "NEUTRAL")
    )

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)

    print("\n--- FORECAST vs NORMAL ---")
    for _, row in summary.iterrows():
        print(
            f"{row['model']:6} {row['run_id']}  |  "
            f"HDD Avg: {row['forecast_hdd_avg']:.1f} (Normal: {row['normal_hdd_avg']:.1f}, {row['vs_normal_hdd']:+.1f})  |  "
            f"CDD Avg: {row['forecast_cdd_avg']:.1f} (Normal: {row['normal_cdd_avg']:.1f}, {row['vs_normal_cdd']:+.1f})  |  "
            f"→ {row['signal']}"
        )
    print("--------------------------\n")

    return summary


if __name__ == "__main__":
    compare()

