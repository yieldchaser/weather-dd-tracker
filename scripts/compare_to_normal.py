"""
compare_to_normal.py

FIX (Issue #3): Upgraded to Phase 2 gas-weighted columns.
- Reads gas-weighted normals (us_gas_weighted_normals.csv) if available.
- Computes both simple and GW anomaly columns for full comparison.
- vs_normal.csv now contains: hdd_anomaly (simple) + hdd_anomaly_gw (gas-weighted)
"""

import pandas as pd
from pathlib import Path

NORMALS_SIMPLE = Path("data/normals/us_daily_normals.csv")
NORMALS_GW     = Path("data/normals/us_gas_weighted_normals.csv")
MASTER_FILE    = Path("outputs/tdd_master.csv")
OUTPUT_FILE    = Path("outputs/vs_normal.csv")


def compare():
    if not MASTER_FILE.exists():
        print("Master file not found, skipping normals comparison.")
        return

    df      = pd.read_csv(MASTER_FILE, parse_dates=["date"])
    normals = pd.read_csv(NORMALS_SIMPLE)

    df["month"] = df["date"].dt.month
    df["day"]   = df["date"].dt.day

    # Phase 1: merge simple national normals
    merged = df.merge(
        normals[["month", "day", "hdd_normal", "cdd_normal", "mean_temp_f"]],
        on=["month", "day"],
        how="left"
    )

    # HDD anomaly (simple)
    merged["hdd_anomaly"] = merged["tdd"] - merged["hdd_normal"]

    # CDD anomaly from mean_temp
    merged["forecast_cdd"] = merged["mean_temp"].apply(lambda t: max(t - 65, 0))
    merged["cdd_anomaly"]  = merged["forecast_cdd"] - merged["cdd_normal"]

    # Phase 2: gas-weighted anomaly (Issue #3 fix)
    gw_mode = NORMALS_GW.exists() and "tdd_gw" in df.columns
    if gw_mode:
        normals_gw = pd.read_csv(NORMALS_GW)
        merged = merged.merge(
            normals_gw[["month", "day", "hdd_normal_gw"]],
            on=["month", "day"],
            how="left"
        )
        # Backfill tdd_gw from tdd for backward compatibility with old CSVs
        merged["tdd_gw"] = merged["tdd_gw"].fillna(merged["tdd"])
        merged["hdd_anomaly_gw"] = merged["tdd_gw"] - merged["hdd_normal_gw"]
        print("  [OK] Gas-weighted anomaly (hdd_anomaly_gw) computed.")
    else:
        merged["hdd_anomaly_gw"] = None
        print("  [WARN]  Gas-weighted anomaly not computed (GW normals or tdd_gw not available).")

    # Dominant anomaly: CDD in Jun–Aug, HDD otherwise
    def dominant_anomaly(row):
        if row["month"] in [6, 7, 8]:
            return row["cdd_anomaly"]
        return row["hdd_anomaly"]

    merged["anomaly"] = merged.apply(dominant_anomaly, axis=1)

    # Per-run summary: both simple and GW
    agg_dict = {
        "forecast_hdd_avg":    ("tdd",           "mean"),
        "normal_hdd_avg":      ("hdd_normal",     "mean"),
        "forecast_cdd_avg":    ("forecast_cdd",   "mean"),
        "normal_cdd_avg":      ("cdd_normal",     "mean"),
        "days":                ("tdd",            "count"),
    }
    if gw_mode:
        agg_dict["forecast_hdd_avg_gw"] = ("tdd_gw",        "mean")
        agg_dict["normal_hdd_avg_gw"]   = ("hdd_normal_gw", "mean")

    summary = (
        merged.groupby(["model", "run_id"])
        .agg(**agg_dict)
        .reset_index()
    )

    summary["vs_normal_hdd"] = summary["forecast_hdd_avg"] - summary["normal_hdd_avg"]
    summary["vs_normal_cdd"] = summary["forecast_cdd_avg"] - summary["normal_cdd_avg"]
    if gw_mode:
        summary["vs_normal_hdd_gw"] = summary["forecast_hdd_avg_gw"] - summary["normal_hdd_avg_gw"]

    # Signal from simple HDD (baseline)
    summary["signal"] = summary["vs_normal_hdd"].apply(
        lambda x: "BULLISH" if x > 0.5 else ("BEARISH" if x < -0.5 else "NEUTRAL")
    )

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)

    print("\n--- FORECAST vs NORMAL ---")
    for _, row in summary.iterrows():
        gw_str = (f"  GW HDD: {row['forecast_hdd_avg_gw']:.1f} "
                  f"(Normal: {row['normal_hdd_avg_gw']:.1f}, "
                  f"{row['vs_normal_hdd_gw']:+.1f})"
                  if gw_mode else "")
        print(
            f"{row['model']:6} {row['run_id']}  |  "
            f"HDD: {row['forecast_hdd_avg']:.1f} "
            f"(Normal: {row['normal_hdd_avg']:.1f}, {row['vs_normal_hdd']:+.1f})"
            f"{gw_str}  |  → {row['signal']}"
        )
    print("--------------------------\n")

    return summary


if __name__ == "__main__":
    compare()
