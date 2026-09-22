"""
compare_to_normal.py

FIX (Issue #3): Upgraded to Phase 2 gas-weighted columns.
- Reads gas-weighted normals (us_gas_weighted_normals.csv) if available.
- Computes both simple and GW anomaly columns for full comparison.
- vs_normal.csv now contains: hdd_anomaly (simple) + hdd_anomaly_gw (gas-weighted)
- Seasonal: HDD Nov-Mar, CDD Apr-Sep, TDD (net) for shoulder months Apr/Oct.
"""

import sys
import pandas as pd
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from season_utils import active_metric

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
    if "hdd_normal_10yr" not in normals.columns:
        normals["hdd_normal_10yr"] = normals["hdd_normal"]
    if "cdd_normal_10yr" not in normals.columns:
        normals["cdd_normal_10yr"] = normals["cdd_normal"]
        
    merged = df.merge(
        normals[["month", "day", "hdd_normal", "cdd_normal", "mean_temp_f", "hdd_normal_10yr", "cdd_normal_10yr"]],
        on=["month", "day"],
        how="left"
    )
    
    # [FIX] Issue 1: Handle out-of-range or missing normal dates (e.g. Feb 29 in non-leap year normals)
    # If a date fails to join, we fill with the nearest available normal (forward-fill then back-fill)
    # This prevents null normals for subseasonal horizons or rare dates.
    norm_cols = ["hdd_normal", "cdd_normal", "mean_temp_f", "hdd_normal_10yr", "cdd_normal_10yr"]
    merged[norm_cols] = merged[norm_cols].ffill().bfill()

    # Phase 1: simple anomalies
    merged["hdd"] = merged["hdd"].fillna(merged["tdd"])
    merged["cdd"] = merged["cdd"].fillna(0)
    merged["forecast_cdd"] = merged["cdd"]

    merged["hdd_anomaly"] = merged["hdd"] - merged["hdd_normal"]
    merged["hdd_anomaly_10yr"] = merged["hdd"] - merged["hdd_normal_10yr"]
    merged["cdd_anomaly"] = merged["cdd"] - merged["cdd_normal"]
    merged["cdd_anomaly_10yr"] = merged["cdd"] - merged["cdd_normal_10yr"]
    merged["tdd_anomaly"] = merged["tdd"] - (merged["hdd_normal"] + merged["cdd_normal"])
    merged["tdd_anomaly_10yr"] = merged["tdd"] - (merged["hdd_normal_10yr"] + merged["cdd_normal_10yr"])

    # Phase 2: gas-weighted anomalies
    gw_mode = NORMALS_GW.exists() and "tdd_gw" in df.columns
    if gw_mode:
        normals_gw = pd.read_csv(NORMALS_GW)
        gw_norm_cols = [
            "month", "day",
            "hdd_normal_gw", "hdd_normal_gw_10yr",
            "cdd_normal_gw", "cdd_normal_gw_10yr"
        ]
        # Ensure columns exist in normals_gw
        for col in ["cdd_normal_gw", "cdd_normal_gw_10yr"]:
            if col not in normals_gw.columns:
                normals_gw[col] = normals_gw.get(col.replace("_gw", ""), 0.0)

        merged = merged.merge(
            normals_gw[[c for c in gw_norm_cols if c in normals_gw.columns]],
            on=["month", "day"],
            how="left"
        )
        # Backfill tdd_gw, hdd_gw, cdd_gw if missing
        merged["tdd_gw"] = merged["tdd_gw"].fillna(merged["tdd"])
        if "hdd_gw" not in merged.columns:
            merged["hdd_gw"] = merged["tdd_gw"]
        if "cdd_gw" not in merged.columns:
            merged["cdd_gw"] = 0.0

        merged["hdd_anomaly_gw"] = merged["hdd_gw"] - merged["hdd_normal_gw"]
        merged["hdd_anomaly_gw_10yr"] = merged["hdd_gw"] - merged["hdd_normal_gw_10yr"]
        merged["cdd_anomaly_gw"] = merged["cdd_gw"] - merged["cdd_normal_gw"]
        merged["cdd_anomaly_gw_10yr"] = merged["cdd_gw"] - merged["cdd_normal_gw_10yr"]
        merged["tdd_anomaly_gw"] = merged["tdd_gw"] - (merged["hdd_normal_gw"] + merged["cdd_normal_gw"])
        merged["tdd_anomaly_gw_10yr"] = merged["tdd_gw"] - (merged["hdd_normal_gw_10yr"] + merged["cdd_normal_gw_10yr"])
        print("  [OK] Gas-weighted anomalies (HDD, CDD, TDD for 30yr and 10yr) computed.")
    else:
        merged["hdd_anomaly_gw"] = None
        merged["hdd_anomaly_gw_10yr"] = None
        merged["cdd_anomaly_gw"] = None
        merged["cdd_anomaly_gw_10yr"] = None
        merged["tdd_anomaly_gw"] = None
        merged["tdd_anomaly_gw_10yr"] = None
        print("  [WARN] Gas-weighted anomaly not computed (GW normals or tdd_gw not available).")

    # Dominant anomaly: dynamic load-aware & season-aware
    def dominant_anomaly(row):
        h_val = row.get("hdd_gw") if pd.notna(row.get("hdd_gw")) else row.get("hdd", 0)
        c_val = row.get("cdd_gw") if pd.notna(row.get("cdd_gw")) else row.get("cdd", 0)
        metric = active_metric(row.get("date"), hdd_val=h_val, cdd_val=c_val)
        if metric == "CDD":
            return row.get("cdd_anomaly_gw") if pd.notna(row.get("cdd_anomaly_gw")) else row["cdd_anomaly"]
        if metric == "BOTH":
            return row.get("tdd_anomaly_gw") if pd.notna(row.get("tdd_anomaly_gw")) else row["tdd_anomaly"]
        return row.get("hdd_anomaly_gw") if pd.notna(row.get("hdd_anomaly_gw")) else row["hdd_anomaly"]

    merged["anomaly"] = merged.apply(dominant_anomaly, axis=1)

    # Per-run summary: both simple and GW
    agg_dict = {
        "forecast_hdd_avg":    ("hdd",           "mean"),
        "normal_hdd_avg":      ("hdd_normal",     "mean"),
        "normal_hdd_avg_10yr": ("hdd_normal_10yr", "mean"),
        "forecast_cdd_avg":    ("cdd",           "mean"),
        "normal_cdd_avg":      ("cdd_normal",     "mean"),
        "normal_cdd_avg_10yr": ("cdd_normal_10yr", "mean"),
        "forecast_tdd_avg":    ("tdd",           "mean"),
        "days":                ("tdd",            "count"),
    }
    if gw_mode:
        agg_dict["forecast_hdd_avg_gw"] = ("hdd_gw",        "mean")
        agg_dict["normal_hdd_avg_gw"]   = ("hdd_normal_gw", "mean")
        agg_dict["normal_hdd_avg_gw_10yr"] = ("hdd_normal_gw_10yr", "mean")
        agg_dict["forecast_cdd_avg_gw"] = ("cdd_gw",        "mean")
        agg_dict["normal_cdd_avg_gw"]   = ("cdd_normal_gw", "mean")
        agg_dict["normal_cdd_avg_gw_10yr"] = ("cdd_normal_gw_10yr", "mean")
        agg_dict["forecast_tdd_avg_gw"] = ("tdd_gw",        "mean")

    summary = (
        merged.groupby(["model", "run_id"])
        .agg(**agg_dict)
        .reset_index()
    )

    summary["vs_normal_hdd"] = summary["forecast_hdd_avg"] - summary["normal_hdd_avg"]
    summary["vs_normal_hdd_10yr"] = summary["forecast_hdd_avg"] - summary["normal_hdd_avg_10yr"]
    summary["vs_normal_cdd"] = summary["forecast_cdd_avg"] - summary["normal_cdd_avg"]
    summary["vs_normal_cdd_10yr"] = summary["forecast_cdd_avg"] - summary["normal_cdd_avg_10yr"]
    summary["vs_normal_tdd"] = (summary["forecast_hdd_avg"] + summary["forecast_cdd_avg"]) - (summary["normal_hdd_avg"] + summary["normal_cdd_avg"])
    
    if gw_mode:
        summary["vs_normal_hdd_gw"] = summary["forecast_hdd_avg_gw"] - summary["normal_hdd_avg_gw"]
        summary["vs_normal_hdd_gw_10yr"] = summary["forecast_hdd_avg_gw"] - summary["normal_hdd_avg_gw_10yr"]
        summary["vs_normal_cdd_gw"] = summary["forecast_cdd_avg_gw"] - summary["normal_cdd_avg_gw"]
        summary["vs_normal_cdd_gw_10yr"] = summary["forecast_cdd_avg_gw"] - summary["normal_cdd_avg_gw_10yr"]
        summary["vs_normal_tdd_gw"] = (summary["forecast_hdd_avg_gw"] + summary["forecast_cdd_avg_gw"]) - (summary["normal_hdd_avg_gw"] + summary["normal_cdd_avg_gw"])

    # Signal: dynamically evaluated based on dominant weather regime
    from season_utils import dominant_metric_from_master
    _cur_metric = dominant_metric_from_master(df)
    if _cur_metric == "CDD":
        _sig_col = "vs_normal_cdd_gw" if gw_mode else "vs_normal_cdd"
    elif _cur_metric == "BOTH":
        _sig_col = "vs_normal_tdd_gw" if gw_mode else "vs_normal_tdd"
    else:
        _sig_col = "vs_normal_hdd_gw" if gw_mode else "vs_normal_hdd"

    summary["signal"] = summary[_sig_col].apply(
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
            f"{gw_str}  |  -> {row['signal']}"
        )
    print("--------------------------\n")

    return summary


if __name__ == "__main__":
    compare()
