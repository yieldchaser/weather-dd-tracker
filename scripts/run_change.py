"""
run_change.py

FIX (Issue #4): Now computes run-to-run change for both tdd (simple)
and tdd_gw (gas-weighted) when available in tdd_master.csv.
Both columns are written to outputs/run_change.csv.
"""

import os
import pandas as pd

MASTER = "outputs/tdd_master.csv"
OUTPUT = "outputs/run_change.csv"


def compute_run_changes():
    df = pd.read_csv(MASTER)
    df = df[df["run_id"].notna()]

    gw_mode = "tdd_gw" in df.columns

    # Compute Average degree days per run (normalized)
    agg = {"tdd": "mean"}
    if "hdd" in df.columns: agg["hdd"] = "mean"
    if "cdd" in df.columns: agg["cdd"] = "mean"
    if gw_mode:
        agg["tdd_gw"] = "mean"
        if "hdd_gw" in df.columns: agg["hdd_gw"] = "mean"
        if "cdd_gw" in df.columns: agg["cdd_gw"] = "mean"

    run_totals = (
        df.groupby(["model", "run_id"])
        .agg(**{k: (k, v) for k, v in agg.items()})
        .reset_index()
        .sort_values(["model", "run_id"])
        .reset_index(drop=True)
    )

    all_rows = []
    for model in run_totals["model"].unique():
        m = run_totals[run_totals["model"] == model].copy().reset_index(drop=True)
        m["prev_tdd"] = m["tdd"].shift(1)
        m["tdd_change"] = m["tdd"] - m["prev_tdd"]
        
        # Default hdd_change to hdd or tdd
        if "hdd" in m.columns:
            m["prev_hdd"] = m["hdd"].shift(1)
            m["hdd_change"] = m["hdd"] - m["prev_hdd"]
        else:
            m["hdd_change"] = m["tdd_change"]
            
        if "cdd" in m.columns:
            m["prev_cdd"] = m["cdd"].shift(1)
            m["cdd_change"] = m["cdd"] - m["prev_cdd"]
        else:
            m["cdd_change"] = 0.0

        if gw_mode:
            m["prev_tdd_gw"] = m["tdd_gw"].shift(1)
            m["tdd_change_gw"] = m["tdd_gw"] - m["prev_tdd_gw"]
            
            if "hdd_gw" in m.columns:
                m["prev_hdd_gw"] = m["hdd_gw"].shift(1)
                m["hdd_change_gw"] = m["hdd_gw"] - m["prev_hdd_gw"]
            else:
                m["hdd_change_gw"] = m["tdd_change_gw"]

            if "cdd_gw" in m.columns:
                m["prev_cdd_gw"] = m["cdd_gw"].shift(1)
                m["cdd_change_gw"] = m["cdd_gw"] - m["prev_cdd_gw"]
            else:
                m["cdd_change_gw"] = 0.0

        all_rows.append(m)

    result = pd.concat(all_rows, ignore_index=True)

    # Fast revision flag: any single run moving >1.0 DD
    # Use GW change if available, otherwise fall back to simple change
    if gw_mode and "hdd_change_gw" in result.columns:
        result["effective_change"] = result["hdd_change_gw"].fillna(result["hdd_change"])
    else:
        result["effective_change"] = result["hdd_change"]
        
    result["fast_revision"] = result["effective_change"].abs() > 1.0

    os.makedirs("outputs", exist_ok=True)
    result.to_csv(OUTPUT, index=False)

    flagged = result[result["fast_revision"]]
    if not flagged.empty:
        print("\n[ALERT] FAST REVISION ALERT (>1.0 HDD/day in one run):")
        print(flagged[["model", "run_id", "effective_change"]].to_string(index=False))

    print("\nTOTAL HDD PER RUN + RUN-TO-RUN CHANGE:\n")
    print(result.to_string())
    print(f"\nSaved to {OUTPUT}")
    return result


if __name__ == "__main__":
    compute_run_changes()
