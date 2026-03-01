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
    # REMOVED global fillna to preserve methodology integrity

    # Compute Average HDD per run (normalized)
    agg = {"tdd": "mean"}
    if gw_mode:
        agg["tdd_gw"] = "mean"

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
        m["prev_tdd"]    = m["tdd"].shift(1)
        m["hdd_change"]  = m["tdd"] - m["prev_tdd"]
        
        if gw_mode:
            m["prev_tdd_gw"]   = m["tdd_gw"].shift(1)
            m["prev_tdd"]      = m["tdd"].shift(1)
            
            # METHODOLOGY INTEGRITY: If tdd_gw exactly equals tdd, it's a fallback 'pollutant'.
            # We treat such rows as missing GW data to ensure apples-to-apples.
            def get_gw_change(row):
                curr_gw = row["tdd_gw"]
                curr_si = row["tdd"]
                prev_gw = row["prev_tdd_gw"]
                prev_si = row["prev_tdd"]
                
                # Check for NaNs and Fallback-Equality
                if pd.isna(curr_gw) or pd.isna(prev_gw): return None
                if abs(curr_gw - curr_si) < 0.01: return None # Current is simple fallback
                if abs(prev_gw - prev_si) < 0.01: return None # Prev is simple fallback
                
                return curr_gw - prev_gw

            m["hdd_change_gw"] = m.apply(get_gw_change, axis=1)
        all_rows.append(m)

    result = pd.concat(all_rows, ignore_index=True)

    # Fast revision flag: any single run moving >3 HDD
    # Use GW change if available, otherwise fall back to simple change
    if gw_mode:
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
