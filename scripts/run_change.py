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
    if gw_mode:
        # Backfill tdd_gw from tdd for old pre-Phase-2 rows
        df["tdd_gw"] = df["tdd_gw"].fillna(df["tdd"])

    # Compute total HDD per run (simple)
    agg = {"tdd": "sum"}
    if gw_mode:
        agg["tdd_gw"] = "sum"

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
            m["hdd_change_gw"] = m["tdd_gw"] - m["prev_tdd_gw"]
        all_rows.append(m)

    result = pd.concat(all_rows, ignore_index=True)
    os.makedirs("outputs", exist_ok=True)
    result.to_csv(OUTPUT, index=False)

    print("\nTOTAL HDD PER RUN + RUN-TO-RUN CHANGE:\n")
    print(result.to_string())
    print(f"\nSaved to {OUTPUT}")
    return result


if __name__ == "__main__":
    compute_run_changes()
