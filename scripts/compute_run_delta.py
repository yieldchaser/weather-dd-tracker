"""
compute_run_delta.py

Computes the day-by-day TDD change between the two most recent runs
of each model. Saves result to outputs/run_delta.csv.

FIX (Issue #4): Now computes delta for both tdd (simple) and
tdd_gw (gas-weighted) when the GW column is available.
"""

import pandas as pd
import os

MASTER = "outputs/tdd_master.csv"
OUTPUT = "outputs/run_delta.csv"


def compute_delta():
    if not os.path.exists(MASTER):
        print("Master file not found. Run merge_tdd.py first.")
        return

    df = pd.read_csv(MASTER, parse_dates=["date"])

    gw_mode = "tdd_gw" in df.columns
    if gw_mode:
        df["tdd_gw"] = df["tdd_gw"].fillna(df["tdd"])

    all_deltas = []

    for model in df["model"].unique():
        m    = df[df["model"] == model].copy()
        runs = sorted(m["run_id"].unique())

        if len(runs) < 2:
            print(f"{model}: need at least 2 runs, skipping.")
            continue

        latest_id = runs[-1]
        prev_id   = runs[-2]

        cols_latest = {"date": "date", "tdd": "tdd_latest"}
        cols_prev   = {"date": "date", "tdd": "tdd_prev"}
        if gw_mode:
            cols_latest["tdd_gw"] = "tdd_gw_latest"
            cols_prev["tdd_gw"]   = "tdd_gw_prev"

        df_latest = m[m["run_id"] == latest_id][list(cols_latest.keys())].rename(columns=cols_latest)
        df_prev   = m[m["run_id"] == prev_id][list(cols_prev.keys())].rename(columns=cols_prev)

        merged = df_latest.merge(df_prev, on="date", how="inner")
        merged["tdd_change"]    = merged["tdd_latest"]    - merged["tdd_prev"]
        if gw_mode:
            merged["tdd_gw_change"] = merged["tdd_gw_latest"] - merged["tdd_gw_prev"]

        merged["model"]      = model
        merged["run_latest"] = latest_id
        merged["run_prev"]   = prev_id

        all_deltas.append(merged)
        print(f"{model}: delta {prev_id} -> {latest_id} | {len(merged)} overlapping days")

    if not all_deltas:
        print("No deltas computed.")
        return

    out = pd.concat(all_deltas, ignore_index=True)
    os.makedirs("outputs", exist_ok=True)
    out.to_csv(OUTPUT, index=False)
    print(f"\nDelta saved to {OUTPUT}")
    print(out.to_string())


if __name__ == "__main__":
    compute_delta()
