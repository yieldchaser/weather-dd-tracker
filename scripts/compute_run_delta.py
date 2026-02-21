"""
compute_run_delta.py

Computes the day-by-day TDD change between the two most recent runs
of each model. Saves result to outputs/run_delta.csv.

Uses outputs/tdd_master.csv (the unified file that actually exists).
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

    all_deltas = []

    for model in df["model"].unique():
        m = df[df["model"] == model].copy()
        runs = sorted(m["run_id"].unique())

        if len(runs) < 2:
            print(f"{model}: need at least 2 runs, skipping.")
            continue

        latest_id = runs[-1]
        prev_id = runs[-2]

        df_latest = m[m["run_id"] == latest_id][["date", "tdd"]].rename(columns={"tdd": "tdd_latest"})
        df_prev = m[m["run_id"] == prev_id][["date", "tdd"]].rename(columns={"tdd": "tdd_prev"})

        merged = df_latest.merge(df_prev, on="date", how="inner")
        merged["tdd_change"] = merged["tdd_latest"] - merged["tdd_prev"]
        merged["model"] = model
        merged["run_latest"] = latest_id
        merged["run_prev"] = prev_id

        all_deltas.append(merged)
        print(f"{model}: delta {prev_id} → {latest_id} | {len(merged)} days matched")

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
