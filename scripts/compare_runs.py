"""
compare_runs.py

Purpose:
- Compare two latest GFS runs
- Compute total TDD delta
- Output a clean text summary

Phase 1 philosophy:
- Focus on deltas, not absolutes
"""

import os
import pandas as pd

DATA_DIR = "data/gfs"
OUTPUT_DIR = "outputs/summaries"


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def latest_two_runs():
    runs = [
        d for d in os.listdir(DATA_DIR)
        if os.path.isdir(os.path.join(DATA_DIR, d))
    ]
    runs.sort()
    return runs[-2:]


def load_total_tdd(run_id):
    csv_path = os.path.join(DATA_DIR, f"{run_id}_tdd.csv")
    df = pd.read_csv(csv_path)
    return df["tdd"].sum()


def compare_latest_runs():
    run_prev, run_latest = latest_two_runs()

    tdd_prev = load_total_tdd(run_prev)
    tdd_latest = load_total_tdd(run_latest)

    delta = tdd_latest - tdd_prev

    ensure_dir(OUTPUT_DIR)

    summary = (
        f"GFS run comparison\n"
        f"Previous run: {run_prev}\n"
        f"Latest run:   {run_latest}\n\n"
        f"Total TDD change: {delta:+.2f}\n"
    )

    output_file = os.path.join(OUTPUT_DIR, "gfs_latest.txt")
    with open(output_file, "w") as f:
        f.write(summary)

    print(summary)
    print(f"Saved summary to {output_file}")


if __name__ == "__main__":
    compare_latest_runs()
