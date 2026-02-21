"""
select_latest_run.py

Selects the most recent run for each model from tdd_master.csv
and saves them to outputs/<model>_latest.csv for quick Excel ingestion.
"""

import pandas as pd
import os

MASTER = "outputs/tdd_master.csv"


def select_latest():
    if not os.path.exists(MASTER):
        print("Master file not found. Run merge_tdd.py first.")
        return

    df = pd.read_csv(MASTER, parse_dates=["date"])

    os.makedirs("outputs", exist_ok=True)

    for model in df["model"].unique():
        m = df[df["model"] == model]
        latest_run = m["run_id"].max()
        latest = m[m["run_id"] == latest_run]

        out_path = f"outputs/{model.lower()}_latest.csv"
        latest.to_csv(out_path, index=False)
        print(f"{model}: latest run = {latest_run} | {len(latest)} rows → {out_path}")


if __name__ == "__main__":
    select_latest()
