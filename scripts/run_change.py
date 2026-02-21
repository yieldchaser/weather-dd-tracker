import os
import pandas as pd

MASTER = "outputs/tdd_master.csv"
OUTPUT = "outputs/run_change.csv"


def compute_run_changes():
    df = pd.read_csv(MASTER)

    # remove rows without run_id
    df = df[df["run_id"].notna()]

    # compute total HDD per run
    run_totals = (
        df.groupby(["model", "run_id"])["tdd"]
        .sum()
        .reset_index()
        .sort_values(["model", "run_id"])
        .reset_index(drop=True)
    )

    all_rows = []

    for model in run_totals["model"].unique():
        m = run_totals[run_totals["model"] == model].copy().reset_index(drop=True)
        m["prev_tdd"] = m["tdd"].shift(1)
        m["hdd_change"] = m["tdd"] - m["prev_tdd"]
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
